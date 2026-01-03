import os
import json
import hmac
import hashlib
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

import aiosqlite
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DB_PATH = "calendar_bot.sqlite3"
TZ = ZoneInfo("Europe/Vilnius")

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN env var (same token as bot)")

app = FastAPI()

# Если форма и API на одном домене — CORS не нужен.
# Если на разных доменах — можно оставить и ограничить allow_origins.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

CREATE_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_id INTEGER NOT NULL,
  poll_id TEXT NOT NULL UNIQUE,
  poll_message_id INTEGER NOT NULL,
  card_message_id INTEGER,
  creator_user_id INTEGER,
  dt_iso TEXT NOT NULL,
  title TEXT NOT NULL,
  cost TEXT NOT NULL,
  location TEXT NOT NULL,
  details TEXT NOT NULL DEFAULT '',
  created_at_iso TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS votes (
  poll_id TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  option_id INTEGER,
  updated_at_iso TEXT NOT NULL,
  PRIMARY KEY (poll_id, user_id)
);

CREATE TABLE IF NOT EXISTS reminders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  run_at_iso TEXT NOT NULL,
  sent INTEGER NOT NULL DEFAULT 0,
  sent_at_iso TEXT,
  UNIQUE(event_id, kind),
  FOREIGN KEY(event_id) REFERENCES events(id)
);

CREATE INDEX IF NOT EXISTS idx_events_chat_dt ON events(chat_id, dt_iso);
CREATE INDEX IF NOT EXISTS idx_reminders_due ON reminders(sent, run_at_iso);
"""


@app.on_event("startup")
async def startup():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()


def telegram_webapp_verify_initdata(init_data: str) -> dict:
    """
    Проверка подписи initData по Telegram WebApp.
    Возвращает dict с user (initDataUnsafe.user), если подпись валидна.
    """
    if not init_data:
        raise HTTPException(401, "Missing initData")

    # init_data: "query_id=...&user=...&auth_date=...&hash=..."
    try:
        parsed = urllib.parse.parse_qs(init_data, strict_parsing=True)
    except Exception:
        raise HTTPException(401, "Bad initData format")

    if "hash" not in parsed:
        raise HTTPException(401, "No hash in initData")

    received_hash = parsed["hash"][0]

    # Собираем data_check_string из всех ключей кроме hash
    pairs = []
    for k in sorted(parsed.keys()):
        if k == "hash":
            continue
        v = parsed[k][0]
        pairs.append(f"{k}={v}")
    data_check_string = "\n".join(pairs)

    # secret_key = HMAC_SHA256("WebAppData", bot_token)
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        raise HTTPException(401, "Bad initData hash")

    user_json = parsed.get("user", [None])[0]
    if not user_json:
        raise HTTPException(401, "No user in initData")

    try:
        user = json.loads(user_json)
    except Exception:
        raise HTTPException(401, "Bad user JSON in initData")

    if "id" not in user:
        raise HTTPException(401, "No user.id in initData")

    return {"user": user}


class EventView(BaseModel):
    id: int
    chat_id: int
    dt_iso: str
    title: str
    cost: str
    location: str
    details: str


class EventPatch(BaseModel):
    dt_iso: str
    title: str
    cost: str
    location: str
    details: Optional[str] = ""


@app.get("/event-form", response_class=HTMLResponse)
async def event_form():
    path = os.path.join("webapp", "index.html")
    if not os.path.exists(path):
        raise HTTPException(500, "webapp/index.html not found")
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/api/event/{event_id}", response_model=EventView)
async def api_get_event(event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, chat_id, dt_iso, title, cost, location, details FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()

    if not row:
        raise HTTPException(404, "event not found")

    return EventView(
        id=row[0],
        chat_id=row[1],
        dt_iso=row[2],
        title=row[3],
        cost=row[4],
        location=row[5],
        details=row[6],
    )


@app.put("/api/event/{event_id}")
async def api_update_event(
    event_id: int,
    patch: EventPatch,
    x_telegram_initdata: str = Header(default="", alias="X-Telegram-InitData"),
):
    auth = telegram_webapp_verify_initdata(x_telegram_initdata)
    user_id = int(auth["user"]["id"])

    # Валидация времени
    try:
        dt = datetime.fromisoformat(patch.dt_iso)
        if dt.tzinfo is None:
            raise ValueError("Timezone required")
    except Exception:
        raise HTTPException(400, "dt_iso must be ISO with timezone, e.g. 2026-01-10T19:00:00+02:00")

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT creator_user_id FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()

        if not row:
            raise HTTPException(404, "event not found")

        creator_user_id = row[0]
        if creator_user_id is None:
            # если старое событие без автора — запрещаем правку (можешь поменять на разрешить)
            raise HTTPException(403, "not allowed")

        if int(creator_user_id) != user_id:
            raise HTTPException(403, "not allowed")

        await db.execute(
            "UPDATE events SET dt_iso=?, title=?, cost=?, location=?, details=? WHERE id=?",
            (patch.dt_iso, patch.title, patch.cost, patch.location, patch.details or "", event_id),
        )
        await db.commit()

    return {"ok": True}
