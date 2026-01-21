import os
import json
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List

import aiosqlite
from fastapi import FastAPI, HTTPException, Header, Query
app = FastAPI() #//добавленная строка
from fastapi.responses import HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()

DB_PATH = os.getenv("DB_PATH", "calendar_bot.sqlite3")
TZ = ZoneInfo("Europe/Moscow")

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN env var (same token as bot)")

app = FastAPI()

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

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  updated_at_iso TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_chat_dt ON events(chat_id, dt_iso);
CREATE INDEX IF NOT EXISTS idx_votes_poll_user ON votes(poll_id, user_id);
"""

@app.on_event("startup")
async def startup():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()


def telegram_webapp_verify_initdata(init_data: str) -> dict:
    if not init_data:
        raise HTTPException(401, "Missing initData")

    try:
        parsed = urllib.parse.parse_qs(init_data, strict_parsing=True)
    except Exception:
        raise HTTPException(401, "Bad initData format")

    if "hash" not in parsed:
        raise HTTPException(401, "No hash in initData")

    received_hash = parsed["hash"][0]

    pairs = []
    for k in sorted(parsed.keys()):
        if k == "hash":
            continue
        v = parsed[k][0]
        pairs.append(f"{k}={v}")
    data_check_string = "\n".join(pairs)

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


def _chat_sig_expected(chat_id: int) -> str:
    key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    msg = str(chat_id).encode("utf-8")
    full = hmac.new(key, msg, hashlib.sha256).hexdigest()
    return full[:20]

def _user_sig_expected(chat_id: int, user_id: int) -> str:
    key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    msg = f"{chat_id}:{user_id}".encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def verify_chat_sig(chat_id: int, sig: str):
    exp = _chat_sig_expected(chat_id)
    if not sig or not hmac.compare_digest(exp, sig):
        raise HTTPException(403, "bad signature")

def make_ics(dt: datetime, title: str, location: str, description: str) -> str:
    dt_utc = dt.astimezone(ZoneInfo("UTC"))
    dtend_utc = (dt + timedelta(hours=2)).astimezone(ZoneInfo("UTC"))

    uid = hashlib.sha1(f"{dt.isoformat()}|{title}|{location}".encode("utf-8")).hexdigest() + "@telegram-meeting-bot"

    def fmt(d: datetime) -> str:
        return d.strftime("%Y%m%dT%H%M%SZ")

    def esc(s: str) -> str:
        s = s or ""
        return s.replace("\\", "\\\\").replace("\n", "\\n").replace(",", "\\,").replace(";", "\\;")

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "METHOD:PUBLISH",
        "PRODID:-//YourApp//EN",
        "CALSCALE:GREGORIAN",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{fmt(datetime.now(tz=ZoneInfo('UTC')))}",
        f"DTSTART:{fmt(dt_utc)}",
        f"DTEND:{fmt(dtend_utc)}",
        f"SUMMARY:{esc(title)}",
        f"LOCATION:{esc(location)}",
        f"DESCRIPTION:{esc(description)}",
        "END:VEVENT",
        "END:VCALENDAR",
    ]
    return "\r\n".join(lines) + "\r\n"


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


class CalendarItem(BaseModel):
    id: int
    dt_iso: str
    title: str
    cost: str
    location: str
    details: str
    poll_link: Optional[str] = None
    my_vote: Optional[str] = None  # "yes" | "maybe" | "no" | None


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
    user_id: Optional[int] = Query(default=None),
    user_sig: Optional[str] = Query(default=None),
    x_telegram_initdata: str = Header(default="", alias="X-Telegram-InitData"),
):
    user_id_final = None
    if x_telegram_initdata:
        auth = telegram_webapp_verify_initdata(x_telegram_initdata)
        user_id_final = int(auth["user"]["id"])
    elif user_id is not None and user_sig:
        user_id_final = int(user_id)

    try:
        dt = datetime.fromisoformat(patch.dt_iso)
        if dt.tzinfo is None:
            raise ValueError("Timezone required")
    except Exception:
        raise HTTPException(400, "dt_iso must be ISO with timezone, e.g. 2026-01-10T19:00:00+02:00")

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT creator_user_id, chat_id FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()

        if not row:
            raise HTTPException(404, "event not found")

        creator_user_id, chat_id = row
        if creator_user_id is None:
            raise HTTPException(403, "not allowed")

        if user_id_final is None:
            raise HTTPException(401, "Missing initData or user signature")

        if x_telegram_initdata == "" and user_sig:
            expected = _user_sig_expected(int(chat_id), int(user_id_final))
            if not hmac.compare_digest(expected, user_sig):
                raise HTTPException(403, "bad user signature")

        if int(creator_user_id) != int(user_id_final):
            raise HTTPException(403, "not allowed")

        await db.execute(
            "UPDATE events SET dt_iso=?, title=?, cost=?, location=?, details=? WHERE id=?",
            (patch.dt_iso, patch.title, patch.cost, patch.location, patch.details or "", event_id),
        )
        await db.commit()

    return {"ok": True}


@app.delete("/api/event/{event_id}")
async def api_delete_event(
    event_id: int,
    user_id: Optional[int] = Query(default=None),
    user_sig: Optional[str] = Query(default=None),
    x_telegram_initdata: str = Header(default="", alias="X-Telegram-InitData"),
):
    user_id_final = None
    if x_telegram_initdata:
        auth = telegram_webapp_verify_initdata(x_telegram_initdata)
        user_id_final = int(auth["user"]["id"])
    elif user_id is not None and user_sig:
        user_id_final = int(user_id)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT creator_user_id, chat_id, poll_id FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()

        if not row:
            raise HTTPException(404, "event not found")

        creator_user_id, chat_id, poll_id = row
        if creator_user_id is None:
            raise HTTPException(403, "not allowed")

        if user_id_final is None:
            raise HTTPException(401, "Missing initData or user signature")

        if x_telegram_initdata == "" and user_sig:
            expected = _user_sig_expected(int(chat_id), int(user_id_final))
            if not hmac.compare_digest(expected, user_sig):
                raise HTTPException(403, "bad user signature")

        if int(creator_user_id) != int(user_id_final):
            raise HTTPException(403, "not allowed")

        await db.execute("DELETE FROM reminders WHERE event_id=?", (event_id,))
        if poll_id:
            await db.execute("DELETE FROM votes WHERE poll_id=?", (poll_id,))
        await db.execute("DELETE FROM events WHERE id=?", (event_id,))
        await db.commit()

    return {"ok": True}

@app.get("/api/calendar/upcoming", response_model=List[CalendarItem])
async def api_calendar_upcoming(
    chat_id: int = Query(...),
    sig: str = Query(...),
    limit: int = Query(50, ge=1, le=200),
    user_id: Optional[int] = Query(default=None),
    user_sig: Optional[str] = Query(default=None),
    x_telegram_initdata: str = Header(default="", alias="X-Telegram-InitData"),
):
    """
    Возвращает ближайшие события + голос текущего пользователя (my_vote).
    Требует:
      - chat_id + sig (подпись от бота)
      - initData необязательно (если нет, my_vote будет пустой)
    """
    verify_chat_sig(chat_id, sig)
    user_id_final = None
    if x_telegram_initdata:
        auth = telegram_webapp_verify_initdata(x_telegram_initdata)
        user_id_final = int(auth["user"]["id"])
    elif user_id is not None and user_sig:
        expected = _user_sig_expected(chat_id, user_id)
        if not hmac.compare_digest(expected, user_sig):
            raise HTTPException(403, "bad user signature")
        user_id_final = int(user_id)

    user_id_param = user_id_final if user_id_final is not None else -1

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
              e.id, e.dt_iso, e.title, e.cost, e.location, e.details, e.poll_message_id, e.poll_id,
              v.option_id
            FROM events e
            LEFT JOIN votes v
              ON v.poll_id = e.poll_id AND v.user_id = ?
            WHERE e.chat_id = ?
            ORDER BY e.dt_iso ASC
            """,
            (user_id_param, chat_id),
        )
        rows = await cur.fetchall()
        await cur.close()

    now_dt = datetime.now(tz=TZ)
    items: List[CalendarItem] = []
    for (eid, dt_iso, title, cost, location, details, poll_mid, poll_id, option_id) in rows:
        try:
            event_dt = datetime.fromisoformat(dt_iso)
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=TZ)
            event_dt = event_dt.astimezone(TZ)
        except Exception:
            continue
        if event_dt < now_dt:
            continue

        poll_link = None
        if str(chat_id).startswith("-100"):
            internal = int(str(abs(chat_id))[3:])
            poll_link = f"https://t.me/c/{internal}/{poll_mid}"

        my_vote = None
        # option_id: 0=yes,1=maybe,2=no
        if option_id is not None:
            if int(option_id) == 0:
                my_vote = "yes"
            elif int(option_id) == 1:
                my_vote = "maybe"
            elif int(option_id) == 2:
                my_vote = "no"

        items.append(CalendarItem(
            id=eid,
            dt_iso=dt_iso,
            title=title,
            cost=cost,
            location=location,
            details=details,
            poll_link=poll_link,
            my_vote=my_vote
        ))
        if len(items) >= limit:
            break

    return items

@app.get("/api/calendar/ics")
async def api_calendar_ics(
    event_id: int = Query(...),
    user_id: Optional[int] = Query(default=None),
    user_sig: Optional[str] = Query(default=None),
    chat_sig: Optional[str] = Query(default=None),
    x_telegram_initdata: str = Header(default="", alias="X-Telegram-InitData"),
):
    user_id_final = None
    if x_telegram_initdata:
        auth = telegram_webapp_verify_initdata(x_telegram_initdata)
        user_id_final = int(auth["user"]["id"])
    elif user_id is not None and user_sig:
        user_id_final = int(user_id)
    elif chat_sig:
        user_id_final = None
    else:
        raise HTTPException(401, "Missing initData or user signature")

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT chat_id, dt_iso, title, cost, location, details FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()

    if not row:
        raise HTTPException(404, "event not found")

    chat_id, dt_iso, title, cost, location, details = row

    if x_telegram_initdata == "" and user_sig:
        expected = _user_sig_expected(int(chat_id), int(user_id_final))
        if not hmac.compare_digest(expected, user_sig):
            raise HTTPException(403, "bad user signature")
    elif x_telegram_initdata == "" and chat_sig:
        expected = _chat_sig_expected(int(chat_id))
        if not hmac.compare_digest(expected, chat_sig):
            raise HTTPException(403, "bad chat signature")

    dt = datetime.fromisoformat(dt_iso).astimezone(TZ)
    description = f"Стоимость: {cost}"
    if (details or "").strip():
        description += f"\n\n{details.strip()}"

    ics_text = make_ics(dt, title, location, description)
    filename = f"event_{event_id}.ics"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(
        content=ics_text,
        media_type="text/calendar; charset=utf-8; method=PUBLISH",
        headers=headers,
    )
