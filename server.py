import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

import aiosqlite
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DB_PATH = "calendar_bot.sqlite3"
TZ = ZoneInfo("Europe/Vilnius")

app = FastAPI()

# Если форма и API на одном домене — CORS не нужен.
# Если на разных — оставь. В проде ограничь origins.
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


class EventPatch(BaseModel):
    dt_iso: str
    title: str
    cost: str
    location: str
    details: Optional[str] = ""


@app.get("/event-form", response_class=HTMLResponse)
async def event_form():
    # отдаём webapp как HTML
    path = os.path.join("webapp", "index.html")
    if not os.path.exists(path):
        raise HTTPException(500, "webapp/index.html not found")
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/api/event/{event_id}")
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
    return {
        "id": row[0],
        "chat_id": row[1],
        "dt_iso": row[2],
        "title": row[3],
        "cost": row[4],
        "location": row[5],
        "details": row[6],
    }


@app.put("/api/event/{event_id}")
async def api_update_event(event_id: int, patch: EventPatch):
    # ВАЖНО: в проде тут нужно проверять подпись Telegram initData,
    # иначе любой сможет дергать API и править события.
    # В рамках “скелета” оставляю упрощённо.

    # базовая валидация dt
    try:
        dt = datetime.fromisoformat(patch.dt_iso)
        if dt.tzinfo is None:
            raise ValueError("tz required")
    except Exception:
        raise HTTPException(400, "dt_iso must be ISO with timezone, e.g. 2026-01-10T19:00:00+02:00")

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM events WHERE id=?", (event_id,))
        exists = await cur.fetchone()
        await cur.close()
        if not exists:
            raise HTTPException(404, "event not found")

        await db.execute(
            "UPDATE events SET dt_iso=?, title=?, cost=?, location=?, details=? WHERE id=?",
            (patch.dt_iso, patch.title, patch.cost, patch.location, patch.details or "", event_id),
        )
        await db.commit()

    return {"ok": True}
