import os
import re
import json
import asyncio
import logging
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, PollAnswer,
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, CallbackQuery,
    FSInputFile
)
from aiogram.enums import ParseMode

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "http://localhost:8000/event-form")
API_BASE = os.getenv("API_BASE", "http://localhost:8000")

if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN env var")

TZ = ZoneInfo("Europe/Vilnius")
DB_PATH = "calendar_bot.sqlite3"

OPTIONS = ["я в деле", "надо подумать", "точно не смогу"]
OPT_YES, OPT_MAYBE, OPT_NO = 0, 1, 2

REM_36H = "maybe_36h"
REM_3H = "yes_3h"

router = Router()

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

def now_tz() -> datetime:
    return datetime.now(tz=TZ)

def format_card(dt: datetime, title: str, cost: str, location: str, details: str = "") -> str:
    text = (
        f"📅 **{title}**\n"
        f"🕒 {dt.strftime('%Y-%m-%d %H:%M')} ({TZ.key})\n"
        f"📍 {location}\n"
        f"💸 {cost}"
    )
    if details.strip():
        text += f"\n\n📝 {details.strip()}"
    return text

def build_message_link(chat_id: int, message_id: int, chat_username: Optional[str]) -> Optional[str]:
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"
    if str(chat_id).startswith("-100"):
        internal = int(str(abs(chat_id))[3:])
        return f"https://t.me/c/{internal}/{message_id}"
    return None

def kb_new_event():
    # Можно прокинуть api_base параметром в URL, если нужно.
    url = WEBAPP_URL
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать встречу (форма)", web_app=WebAppInfo(url=url))],
        [InlineKeyboardButton(text="📅 Календарь", callback_data="calendar:show")]
    ])

def kb_event_actions(event_id: int):
    edit_url = f"{WEBAPP_URL}?event_id={event_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✏️ Редактировать", web_app=WebAppInfo(url=edit_url)),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"event:del:{event_id}"),
        ],
        [
            InlineKeyboardButton(text="📆 Добавить в мой календарь", callback_data=f"event:ics:{event_id}"),
        ],
    ])

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()

async def create_or_replace_reminders(db, event_id: int, dt: datetime):
    t36 = dt - timedelta(hours=36)
    t3 = dt - timedelta(hours=3)
    await db.execute("DELETE FROM reminders WHERE event_id=?", (event_id,))
    if t36 > now_tz():
        await db.execute(
            "INSERT OR IGNORE INTO reminders(event_id, kind, run_at_iso, sent) VALUES(?, ?, ?, 0)",
            (event_id, REM_36H, t36.isoformat()),
        )
    if t3 > now_tz():
        await db.execute(
            "INSERT OR IGNORE INTO reminders(event_id, kind, run_at_iso, sent) VALUES(?, ?, ?, 0)",
            (event_id, REM_3H, t3.isoformat()),
        )

async def get_due_reminders(db):
    cur = await db.execute(
        "SELECT id, event_id, kind FROM reminders WHERE sent=0 AND run_at_iso<=? ORDER BY run_at_iso ASC",
        (now_tz().isoformat(),),
    )
    rows = await cur.fetchall()
    await cur.close()
    return rows

async def mark_reminder_sent(db, reminder_id: int):
    await db.execute(
        "UPDATE reminders SET sent=1, sent_at_iso=? WHERE id=?",
        (now_tz().isoformat(), reminder_id),
    )

async def get_user_ids_by_choice(db, poll_id: str, option_id: int) -> List[int]:
    cur = await db.execute(
        "SELECT user_id FROM votes WHERE poll_id=? AND option_id=?",
        (poll_id, option_id),
    )
    rows = await cur.fetchall()
    await cur.close()
    return [r[0] for r in rows]

def mention(uid: int, name: str = "user") -> str:
    return f"[{name}](tg://user?id={uid})"

async def reminders_worker(bot: Bot):
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                due = await get_due_reminders(db)
                for reminder_id, event_id, kind in due:
                    cur = await db.execute(
                        "SELECT chat_id, poll_id, poll_message_id, dt_iso, title, cost, location, details "
                        "FROM events WHERE id=?",
                        (event_id,),
                    )
                    event = await cur.fetchone()
                    await cur.close()
                    if not event:
                        await mark_reminder_sent(db, reminder_id)
                        continue

                    chat_id, poll_id, poll_msg_id, dt_iso, title, cost, location, details = event
                    dt = datetime.fromisoformat(dt_iso).astimezone(TZ)

                    link = None
                    if str(chat_id).startswith("-100"):
                        internal = int(str(abs(chat_id))[3:])
                        link = f"https://t.me/c/{internal}/{poll_msg_id}"

                    if kind == REM_36H:
                        user_ids = await get_user_ids_by_choice(db, poll_id, OPT_MAYBE)
                        if user_ids:
                            mentions = ", ".join(mention(uid) for uid in user_ids[:30])
                            more = f" …и ещё {len(user_ids)-30}" if len(user_ids) > 30 else ""
                            text = f"⏳ До встречи осталось ~36 часов.\n{mentions}{more}\n**Вы как?** Переголосуйте, пожалуйста 🙂"
                            if link:
                                text += f"\n\nОпрос: {link}"
                            await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN)

                    elif kind == REM_3H:
                        user_ids = await get_user_ids_by_choice(db, poll_id, OPT_YES)
                        if user_ids:
                            mentions = ", ".join(mention(uid) for uid in user_ids[:30])
                            more = f" …и ещё {len(user_ids)-30}" if len(user_ids) > 30 else ""
                            text = (
                                f"🔔 Через ~3 часа встреча!\n{mentions}{more}\n\n"
                                f"📅 **{title}**\n"
                                f"🕒 {dt.strftime('%Y-%m-%d %H:%M')} ({TZ.key})\n"
                                f"📍 {location}\n"
                                f"💸 {cost}"
                            )
                            if link:
                                text += f"\n\nОпрос: {link}"
                            await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN)

                    await mark_reminder_sent(db, reminder_id)

                await db.commit()
        except Exception:
            logging.exception("reminders_worker error")
        await asyncio.sleep(30)


@router.message(Command("new"))
async def cmd_new(message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда работает в группах/супергруппах.")
        return
    await message.answer("Создание встречи:", reply_markup=kb_new_event())


@router.message(Command("calendar"))
async def cmd_calendar(message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Календарь работает в группах/супергруппах.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, dt_iso, title, cost, location FROM events WHERE chat_id=? AND dt_iso>=? ORDER BY dt_iso LIMIT 10",
            (message.chat.id, now_tz().isoformat()),
        )
        rows = await cur.fetchall()
        await cur.close()

    if not rows:
        await message.answer("Ближайших встреч нет.")
        return

    lines = ["📌 **Ближайшие встречи:**"]
    for event_id, dt_iso, title, cost, location in rows:
        dt = datetime.fromisoformat(dt_iso).astimezone(TZ)
        lines.append(f"• {dt.strftime('%Y-%m-%d %H:%M')} — **{title}** ({location}, {cost})")
    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


@router.message(F.web_app_data)
async def on_webapp_data(message: Message, bot: Bot):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Создание/редактирование работает в группах/супергруппах.")
        return

    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        await message.answer("Не смог прочитать данные формы 😕")
        return

    action = data.get("action")
    date = (data.get("date") or "").strip()
    time = (data.get("time") or "").strip()
    title = (data.get("title") or "Встреча").strip()
    cost = (data.get("cost") or "-").strip()
    location = (data.get("location") or "-").strip()
    details = (data.get("details") or "").strip()

    if not date or not time:
        await message.answer("Нужны дата и время.")
        return

    dt = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    if dt <= now_tz():
        await message.answer("Похоже, это время уже в прошлом.")
        return

    if action == "create":
        # 1) карточка
        card_msg = await bot.send_message(
            message.chat.id,
            format_card(dt, title, cost, location, details),
            parse_mode=ParseMode.MARKDOWN
        )

        # 2) опрос
        poll_msg = await bot.send_poll(
            chat_id=message.chat.id,
            question=f"{title} — {dt.strftime('%Y-%m-%d %H:%M')}",
            options=OPTIONS,
            is_anonymous=False,
            allows_multiple_answers=False,
        )

        # 3) сохраняем
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO events(chat_id, poll_id, poll_message_id, card_message_id, dt_iso, title, cost, location, details, created_at_iso) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    message.chat.id,
                    poll_msg.poll.id,
                    poll_msg.message_id,
                    card_msg.message_id,
                    dt.isoformat(),
                    title,
                    cost,
                    location,
                    details,
                    now_tz().isoformat(),
                ),
            )
            cur = await db.execute("SELECT id FROM events WHERE poll_id=?", (poll_msg.poll.id,))
            row = await cur.fetchone()
            await cur.close()
            event_id = row[0]

            await create_or_replace_reminders(db, event_id, dt)
            await db.commit()

        # 4) кнопки
        await bot.edit_message_reply_markup(
            chat_id=message.chat.id,
            message_id=card_msg.message_id,
            reply_markup=kb_event_actions(event_id),
        )

        # 5) автозакреп (если бот админ)
        try:
            await bot.pin_chat_message(message.chat.id, card_msg.message_id, disable_notification=True)
        except Exception:
            # молча: нет прав — ок
            pass

        await message.answer("✅ Встреча создана. Голосуйте в опросе 👇")
        return

    if action == "edit":
        event_id = data.get("event_id")
        if not event_id:
            await message.answer("Не вижу event_id для редактирования.")
            return

        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT chat_id, card_message_id FROM events WHERE id=?",
                (int(event_id),),
            )
            row = await cur.fetchone()
            await cur.close()
            if not row:
                await message.answer("Событие не найдено.")
                return
            chat_id, card_message_id = row

            # защита от редактирования “чужого чата”
            if chat_id != message.chat.id:
                await message.answer("Это событие относится к другому чату.")
                return

            await db.execute(
                "UPDATE events SET dt_iso=?, title=?, cost=?, location=?, details=? WHERE id=?",
                (dt.isoformat(), title, cost, location, details, int(event_id)),
            )
            await create_or_replace_reminders(db, int(event_id), dt)
            await db.commit()

        # обновим карточку
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=int(card_message_id),
                text=format_card(dt, title, cost, location, details),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_event_actions(int(event_id)),
            )
        except Exception:
            pass

        await message.answer("✅ Изменения сохранены.")
        return

    await message.answer("Неизвестное действие формы.")


@router.poll_answer()
async def on_poll_answer(poll_answer: PollAnswer):
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    if not user:
        return

    option_id: Optional[int]
    if not poll_answer.option_ids:
        option_id = None
    else:
        option_id = int(poll_answer.option_ids[0])

    async with aiosqlite.connect(DB_PATH) as db:
        # сохраняем только для известных poll_id
        cur = await db.execute("SELECT 1 FROM events WHERE poll_id=?", (poll_id,))
        ok = await cur.fetchone()
        await cur.close()
        if not ok:
            return

        await db.execute(
            "INSERT INTO votes(poll_id, user_id, option_id, updated_at_iso) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(poll_id, user_id) DO UPDATE SET option_id=excluded.option_id, updated_at_iso=excluded.updated_at_iso",
            (poll_id, user.id, option_id, now_tz().isoformat()),
        )
        await db.commit()


@router.callback_query(F.data.startswith("event:del:"))
async def on_event_delete(cb: CallbackQuery, bot: Bot):
    event_id = int(cb.data.split(":")[-1])

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT chat_id, poll_message_id, card_message_id, poll_id FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            await cb.answer("Не нашёл событие")
            return
        chat_id, poll_msg_id, card_msg_id, poll_id = row

        await db.execute("DELETE FROM reminders WHERE event_id=?", (event_id,))
        await db.execute("DELETE FROM votes WHERE poll_id=?", (poll_id,))
        await db.execute("DELETE FROM events WHERE id=?", (event_id,))
        await db.commit()

    for mid in [card_msg_id, poll_msg_id]:
        if mid:
            try:
                await bot.delete_message(chat_id, int(mid))
            except Exception:
                pass

    await cb.answer("Удалено")


def make_ics(dt: datetime, title: str, location: str, description: str) -> str:
    # В .ics удобнее хранить время в UTC
    dt_utc = dt.astimezone(ZoneInfo("UTC"))
    dtend_utc = (dt + timedelta(hours=2)).astimezone(ZoneInfo("UTC"))  # длительность по умолчанию 2ч
    uid = hashlib.sha1(f"{dt.isoformat()}|{title}|{location}".encode("utf-8")).hexdigest() + "@telegram-bot"

    def fmt(d: datetime) -> str:
        return d.strftime("%Y%m%dT%H%M%SZ")

    esc = lambda s: (s or "").replace("\\", "\\\\").replace("\n", "\\n").replace(",", "\\,").replace(";", "\\;")
    return (
        "BEGIN:VCALENDAR\n"
        "VERSION:2.0\n"
        "PRODID:-//TelegramMeetingBot//EN\n"
        "CALSCALE:GREGORIAN\n"
        "BEGIN:VEVENT\n"
        f"UID:{uid}\n"
        f"DTSTAMP:{fmt(datetime.now(tz=ZoneInfo('UTC')))}\n"
        f"DTSTART:{fmt(dt_utc)}\n"
        f"DTEND:{fmt(dtend_utc)}\n"
        f"SUMMARY:{esc(title)}\n"
        f"LOCATION:{esc(location)}\n"
        f"DESCRIPTION:{esc(description)}\n"
        "END:VEVENT\n"
        "END:VCALENDAR\n"
    )

@router.callback_query(F.data.startswith("event:ics:"))
async def on_event_ics(cb: CallbackQuery, bot: Bot):
    event_id = int(cb.data.split(":")[-1])
    user = cb.from_user

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT dt_iso, title, cost, location, details FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()

    if not row:
        await cb.answer("Событие не найдено")
        return

    dt_iso, title, cost, location, details = row
    dt = datetime.fromisoformat(dt_iso).astimezone(TZ)

    description = f"Стоимость: {cost}"
    if details.strip():
        description += f"\n\n{details.strip()}"

    ics_text = make_ics(dt, title, location, description)
    filename = f"event_{event_id}.ics"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(ics_text)

    # “Персонально” — отправляем в ответ пользователю (но это всё равно сообщение в чат).
    # Если нужно строго в личку — пользователь должен сделать /start боту.
    await bot.send_document(
        chat_id=cb.message.chat.id,
        document=FSInputFile(filename),
        caption=f"📆 {user.full_name}, файл для добавления в календарь: **{title}**",
        parse_mode=ParseMode.MARKDOWN
    )

    await cb.answer("Отправил .ics")


async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.MARKDOWN)
    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(reminders_worker(bot))

    logging.info("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
