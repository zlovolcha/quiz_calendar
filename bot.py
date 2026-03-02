import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, PollAnswer,
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton, FSInputFile
)
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError

from core import (
    BOT_TOKEN,
    BOT_USERNAME,
    WEBAPP_URL,
    MINIAPP_LINK,
    TZ,
    DB_PATH,
    now_tz,
    format_dt,
    format_card,
    make_chat_sig,
    make_user_sig,
    with_qs,
    api_base_url,
    build_poll_link,
    make_ics,
)
from db_schema import CREATE_SQL

logging.basicConfig(level=logging.INFO, force=True)

OPTIONS = ["я в деле", "надо подумать", "точно не смогу"]
OPT_YES, OPT_MAYBE, OPT_NO = 0, 1, 2

REM_36H = "maybe_36h"
REM_3H = "yes_3h"
REM_UNPIN_23 = "unpin_23"

router = Router()


async def _can_bot_pin_messages(bot: Bot, chat_id: int) -> tuple[bool, str]:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id=chat_id, user_id=me.id)
        status = getattr(member, "status", "")
        if status == "creator":
            return True, "creator"
        if status == "administrator":
            if bool(getattr(member, "can_pin_messages", False)):
                return True, "administrator_with_can_pin_messages"
            return False, "administrator_without_can_pin_messages"
        return False, f"status={status}"
    except Exception as e:
        return False, f"check_failed:{type(e).__name__}"


async def _pin_message_if_allowed(
    bot: Bot,
    chat_id: int,
    message_id: int,
    kind: str,
    disable_notification: bool = False,
) -> tuple[bool, str]:
    allowed, reason = await _can_bot_pin_messages(bot, chat_id)
    if not allowed:
        logging.warning(
            "skip pin %s: chat_id=%s message_id=%s reason=%s",
            kind,
            chat_id,
            message_id,
            reason,
        )
        return False, reason
    try:
        await bot.pin_chat_message(
            chat_id=chat_id,
            message_id=message_id,
            disable_notification=disable_notification,
        )
        logging.info(
            "pin %s ok: chat_id=%s message_id=%s",
            kind,
            chat_id,
            message_id,
        )
        return True, "ok"
    except Exception as e:
        logging.warning(
            "pin %s failed: chat_id=%s message_id=%s error=%s",
            kind,
            chat_id,
            message_id,
            type(e).__name__,
        )
        return False, f"api_error:{type(e).__name__}"

def start_payload(text: str) -> str:
    if not text:
        return ""
    parts = text.strip().split(maxsplit=1)
    return parts[1] if len(parts) > 1 else ""

def parse_start_payload(payload: str):
    if not payload:
        return None, None, None
    parts = payload.split("_", 2)
    if len(parts) < 3:
        return None, None, None
    return parts[0], parts[1], parts[2]

def start_link(username: str, payload: str) -> str:
    return f"https://t.me/{username}?start={payload}"

def kb_private_webapp(chat_id: int, sig: str, mode: str, user_id: int):
    if not WEBAPP_URL:
        return ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⚠️ WEBAPP_URL не настроен")]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )

    params = {
        "chat_id": chat_id,
        "sig": sig,
        "user_id": user_id,
        "user_sig": make_user_sig(chat_id, user_id),
    }
    if mode == "calendar":
        params["mode"] = "calendar"
    elif mode == "manage":
        params["mode"] = "manage"
    url = with_qs(WEBAPP_URL, params)
    if mode == "calendar":
        label = "📅 Открыть календарь"
    elif mode == "manage":
        label = "🛠 Управление встречами"
    else:
        label = "➕ Создать встречу (форма)"
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=label, web_app=WebAppInfo(url=url))]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_new_event(chat_id: int, chat_type: str):
    sig = make_chat_sig(chat_id)

    if not WEBAPP_URL and not MINIAPP_LINK:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ Mini App не настроен. Задай WEBAPP_URL или MINIAPP_LINK", callback_data="noop")],
        ])

    if chat_type in ("group", "supergroup"):
        create_link = f"{MINIAPP_LINK}?startapp=create_{chat_id}_{sig}"
        calendar_link = f"{MINIAPP_LINK}?startapp=cal_{chat_id}_{sig}"
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать встречу (форма)", url=create_link)],
            [InlineKeyboardButton(text="📅 Открыть календарь", url=calendar_link)],
        ])

    if WEBAPP_URL:
        create_link = with_qs(WEBAPP_URL, {"chat_id": chat_id, "sig": sig})
        calendar_link = with_qs(WEBAPP_URL, {"mode": "calendar", "chat_id": chat_id, "sig": sig})

        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать встречу (форма)", web_app=WebAppInfo(url=create_link))],
            [InlineKeyboardButton(text="📅 Открыть календарь", web_app=WebAppInfo(url=calendar_link))],
        ])

    create_link = f"{MINIAPP_LINK}?startapp=create_{chat_id}_{sig}"
    calendar_link = f"{MINIAPP_LINK}?startapp=cal_{chat_id}_{sig}"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать встречу (форма)", url=create_link)],
        [InlineKeyboardButton(text="📅 Открыть календарь", url=calendar_link)],
    ])



def kb_event_actions(event_id: int):
    return None


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()

async def create_or_replace_reminders(db, event_id: int, dt: datetime):
    dt_local = dt.astimezone(TZ)
    t36 = dt - timedelta(hours=36)
    t3 = dt - timedelta(hours=3)
    t_unpin = dt_local.replace(hour=23, minute=0, second=0, microsecond=0)

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
    if t_unpin > now_tz():
        await db.execute(
            "INSERT OR IGNORE INTO reminders(event_id, kind, run_at_iso, sent) VALUES(?, ?, ?, 0)",
            (event_id, REM_UNPIN_23, t_unpin.isoformat()),
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

async def get_users_by_choice(db, poll_id: str, option_id: int):
    cur = await db.execute(
        """
        SELECT v.user_id, u.username, u.first_name, u.last_name
        FROM votes v
        LEFT JOIN users u ON u.user_id = v.user_id
        WHERE v.poll_id=? AND v.option_id=?
        """,
        (poll_id, option_id),
    )
    rows = await cur.fetchall()
    await cur.close()
    return rows

def md_escape(text: str) -> str:
    for ch in ("\\", "*", "_", "[", "]", "(", ")"):
        text = text.replace(ch, f"\\{ch}")
    return text

def display_name(username: Optional[str], first_name: Optional[str], last_name: Optional[str]) -> str:
    if username:
        return f"@{username}"
    name = " ".join(p for p in [first_name, last_name] if p)
    return name.strip() or "user"

def mention(uid: int, name: str = "user") -> str:
    return f"[{md_escape(name)}](tg://user?id={uid})"

async def reminders_worker(bot: Bot):
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                due = await get_due_reminders(db)
                for reminder_id, event_id, kind in due:
                    try:
                        cur = await db.execute(
                            "SELECT chat_id, poll_id, poll_message_id, card_message_id, dt_iso, title, cost, location, details "
                            "FROM events WHERE id=?",
                            (event_id,),
                        )
                        event = await cur.fetchone()
                        await cur.close()
                        if not event:
                            await mark_reminder_sent(db, reminder_id)
                            continue

                        chat_id, poll_id, poll_msg_id, card_msg_id, dt_iso, title, cost, location, details = event
                        dt = datetime.fromisoformat(dt_iso).astimezone(TZ)

                        poll_link = build_poll_link(chat_id, poll_msg_id)

                        if kind == REM_36H:
                            users = await get_users_by_choice(db, poll_id, OPT_MAYBE)
                            if users:
                                mentions = ", ".join(
                                    mention(uid, display_name(username, first_name, last_name))
                                    for uid, username, first_name, last_name in users[:30]
                                )
                                more = f" …и ещё {len(users)-30}" if len(users) > 30 else ""
                                text = (
                                    f"⏳ До встречи осталось ~36 часов.\n{mentions}{more}\n"
                                    f"**Вы как?** Переголосуйте, пожалуйста 🙂\n\n"
                                    f"📅 **{title}**\n"
                                    f"🕒 {format_dt(dt)}\n"
                                    f"📍 {location}\n"
                                    f"💸 {cost}"
                                )
                                if (details or "").strip():
                                    text += f"\n\n📝 {details.strip()}"
                                if poll_link:
                                    text += f"\n\nОпрос: {poll_link}"
                                try:
                                    await bot.send_message(
                                        chat_id,
                                        text,
                                        parse_mode=ParseMode.MARKDOWN,
                                        reply_to_message_id=int(poll_msg_id),
                                    )
                                except Exception:
                                    await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN)

                        elif kind == REM_3H:
                            users = await get_users_by_choice(db, poll_id, OPT_YES)
                            if users:
                                mentions = ", ".join(
                                    mention(uid, display_name(username, first_name, last_name))
                                    for uid, username, first_name, last_name in users[:30]
                                )
                                more = f" …и ещё {len(users)-30}" if len(users) > 30 else ""
                                text = (
                                    f"🔔 Через ~3 часа встреча!\n{mentions}{more}\n\n"
                                    f"📅 **{title}**\n"
                                    f"🕒 {format_dt(dt)}\n"
                                    f"📍 {location}\n"
                                    f"💸 {cost}"
                                )
                                if (details or "").strip():
                                    text += f"\n\n📝 {details.strip()}"
                                if poll_link:
                                    text += f"\n\nОпрос: {poll_link}"
                                await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN)
                        elif kind == REM_UNPIN_23:
                            for mid in (poll_msg_id, card_msg_id):
                                if mid:
                                    try:
                                        await bot.unpin_chat_message(chat_id=chat_id, message_id=int(mid))
                                    except Exception:
                                        pass

                        await mark_reminder_sent(db, reminder_id)
                    except TelegramForbiddenError as e:
                        logging.warning(
                            "skip reminder due to forbidden: reminder_id=%s event_id=%s kind=%s error=%s",
                            reminder_id,
                            event_id,
                            kind,
                            type(e).__name__,
                        )
                        await mark_reminder_sent(db, reminder_id)
                    except Exception:
                        logging.exception(
                            "failed processing reminder: reminder_id=%s event_id=%s kind=%s",
                            reminder_id,
                            event_id,
                            kind,
                        )

                await db.commit()
        except Exception:
            logging.exception("reminders_worker error")
        await asyncio.sleep(30)

async def delete_event(bot: Bot, event_id: int, actor_user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT chat_id, poll_message_id, card_message_id, poll_id, creator_user_id FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return "Событие не найдено."

        chat_id, poll_msg_id, card_msg_id, poll_id, creator_user_id = row
        if creator_user_id is not None and int(creator_user_id) != int(actor_user_id):
            return "Удалить может только создатель события."

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

    return "Удалено ✅"

@router.message(Command("start"))
async def cmd_start(message: Message):
    payload = start_payload(message.text)
    mode, chat_id, sig = parse_start_payload(payload)
    if mode in ("create", "cal", "manage") and chat_id and sig:
        try:
            chat_id_int = int(chat_id)
        except Exception:
            await message.answer("Некорректный chat_id в ссылке.")
            return
        if mode == "cal":
            mode_name = "calendar"
        elif mode == "manage":
            mode_name = "manage"
        else:
            mode_name = "create"
        await message.answer(
            "Открой форму кнопкой ниже:",
            reply_markup=kb_private_webapp(chat_id_int, sig, mode_name, message.from_user.id),
        )
        return

    await message.answer(
        "✅ Готово! Теперь я могу присылать тебе личные .ics-файлы.\n"
        "Вернись в чат и нажми «Добавить в мой календарь» под нужной встречей."
    )

@router.message(Command("new"))
async def cmd_new(message: Message, bot: Bot):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда работает в группах/супергруппах.")
        return
    logging.info(
        "cmd_new: chat_id=%s webapp_url=%s miniapp_link=%s",
        message.chat.id,
        WEBAPP_URL,
        MINIAPP_LINK,
    )
    try:
        bot_username = BOT_USERNAME
        if not bot_username:
            me = await bot.get_me()
            bot_username = me.username or ""
        if not bot_username:
            await message.answer("Не могу получить username бота для ссылки.")
            return

        sig = make_chat_sig(message.chat.id)
        create_link = start_link(bot_username, f"create_{message.chat.id}_{sig}")
        calendar_link = start_link(bot_username, f"cal_{message.chat.id}_{sig}")
        manage_link = start_link(bot_username, f"manage_{message.chat.id}_{sig}")

        await message.answer(
            "Открой в личке и создай встречу:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Создать встречу", url=create_link)],
                [InlineKeyboardButton(text="📅 Календарь", url=calendar_link)],
                [InlineKeyboardButton(text="🛠 Управление встречами", url=manage_link)],
            ]),
        )
    except Exception:
        logging.exception("cmd_new: failed to send keyboard")

@router.message(F.web_app_data)
async def on_webapp_data(message: Message, bot: Bot):
    """
    WebAppData может прийти НЕ из группы (если Mini App открыто по ссылке t.me/<bot>/<app>).
    Поэтому мы:
      - читаем payload
      - берём target_chat_id и sig из payload (или говорим пользователю открыть через кнопку в нужном чате)
      - проверяем подпись (sig)
      - публикуем карточку + опрос именно в target_chat_id
    """

    logging.info("web_app_data received from chat_id=%s user_id=%s", message.chat.id, message.from_user.id if message.from_user else None)
    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        logging.exception("web_app_data parse error")
        await message.answer("Не смог прочитать данные 😕")
        return
    logging.info("web_app_data payload: %s", data)

    # Запрос .ics из календаря (мини-приложение): отправляем .ics в личку
    if data.get("action") == "ics_request":
        event_id = int(data.get("event_id"))
        await _send_ics_to_user(bot, message.from_user.id, event_id, message)
        return

    # После редактирования через API просто обновляем карточку в чате
    if data.get("action") == "edited_via_api":
        event_id = int(data.get("event_id"))

        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT chat_id, card_message_id, dt_iso, title, cost, location, details FROM events WHERE id=?",
                (event_id,),
            )
            row = await cur.fetchone()
            await cur.close()
            if not row:
                await message.answer("Событие не найдено.")
                return

            chat_id, card_mid, dt_iso, title, cost, location, details = row
            dt = datetime.fromisoformat(dt_iso).astimezone(TZ)

            await create_or_replace_reminders(db, event_id, dt)
            await db.commit()

        try:
            if card_mid:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=int(card_mid),
                    text=format_card(dt, title, cost, location, details),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=None,
                )
        except Exception:
            pass

        try:
            await _send_ics_to_chat(
                bot,
                chat_id,
                event_id,
                reply_to_message_id=int(card_mid) if card_mid else None,
            )
        except Exception:
            pass

        await message.answer("✅ Обновил событие.")
        return

    if data.get("action") == "delete":
        event_id = int(data.get("event_id"))
        result = await delete_event(bot, event_id, message.from_user.id)
        await message.answer(result)
        return

    # Создание встречи из формы (Mini App)
    if data.get("action") == "create":
        logging.info("web_app_data action=create")
        # 1) Достаём целевой чат и подпись
        target_chat_id = data.get("chat_id")
        sig = data.get("sig")

        if not target_chat_id or not sig:
            logging.warning("missing chat_id/sig in payload: chat_id=%s sig=%s", target_chat_id, sig)
            await message.answer("Не вижу chat_id/sig. Открой форму кнопкой из нужного чата и попробуй ещё раз.")
            return

        try:
            target_chat_id = int(target_chat_id)
        except Exception:
            logging.warning("invalid chat_id in payload: %s", target_chat_id)
            await message.answer("Некорректный chat_id. Открой форму кнопкой из нужного чата.")
            return

        if make_chat_sig(target_chat_id) != str(sig):
            logging.warning("bad chat signature for chat_id=%s", target_chat_id)
            await message.answer("Подпись не совпала. Открой форму кнопкой из нужного чата и попробуй ещё раз.")
            return

        # 2) Достаём поля формы
        date = (data.get("date") or "").strip()
        time = (data.get("time") or "").strip()
        title = (data.get("title") or "Встреча").strip()
        cost = (data.get("cost") or "-").strip()
        location = (data.get("location") or "-").strip()
        details = (data.get("details") or "").strip()

        if not date or not time:
            logging.warning("missing date/time in payload: date=%s time=%s", date, time)
            await message.answer("Нужны дата и время.")
            return

        try:
            dt = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
        except Exception:
            logging.exception("bad date/time in payload: date=%s time=%s", date, time)
            await message.answer("Не понял дату/время. Проверь формат.")
            return

        if dt <= now_tz():
            logging.warning("attempt to create event in the past: dt=%s", dt.isoformat())
            await message.answer("Похоже, это время уже в прошлом.")
            return

        # 3) Публикуем карточку в целевом чате
        card_msg = await bot.send_message(
            target_chat_id,
            format_card(dt, title, cost, location, details),
            parse_mode=ParseMode.MARKDOWN
        )
        logging.info("card message sent: chat_id=%s message_id=%s", target_chat_id, card_msg.message_id)

        # 4) Публикуем опрос в целевом чате
        poll_msg = await bot.send_poll(
            chat_id=target_chat_id,
            question=f"{title} — {format_dt(dt)}",
            options=OPTIONS,
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        logging.info("poll sent: chat_id=%s poll_id=%s message_id=%s", target_chat_id, poll_msg.poll.id, poll_msg.message_id)
        poll_pin_ok, poll_pin_reason = await _pin_message_if_allowed(
            bot,
            target_chat_id,
            poll_msg.message_id,
            kind="poll",
        )

        # 5) Сохраняем в БД и планируем напоминания
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO events(
                    chat_id,
                    poll_id,
                    poll_message_id,
                    card_message_id,
                    creator_user_id,
                    dt_iso,
                    title,
                    cost,
                    location,
                    details,
                    created_at_iso
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_chat_id,
                    poll_msg.poll.id,
                    poll_msg.message_id,
                    card_msg.message_id,
                    message.from_user.id if message.from_user else None,
                    dt.isoformat(),
                    title,
                    cost,
                    location,
                    details,
                    now_tz().isoformat(),
                ),
            )

            cur = await db.execute(
                "SELECT id FROM events WHERE poll_id=?",
                (poll_msg.poll.id,)
            )
            row = await cur.fetchone()
            await cur.close()

            event_id = row[0]

            await create_or_replace_reminders(db, event_id, dt)
            await db.commit()
        logging.info("event saved: event_id=%s chat_id=%s", event_id, target_chat_id)

        # 6) Добавляем кнопки управления на карточку
        try:
            await bot.edit_message_reply_markup(
                chat_id=target_chat_id,
                message_id=card_msg.message_id,
                reply_markup=None,
            )
        except Exception:
            pass

        # 7) Автозакреп карточки
        card_pin_ok, card_pin_reason = await _pin_message_if_allowed(
            bot,
            target_chat_id,
            card_msg.message_id,
            kind="card",
            disable_notification=True,
        )

        # 7.1) Отправляем .ics в чат группы
        try:
            await _send_ics_to_chat(bot, target_chat_id, event_id)
        except Exception:
            pass

        # 8) Сообщение пользователю (в том чате, где он открыл mini app)
        await message.answer("✅ Встреча создана. Опрос отправлен в чат 👇")
        pin_failures = []
        if not poll_pin_ok:
            pin_failures.append(f"опрос: {poll_pin_reason}")
        if not card_pin_ok:
            pin_failures.append(f"карточка: {card_pin_reason}")
        if pin_failures:
            await message.answer(
                "⚠️ Не удалось закрепить сообщения в чате.\n"
                + "\n".join(pin_failures)
            )
        return

    await message.answer("Неизвестное действие.")


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
        await db.execute(
            "INSERT INTO users(user_id, username, first_name, last_name, updated_at_iso) VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, "
            "last_name=excluded.last_name, updated_at_iso=excluded.updated_at_iso",
            (user.id, user.username, user.first_name, user.last_name, now_tz().isoformat()),
        )
        await db.commit()

@router.callback_query(F.data.startswith("event:del:"))
async def on_event_delete(cb: CallbackQuery, bot: Bot):
    event_id = int(cb.data.split(":")[-1])
    result = await delete_event(bot, event_id, cb.from_user.id)
    await cb.answer(result, show_alert=result != "Удалено ✅")


async def _send_ics(
    bot: Bot,
    chat_id: int,
    event_id: int,
    caption: str,
    context_message: Optional[Message] = None,
    user_id: Optional[int] = None,
    allow_chat_link: bool = False,
    reply_to_message_id: Optional[int] = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT chat_id, dt_iso, title, cost, location, details FROM events WHERE id=?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()

    if not row:
        if context_message:
            await context_message.answer("Событие не найдено.")
        return

    event_chat_id, dt_iso, title, cost, location, details = row
    dt = datetime.fromisoformat(dt_iso).astimezone(TZ)

    description = f"Стоимость: {cost}"
    if (details or "").strip():
        description += f"\n\n{details.strip()}"

    ics_link = ""
    webcal_link = ""
    api_base = api_base_url()
    if api_base:
        if user_id is not None:
            user_sig = make_user_sig(int(event_chat_id), int(user_id))
            ics_link = (
                f"{api_base}/api/calendar/ics"
                f"?event_id={event_id}&user_id={user_id}&user_sig={user_sig}"
            )
        elif allow_chat_link:
            chat_sig = make_chat_sig(int(event_chat_id))
            ics_link = f"{api_base}/api/calendar/ics?event_id={event_id}&chat_sig={chat_sig}"

        if ics_link.startswith("https://"):
            webcal_link = "webcal://" + ics_link[len("https://"):]
        elif ics_link.startswith("http://"):
            webcal_link = "webcal://" + ics_link[len("http://"):]

    if ics_link:
        caption += (
            "\n\nЕсли iPhone не открывает файл, попробуйте "
            f"[ссылку для скачивания]({ics_link})."
        )
    if webcal_link:
        caption += f"\nИли откройте [webcal-ссылку]({webcal_link})."

    ics_text = make_ics(dt, title, location, description)
    filename = None
    try:
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".ics", delete=False, encoding="utf-8") as f:
            f.write(ics_text)
            filename = f.name

        await bot.send_document(
            chat_id=chat_id,
            document=FSInputFile(filename),
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            reply_to_message_id=reply_to_message_id,
        )
    except TelegramForbiddenError:
        if context_message:
            await context_message.answer("Я не могу написать тебе в личку. Открой бота и нажми /start, затем повтори.")
        else:
            # если нет контекста — молча
            pass
    finally:
        if filename:
            try:
                os.unlink(filename)
            except Exception:
                pass

async def _send_ics_to_user(bot: Bot, user_id: int, event_id: int, context_message: Optional[Message] = None):
    await _send_ics(
        bot,
        user_id,
        event_id,
        caption=(
            "📎 Файл события для календаря.\n"
            "Открой файл и нажми «Добавить в календарь»."
        ),
        context_message=context_message,
        user_id=user_id,
    )

async def _send_ics_to_chat(
    bot: Bot,
    chat_id: int,
    event_id: int,
    reply_to_message_id: Optional[int] = None,
):
    await _send_ics(
        bot,
        chat_id,
        event_id,
        caption=(
            "📎 Файл события для календаря.\n"
            "Открой файл и нажми «Добавить в календарь»."
        ),
        context_message=None,
        allow_chat_link=True,
        reply_to_message_id=reply_to_message_id,
    )

@router.callback_query(F.data.startswith("event:ics:"))
async def on_event_ics(cb: CallbackQuery, bot: Bot):
    event_id = int(cb.data.split(":")[-1])
    try:
        await _send_ics_to_user(bot, cb.from_user.id, event_id)
        await cb.answer("Отправил в личку ✅")
    except TelegramForbiddenError:
        await cb.answer("Открой бота в личке и нажми /start, затем повтори.", show_alert=True)

async def main():
    await init_db()

    from aiogram.client.default import DefaultBotProperties

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    )

    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(reminders_worker(bot))

    logging.info("Bot started")
    await dp.start_polling(bot)



if __name__ == "__main__":
    asyncio.run(main())
