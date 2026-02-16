import os
import hashlib
import hmac
from datetime import datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo
from typing import Optional

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TZ = ZoneInfo("Europe/Moscow")
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "calendar_bot.sqlite3"))

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN env var")

BOT_USERNAME = os.getenv("BOT_USERNAME", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://bot01.ficsh.ru/event-form")
MINIAPP_LINK = os.getenv("MINIAPP_LINK", "")
API_BASE_URL = os.getenv("API_BASE_URL", "")


def now_tz() -> datetime:
    return datetime.now(tz=TZ)


def format_dt(dt: datetime) -> str:
    return dt.strftime("%d-%m-%Y %H:%M")


def format_card(dt: datetime, title: str, cost: str, location: str, details: str = "") -> str:
    text = (
        f"📅 **{title}**\n"
        f"🕒 {format_dt(dt)}\n"
        f"📍 {location}\n"
        f"💸 {cost}"
    )
    if (details or "").strip():
        text += f"\n\n📝 {details.strip()}"
    return text


def make_chat_sig(chat_id: int) -> str:
    key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    msg = str(chat_id).encode("utf-8")
    full = hmac.new(key, msg, hashlib.sha256).hexdigest()
    return full[:20]


def make_user_sig(chat_id: int, user_id: int) -> str:
    key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    msg = f"{chat_id}:{user_id}".encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def with_qs(url: str, params: dict) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query.update(params)
    return urlunparse(parsed._replace(query=urlencode(query)))


def api_base_url() -> str:
    if API_BASE_URL:
        return API_BASE_URL.rstrip("/")
    if WEBAPP_URL:
        parsed = urlparse(WEBAPP_URL)
        if parsed.scheme and parsed.netloc:
            return urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")).rstrip("/")
    return ""


def build_poll_link(chat_id: int, poll_message_id: int) -> Optional[str]:
    if str(chat_id).startswith("-100"):
        internal = int(str(abs(chat_id))[3:])
        return f"https://t.me/c/{internal}/{poll_message_id}"
    return None


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
