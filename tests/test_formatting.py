import logging
import os
import sys
import types
import unittest
from datetime import datetime

# Formatting does not need a live Telegram token or the optional dotenv package.
os.environ.setdefault("BOT_TOKEN", "test-token")
if "dotenv" not in sys.modules:
    try:
        __import__("dotenv")
    except ImportError:
        dotenv = types.ModuleType("dotenv")
        dotenv.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv

from core import format_card, format_event_dt
from log_config import ConciseFormatter


class FormattingTests(unittest.TestCase):
    def test_all_weekdays_and_card(self):
        for day, weekday in enumerate(("пн", "вт", "ср", "чт", "пт", "сб", "вс"), 16):
            dt = datetime(2026, 11, day, 19, 30)
            expected = f"{day:02d}-11-2026 ({weekday}) 19:30"
            self.assertEqual(format_event_dt(dt), expected)
            self.assertIn(expected, format_card(dt, "Встреча", "100", "Кафе"))

    def test_exception_chain_is_concise(self):
        try:
            try:
                raise TimeoutError("превышено время ожидания")
            except TimeoutError as error:
                raise RuntimeError("не удалось отправить сообщение") from error
        except RuntimeError:
            record = logging.LogRecord("test", logging.ERROR, __file__, 1, "Напоминание event_id=74", (), sys.exc_info())
        result = ConciseFormatter("%(levelname)s: %(message)s").format(record)
        self.assertIn("event_id=74", result)
        self.assertIn("RuntimeError: не удалось отправить сообщение", result)
        self.assertIn("TimeoutError: превышено время ожидания", result)
        self.assertNotIn("Traceback", result)
        self.assertNotIn('File "', result)


if __name__ == "__main__":
    unittest.main()
