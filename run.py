import asyncio
import os
import logging
import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from bot import router, init_db, reminders_worker
from server import app as fastapi_app
from core import BOT_TOKEN

logging.basicConfig(level=logging.INFO, force=True)


async def main():
    await init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )
    dp = Dispatcher()
    dp.include_router(router)

    port = int(os.environ.get("PORT", 6000))
    config = uvicorn.Config(fastapi_app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)

    logging.info("Starting bot + server on port %d", port)
    await asyncio.gather(
        server.serve(),
        dp.start_polling(bot),
        reminders_worker(bot),
    )


if __name__ == "__main__":
    asyncio.run(main())
