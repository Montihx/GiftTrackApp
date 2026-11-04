# /workspace/bot/bot_main.py — точка входа для сервиса bot
"""
Точка входа для сервиса bot:
    python -m bot.bot_main

Запускает Aiogram бота (polling) и NotificationDispatcher параллельно.
"""
from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger

from bot.handlers import router
from core.config import get_settings
from core.logging import setup_logging
from core.redis_client import create_redis
from core.shutdown import ShutdownManager
from db.engine import dispose_engine, session_scope
from services.dispatcher import NotificationDispatcher
from services.resolver.resolver import OwnerResolver


async def _main() -> None:
    setup_logging(service_name="bot")
    settings = get_settings()

    if not settings.bot_token:
        logger.error("BOT_TOKEN не задан — бот не запустится")
        return

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    redis = create_redis()
    sm = ShutdownManager(timeout=15)

    # Инжектим session в handlers через middleware
    from aiogram import BaseMiddleware
    from aiogram.types import TelegramObject

    class SessionMiddleware(BaseMiddleware):
        async def __call__(self, handler, event: TelegramObject, data: dict):
            async with session_scope() as session:
                data["session"] = session
                return await handler(event, data)

    dp.message.middleware(SessionMiddleware())

    async def run_dispatcher() -> None:
        async with session_scope() as session:
            resolver = OwnerResolver(session=session, redis=redis, settings=settings)
            dispatcher = NotificationDispatcher(
                bot=bot, session=session, redis=redis, resolver=resolver, settings=settings
            )
            await dispatcher.run_loop(sm._stop_event)  # noqa: SLF001

    async def run_bot() -> None:
        logger.info("Бот запущен (polling)")
        await dp.start_polling(bot)

    async def combined() -> None:
        dispatcher_task = asyncio.create_task(run_dispatcher())
        bot_task = asyncio.create_task(run_bot())
        await asyncio.gather(dispatcher_task, bot_task)

    try:
        await sm.run(combined)
    finally:
        await bot.session.close()
        await redis.aclose()
        await dispose_engine()
        logger.info("Бот остановлен")


if __name__ == "__main__":
    asyncio.run(_main())
