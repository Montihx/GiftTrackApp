# /workspace/bot/resolver_main.py — точка входа для сервиса resolver
"""
Точка входа для сервиса resolver:
    python -m bot.resolver_main

Запускает фоновый джоб обогащения кэша через Telethon (скан bio пользователей).
Запускается только если API_ID / API_HASH / PHONE заданы в .env.
"""
from __future__ import annotations

import asyncio

from loguru import logger

from core.config import get_settings
from core.logging import setup_logging
from core.redis_client import create_redis
from core.shutdown import ShutdownManager
from db.engine import dispose_engine, session_scope
from services.resolver.telethon_worker import BioScanner


async def _telethon_worker(redis, stop_event: asyncio.Event) -> None:
    """
    Cold path: скан bio известных пользователей через Telethon + разбор очереди
    telethon_resolve_queue (см. services/resolver/telethon_worker.py).

    Требует авторизованной сессии. Первый вход (код из Telegram, один раз):
        docker compose run --rm -it resolver python -m bot.resolver_main --login
    """
    settings = get_settings()
    if not (settings.api_id and settings.api_hash and settings.phone):
        logger.warning("Telethon не настроен (API_ID/API_HASH/PHONE пусты) — worker простаивает")
        await stop_event.wait()  # не завершаемся, чтобы контейнер не уходил в crash-loop
        return

    client = _make_client(settings)
    await client.connect()
    if not await client.is_user_authorized():
        logger.error(
            "Telethon не авторизован. Выполните один раз: "
            "docker compose run --rm -it resolver python -m bot.resolver_main --login"
        )
        await stop_event.wait()
        await client.disconnect()
        return

    logger.info("Telethon worker запущен")
    scanner = BioScanner(client, redis, session_scope, settings)
    try:
        await scanner.run(stop_event)
    finally:
        await client.disconnect()


def _make_client(settings):
    from pathlib import Path

    from telethon import TelegramClient

    Path(settings.telethon_session).parent.mkdir(parents=True, exist_ok=True)
    return TelegramClient(settings.telethon_session, settings.api_id, settings.api_hash)


async def _login() -> None:
    """Интерактивный вход userbot (код из Telegram / пароль 2FA)."""
    settings = get_settings()
    client = _make_client(settings)
    await client.start(phone=settings.phone)
    me = await client.get_me()
    print(f"Авторизован как @{me.username or me.id}. Сессия сохранена.")
    await client.disconnect()


async def _main() -> None:
    setup_logging(service_name="resolver")
    redis = create_redis()
    sm = ShutdownManager(timeout=15)

    async def run() -> None:
        async with session_scope():
            await _telethon_worker(redis, sm._stop_event)  # noqa: SLF001

    try:
        await sm.run(run)
    finally:
        await redis.aclose()
        await dispose_engine()


if __name__ == "__main__":
    import sys

    asyncio.run(_login() if "--login" in sys.argv else _main())
