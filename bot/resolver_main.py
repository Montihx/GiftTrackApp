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


async def _telethon_worker(redis, stop_event: asyncio.Event) -> None:
    """
    Фоновый джоб: берёт TON-адреса из очереди telethon_resolve_queue,
    сканирует bio пользователей через Telethon на наличие ton:// ссылок.

    Требует авторизации userbot (API_ID, API_HASH, PHONE в .env).
    """
    settings = get_settings()
    if not (settings.api_id and settings.api_hash and settings.phone):
        logger.warning("Telethon не настроен (API_ID/API_HASH/PHONE пусты) — worker не запущен")
        return

    from telethon import TelegramClient

    client = TelegramClient("resolver_session", settings.api_id, settings.api_hash)
    await client.start(phone=settings.phone)
    logger.info("Telethon worker запущен")

    while not stop_event.is_set():
        ton_address = await redis.brpop("telethon_resolve_queue", timeout=5)
        if ton_address is None:
            continue

        ton_address = ton_address[1].decode()
        logger.debug(f"Telethon scan: {ton_address[:12]}…")
        # TODO: реализовать скан bio пользователей по ton_address
        # (поиск пользователей, у которых в bio есть ton:// или адрес кошелька)

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
    asyncio.run(_main())
