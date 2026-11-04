# /workspace/bot/market_main.py — точка входа для сервиса listener-market
"""
Точка входа для сервиса listener-market:
    python -m bot.market_main

Запускает MarketListener в бесконечном цикле с graceful shutdown.
"""
from __future__ import annotations

import asyncio

from core.logging import setup_logging
from core.redis_client import create_redis
from core.shutdown import ShutdownManager
from db.engine import dispose_engine, session_scope
from services.market.listener import MarketListener


async def _main() -> None:
    setup_logging(service_name="listener-market")
    redis = create_redis()
    sm = ShutdownManager(timeout=15)

    async def run() -> None:
        async with session_scope() as session:
            listener = MarketListener(session=session, redis=redis)
            await listener.run_loop(sm._stop_event)  # noqa: SLF001

    try:
        await sm.run(run)
    finally:
        await redis.aclose()
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(_main())
