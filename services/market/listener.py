# /workspace/services/market/listener.py — цикл скана TonAPI в сервисе listener-market
"""
MarketListener: запускает TonApiSync (services/tonapi_sync.py) каждые tonapi_poll_interval и, если включено, поток
событий TonApiStream (services/tonapi_stream.py). Скан считает флоры, количество и атрибуты коллекций; публикация листингов
и апгрейдов из блокчейна выключена по умолчанию (ONCHAIN_*), продажи — по ONCHAIN_SALES_ENABLED.

Без TONAPI_KEY сервис ничего не делает: прежний источник (GetGems GraphQL) платный и удалён.
"""
from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from services.tonapi_sync import TonApiSync


class MarketListener:
    """DI: session (AsyncSession), redis (Redis client), settings (Settings)."""

    def __init__(self, session: AsyncSession, redis: Any, settings: Settings | None = None) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings or get_settings()
        self._tonapi_sync = TonApiSync(session, redis, self.settings) if self.settings.tonapi_enabled else None

    async def process_collections(self) -> None:
        """Один цикл скана всех коллекций."""
        if self._tonapi_sync is not None:
            await self._tonapi_sync.process_collections()

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        if self._tonapi_sync is None:
            logger.warning("[market_listener] TONAPI_KEY не задан — скан блокчейна не запускается")
            await stop_event.wait()
            return
        interval = self.settings.tonapi_poll_interval
        logger.info(f"[market_listener] старт, источник TonAPI, интервал {interval}s")

        stream_task: asyncio.Task[None] | None = None
        if self.settings.tonapi_stream_enabled:
            from services.tonapi_stream import TonApiStream

            stream_task = asyncio.create_task(TonApiStream(self._tonapi_sync, self.settings).run(stop_event))

        while not stop_event.is_set():
            try:
                await self.process_collections()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[market_listener] итерация упала: {exc!r}", exc_info=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

        if stream_task is not None:
            stream_task.cancel()
            try:
                await stream_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        logger.info("[market_listener] остановлен")
