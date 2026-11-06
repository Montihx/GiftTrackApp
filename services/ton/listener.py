# /workspace/services/ton/listener.py — основной цикл TON-листенера
"""
TonListener: мониторинг mint/transfer событий через Tonviewer API.

Поллинг каждые settings.ton_poll_interval секунд (по умолчанию 3 сек).
Использует REST API вместо WebSocket для простоты и надёжности.

API endpoint:
    GET https://tonviewer.com/api/v4/accounts/{address}/transactions?limit=100

Пример ответа (упрощённо):
    [
        {
            "hash": "abc123...",
            "time": 1699999999,
            "in_msg": {
                "source": "EQ...",
                "destination": "EQ...",
                "op": 1607361556,
                "decoded_body": {
                    "type": "nft_transfer",
                    "new_owner": "EQ...new_owner"
                }
            }
        }
    ]
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
import orjson
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import STREAM_NFT_UPGRADES, cache_get_json, cache_set_json, stream_publish
from core.retry import with_retry
from services.collection_discovery import _http_request
from services.ton.events import (
    BlockEvents,
    MintEvent,
    TransferEvent,
    mint_to_stream_payload,
    transfer_to_stream_payload,
)
from services.ton.parsing import parse_tonviewer_transactions
from services.ton.state import TonState


class TonListener:
    """
    Слушатель событий TON через Tonviewer API.

    DI: session (AsyncSession), redis (Redis client), settings (Settings).
    """

    TONVIEWER_API = "https://tonviewer.com/api/v4"
    CACHE_LAST_TS_KEY = "ton_listener:last_ts:{collection}"

    def __init__(
        self,
        session: AsyncSession,
        redis: Any,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings or get_settings()
        self.state = TonState(session)
        self._rate = asyncio.Semaphore(self.settings.api_max_concurrency)
        self.stats = {"mints": 0, "transfers": 0, "errors": 0, "empty": 0}

    async def process_collections(self) -> None:
        """
        Один цикл обработки всех коллекций из БД.

        Для каждой коллекции:
        1. Получаем транзакции от Tonviewer
        2. Парсим в BlockEvents
        3. Обрабатываем mint/transfer
        4. Публикуем в Redis Stream
        5. Сохраняем последний timestamp
        """
        from db.models import Collection
        from sqlalchemy import select

        # Получаем все коллекции с EQ-адресами
        stmt = select(Collection.id, Collection.name, Collection.contract_address).where(
            ~Collection.contract_address.startswith("tg_gift:")
        )
        rows = (await self.session.execute(stmt)).all()

        if not rows:
            logger.debug("process_collections: коллекций с EQ-адресом нет")
            return

        for coll_id, coll_name, contract_addr in rows:
            try:
                await self._process_collection(str(coll_id), coll_name, contract_addr)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Ошибка обработки коллекции {coll_name}: {exc!r}")
                self.stats["errors"] += 1
    async def _process_collection(
        self, collection_id: str, collection_name: str, contract_addr: str
    ) -> None:
        state_key = f"{TonState.STATE_KEY}:{collection_id}"
        cache_key = self.CACHE_LAST_TS_KEY.format(collection=collection_id)

        last_ts = await cache_get_json(self.redis, cache_key)
        if last_ts is None:
            last_ts = await self.state.load_last_timestamp(state_key)
        last_ts = int(last_ts or 0)

        transactions = await self._fetch_transactions(contract_addr, limit=100)
        if not transactions:
            self.stats["empty"] += 1
            return

        fresh = [tx for tx in transactions if int(tx.get("time", 0)) > last_ts]
        if not fresh:
            self.stats["empty"] += 1
            return

        events = parse_tonviewer_transactions(fresh, contract_addr, seqno=last_ts)
        if events.is_empty():
            self.stats["empty"] += 1
            return

        max_ts = last_ts
        for mint in events.mints:
            await self._handle_mint(mint, collection_id)
            max_ts = max(max_ts, int(mint.timestamp.timestamp()))
            self.stats["mints"] += 1

        for transfer in events.transfers:
            await self._handle_transfer(transfer)
            max_ts = max(max_ts, int(transfer.timestamp.timestamp()))
            self.stats["transfers"] += 1

        await cache_set_json(self.redis, cache_key, max_ts, ttl=86400)
        await self.state.save_last_timestamp(max_ts, state_key)
        await self.session.commit()

        logger.info(
            f"[{collection_name}] mints={len(events.mints)}, transfers={len(events.transfers)}"
        )


    async def _handle_mint(self, event: MintEvent, collection_id: str) -> None:
        """Обработка mint-события: скачивание metadata, сохранение в БД, публикация в Stream."""
        # Скачиваем metadata (если есть URI)
        metadata: dict[str, Any] | None = None
        if event.metadata_uri:
            try:
                metadata = await self._fetch_metadata(event.metadata_uri)
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"Не удалось скачать metadata для {event.nft_address}: {exc!r}")

        # Сохраняем в БД
        await self.state.upsert_mint(event, metadata)

        # Публикуем в Redis Stream
        payload = mint_to_stream_payload(event, metadata)
        payload["collection_id"] = collection_id
        await stream_publish(self.redis, STREAM_NFT_UPGRADES, payload)

    async def _handle_transfer(self, event: TransferEvent) -> None:
        """Обработка transfer-события: запись в БД и Stream."""
        # Сохраняем в БД
        await self.state.record_transfer(event)

        # Публикуем в Redis Stream
        payload = transfer_to_stream_payload(event)
        await stream_publish(self.redis, STREAM_NFT_UPGRADES, payload)

    async def _fetch_transactions(self, address: str, limit: int = 100) -> list[dict[str, Any]]:
        """Получение транзакций от Tonviewer API."""
        url = f"{self.TONVIEWER_API}/accounts/{address}/transactions"
        params = {"limit": str(limit)}
        headers = {}
        if self.settings.tonviewer_api_key:
            headers["Authorization"] = f"Bearer {self.settings.tonviewer_api_key}"

        async def _do() -> list[dict[str, Any]]:
            raw = await _http_request(
                self._rate, "GET", url, params=params, headers=headers,
                context=f"tonviewer.transactions({address[:12]}…)",
            )
            payload = orjson.loads(raw)
            return payload if isinstance(payload, list) else []

        return await with_retry(_do, context=f"fetch_transactions({address[:12]}…)")

    async def _fetch_metadata(self, uri: str) -> dict[str, Any]:
        """Скачивание metadata JSON по URI (https/ipfs)."""
        import re

        # Конвертация ipfs:// в https://ipfs.io/ipfs/
        if uri.startswith("ipfs://"):
            ipfs_hash = re.sub(r"^ipfs://(?:ipfs/)?", "", uri)
            uri = f"https://ipfs.io/ipfs/{ipfs_hash}"

        async def _do() -> dict[str, Any]:
            raw = await _http_request(
                self._rate, "GET", uri, context=f"metadata({uri[:50]}…)",
            )
            return orjson.loads(raw)

        return await with_retry(_do, context=f"fetch_metadata({uri[:50]}…)")

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """
        Основной цикл: обработка всех коллекций каждые settings.ton_poll_interval секунд.

        Останавливается по stop_event (ShutdownManager).
        """
        if not self.settings.tonviewer_enabled:
            logger.warning(
                "[ton_listener] Tonviewer отключён (TONVIEWER_ENABLED=false) — данные идут "
                "через TonAPI-синхронизацию в listener-market; ожидаю остановки"
            )
            await stop_event.wait()
            return
        logger.info(f"[ton_listener] старт, интервал {self.settings.ton_poll_interval}s")

        while not stop_event.is_set():
            try:
                await self.process_collections()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[ton_listener] итерация упала: {exc!r}", exc_info=True)

            # Ждём интервал ИЛИ сигнал остановки
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.settings.ton_poll_interval)
            except asyncio.TimeoutError:
                pass

        logger.info(f"[ton_listener] остановлен, статистика: {self.stats}")
