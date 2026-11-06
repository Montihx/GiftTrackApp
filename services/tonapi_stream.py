# services/tonapi_stream.py — события в реальном времени: SSE-поток переводов NFT от TonAPI
"""
Полный скан ~1,5 млн предметов на free-тарифе (1 RPS) занимает часы, поэтому события берём
из потока, а скан оставляем для floors/атрибутов и как страховку от пропусков.

  GET {tonapi_url}/v2/sse/accounts/transactions?accounts=ALL&operations=0x5fcc3d14

присылает каждую финализированную транзакцию с op «NFT transfer» по всей сети TON:
  data: {"account_id": "0:<адрес NFT>", "lt": ..., "tx_hash": "..."}
Листинг (NFT уходит на контракт продажи), продажа (контракт продажи отдаёт NFT покупателю),
снятие с продажи и перевод — всё это переводы NFT. Для каждого события:
  1. адрес NFT ищется в processed_nfts (локально, без API) — чужие NFT отбрасываются;
  2. через tonapi_stream_delay секунд (индексатор TonAPI успевает обновить NFT) —
     GET /v2/nfts/{addr} (один запрос через общий rate limiter);
  3. TonApiSync.apply_item: сравнение с прошлым состоянием -> «ЛИСТИНГ» / «GIFT SOLD!», флор вниз.
Поток платной квоты почти не тратит: запрос делается только по переводам наших подарков.
Обрыв соединения — переподключение с backoff; пропущенное за время обрыва догонит скан.
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
import orjson
from loguru import logger

from core.config import Settings

NFT_TRANSFER_OP = "0x5fcc3d14"
QUEUE_LIMIT = 10_000


class TonApiStream:
    def __init__(self, sync: Any, settings: Settings) -> None:
        self.sync = sync  # TonApiSync: client, is_tracked_item, apply_item
        self.settings = settings
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self._pending: set[str] = set()
        self.stats = {"events": 0, "tracked": 0, "applied": 0, "dropped": 0, "reconnects": 0, "errors": 0}

    def handle_data(self, data: str) -> None:
        """Строка `data:` из SSE -> адрес NFT в очередь (без дублей, пока не обработан)."""
        try:
            account = orjson.loads(data).get("account_id")
        except (orjson.JSONDecodeError, AttributeError):
            return
        if not account:
            return
        self.stats["events"] += 1
        account = str(account).lower()
        if account in self._pending:
            return
        try:
            self.queue.put_nowait(account)
            self._pending.add(account)
        except asyncio.QueueFull:
            self.stats["dropped"] += 1

    async def process_one(self, address: str) -> bool:
        """Обработать одно событие. True — подарок наш и состояние применено."""
        self._pending.discard(address)
        if not await self.sync.is_tracked_item(address):
            return False
        self.stats["tracked"] += 1
        if self.settings.tonapi_stream_delay:
            await asyncio.sleep(self.settings.tonapi_stream_delay)
        raw = await self.sync.client.fetch_nft(address)
        if not raw:
            return False
        diff = await self.sync.apply_item(raw)
        if diff is not None:
            self.stats["applied"] += 1
        return diff is not None

    async def _worker(self) -> None:
        while True:
            address = await self.queue.get()
            try:
                await self.process_one(address)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — одно событие не роняет поток
                self.stats["errors"] += 1
                logger.warning(f"[tonapi_stream] {address[:12]}…: {exc!r}")
                async with self.sync.lock:
                    await self.sync.session.rollback()

    async def _listen(self, stop_event: asyncio.Event) -> None:
        url = f"{self.settings.tonapi_url.rstrip('/')}/v2/sse/accounts/transactions"
        params = {"accounts": "ALL", "operations": NFT_TRANSFER_OP}
        headers = {"Authorization": f"Bearer {self.settings.tonapi_key}", "Accept": "text/event-stream"}
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=120)
        async with aiohttp.ClientSession(timeout=timeout, headers=headers, trust_env=True) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:200]
                    raise RuntimeError(f"SSE HTTP {resp.status}: {body}")
                logger.info("[tonapi_stream] подключён к потоку переводов NFT")
                async for line in resp.content:
                    if stop_event.is_set():
                        return
                    text = line.decode(errors="ignore").strip()
                    if text.startswith("data:"):
                        self.handle_data(text[5:].strip())

    async def run(self, stop_event: asyncio.Event) -> None:
        worker = asyncio.create_task(self._worker())
        backoff = 1.0
        try:
            while not stop_event.is_set():
                try:
                    await self._listen(stop_event)
                    backoff = 1.0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 — обрыв/ошибка: переподключаемся
                    self.stats["reconnects"] += 1
                    logger.warning(f"[tonapi_stream] поток прервался: {exc!r}; повтор через {backoff:.0f}s")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60.0)
        finally:
            worker.cancel()
            logger.info(f"[tonapi_stream] остановлен, статистика: {self.stats}")
