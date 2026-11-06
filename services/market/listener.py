# /workspace/services/market/listener.py — поллинг маркетплейсов на новые листинги
"""
MarketListener: мониторинг новых листингов через REST API маркетплейсов.

Поллинг каждые settings.market_poll_interval секунд (по умолчанию 5 сек).

GetGems GraphQL (основной):
    POST https://api.getgems.io/graphql
    query RecentListings($address: Address!, $limit: Int!) {
      nftItems(input: {
        collectionAddress: $address,
        sortedBy: CREATED_AT,
        sortDirection: DESC,
        limit: $limit
      }) {
        edges {
          node {
            address
            index
            collection { address }
            metadata { attributes { name value } }
            minPrice { amount { value } }
            owner { address }
            createdAt
          }
        }
      }
    }

Пример ответа:
    {"data": {"nftItems": {"edges": [{"node": {
        "address": "EQnft123",
        "index": "8681",
        "collection": {"address": "EQcollection"},
        "metadata": {"attributes": [
            {"name": "Model", "value": "Obelisk"},
            {"name": "Background", "value": "Silver Blue"}
        ]},
        "minPrice": {"amount": {"value": "2400.0"}},
        "owner": {"address": "EQseller"},
        "createdAt": "2026-11-04T18:00:00Z"
    }}]}}}
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import orjson
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import STREAM_NFT_LISTINGS, cache_get_json, cache_set_json, stream_publish
from core.retry import with_retry
from services.collection_discovery import _http_request
from services.market.events import ListingEvent, listing_to_stream_payload
from services.tonapi_sync import TonApiSync

GQL_RECENT_LISTINGS = """
query RecentListings($address: Address!, $limit: Int!) {
  nftItems(input: {
    collectionAddress: $address,
    sortedBy: CREATED_AT,
    sortDirection: DESC,
    limit: $limit
  }) {
    edges {
      node {
        address
        index
        collection { address }
        metadata { attributes { name value } }
        minPrice { amount { value } }
        owner { address }
        createdAt
      }
    }
  }
}
"""


class MarketListener:
    """
    Слушатель листингов на маркетплейсах.

    DI: session (AsyncSession), redis (Redis client), settings (Settings).
    """

    CACHE_LAST_LISTING_KEY = "market_listener:last_listing:{collection}"

    def __init__(
        self,
        session: AsyncSession,
        redis: Any,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings or get_settings()
        self._rate = asyncio.Semaphore(self.settings.api_max_concurrency)
        self.stats = {"listings": 0, "errors": 0, "empty": 0}
        # TonAPI primary при наличии ключа (A9): снимки items вместо GetGems, опрос каждые tonapi_poll_interval
        self._tonapi_sync = TonApiSync(session, redis, self.settings) if self.settings.tonapi_enabled else None

    async def process_collections(self) -> None:
        """Один цикл обработки всех коллекций из БД."""
        if self._tonapi_sync is not None:
            await self._tonapi_sync.process_collections()
            return
        from db.models import Collection

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
        """Обработка одной коллекции: получение листингов, парсинг, публикация."""
        cache_key = self.CACHE_LAST_LISTING_KEY.format(collection=collection_id)
        last_seen_raw = await cache_get_json(self.redis, cache_key)

        # Парсим last_seen в datetime (None → эпоха UTC)
        if last_seen_raw:
            try:
                last_seen_dt = dt.datetime.fromisoformat(
                    last_seen_raw.replace("Z", "+00:00")
                )
            except ValueError:
                last_seen_dt = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        else:
            last_seen_dt = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)

        listings = await self._fetch_listings(contract_addr, limit=50)
        if not listings:
            self.stats["empty"] += 1
            return

        # Сравниваем datetime-объекты, не строки
        new_listings = []
        for l in listings:
            created_raw = l.get("createdAt") or ""
            try:
                created_dt = dt.datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                if created_dt > last_seen_dt:
                    new_listings.append(l)
            except ValueError:
                continue

        if not new_listings:
            self.stats["empty"] += 1
            return

        max_dt = last_seen_dt
        for listing in new_listings:
            event = self._parse_listing(listing, contract_addr)
            if event is None:
                continue

            payload = listing_to_stream_payload(event)
            payload["collection_id"] = collection_id
            await stream_publish(self.redis, STREAM_NFT_LISTINGS, payload)

            if event.listed_at > max_dt:
                max_dt = event.listed_at
            self.stats["listings"] += 1

        # Сохраняем max_created в ISO-формате с Z-суффиксом
        max_created_str = max_dt.isoformat().replace("+00:00", "Z")
        await cache_set_json(self.redis, cache_key, max_created_str, ttl=86400)
        logger.info(f"[{collection_name}] новых листингов: {len(new_listings)}")


    def _parse_listing(self, node: dict[str, Any], contract_addr: str) -> ListingEvent | None:
        """Парсинг GraphQL-ноды листинга в ListingEvent."""
        nft_address = node.get("address")
        if not nft_address:
            return None

        index_raw = node.get("index")
        index = int(index_raw) if index_raw else 0

        price_raw = (((node.get("minPrice") or {}).get("amount") or {}).get("value")) or "0"
        try:
            price_ton = float(price_raw)
        except ValueError:
            price_ton = 0.0

        seller = (node.get("owner") or {}).get("address") or ""

        attributes = {}
        for attr in ((node.get("metadata") or {}).get("attributes") or []):
            name = (attr.get("name") or "").lower()
            value = attr.get("value")
            if name and value:
                attributes[name] = value

        created_raw = node.get("createdAt") or ""
        try:
            listed_at = dt.datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
        except ValueError:
            listed_at = dt.datetime.now(dt.timezone.utc)

        return ListingEvent(
            nft_address=nft_address,
            nft_index=index,
            collection_address=contract_addr,
            marketplace="getgems",
            price_ton=price_ton,
            seller_ton=seller,
            attributes=attributes,
            listed_at=listed_at,
        )

    async def _fetch_listings(self, address: str, limit: int = 50) -> list[dict[str, Any]]:
        """Получение последних листингов от GetGems GraphQL."""

        async def _do() -> list[dict[str, Any]]:
            raw = await _http_request(
                self._rate,
                "POST",
                self.settings.getgems_api_url,
                json_body={"query": GQL_RECENT_LISTINGS, "variables": {"address": address, "limit": limit}},
                context=f"getgems.recentListings({address[:12]}…)",
            )
            payload = orjson.loads(raw)
            if isinstance(payload, dict) and payload.get("errors"):
                raise RuntimeError(f"GraphQL errors: {payload['errors'][:2]}")
            edges = (((payload.get("data") or {}).get("nftItems") or {}).get("edges")) or []
            return [e.get("node") or {} for e in edges]

        return await with_retry(_do, context=f"fetch_listings({address[:12]}…)")

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Основной цикл: обработка всех коллекций каждые settings.market_poll_interval."""
        interval = (
            self.settings.tonapi_poll_interval if self._tonapi_sync is not None
            else self.settings.market_poll_interval
        )
        source = "TonAPI" if self._tonapi_sync is not None else "GetGems"
        logger.info(f"[market_listener] старт, источник {source}, интервал {interval}s")

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

        logger.info(f"[market_listener] остановлен, статистика: {self.stats}")
