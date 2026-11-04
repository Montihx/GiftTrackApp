# /workspace/services/updater.py — МОДУЛЬ 5: Floor & Sales Updater (фоновые джобы)
"""
Updater — три asyncio-задачи (точка входа `python -m services.updater`):

1. update_floors()        каждые 10 сек:
   - GetGems GraphQL nftCollectionFloorPrice -> общий floor коллекции;
   - query nftItems с фильтром по attribute (trait) -> floor по каждой модели;
   - запись в Redis: floor:{collection_id} и floor:{collection_id}:{model} (TTL 30s);
   - обновление БД: collections.floor_price, attributes.floor_price.

2. update_recent_sales()  каждые 30 сек:
   - GetGems GraphQL nftSales (последние сделки по моделям);
   - запись в Redis: recent_sales:{collection_id}:{model} (TTL 30s);
   - in-flight dedupe + INSERT в БД model_sales.

3. update_collection_meta() раз в 3600 сек:
   - CollectionDiscovery.discover_all() (ЭТАП 2, модуль 1).

Rate limiting: единый asyncio.Semaphore(settings.api_max_concurrency) на все внешние вызовы.
Ретраи: core/retry.with_retry (exponential backoff). Shutdown: core/shutdown.ShutdownManager.

Примеры GraphQL-запросов (GetGems API https://api.getgems.io/graphql):

  query FloorPrice($address: Address!) {
    nftCollectionFloorPrice(address: $address) {
      marketplace { name }
      amount      { value }     # строка-дробь TON, напр. "5.5"
    }
  }
  Ответ: {"data": {"nftCollectionFloorPrice": {"marketplace": {"name": "getgems"},
                                                "amount": {"value": "5.5"}}}}

  query ModelFloor($address: Address!, $model: String!, $limit: Int!) {
    nftItems(input: { collectionAddress: $address,
                      attributes: [{ name: "Model", values: [$model] }],
                      sortedBy: FLOOR_PRICE, sortDirection: ASC, limit: $limit }) {
      edges { node { address index minPrice { amount { value } } } }
    }
  }
  Ответ: {"data": {"nftItems": {"edges": [{"node": {"address": "EQ...", "index": "8681",
                                                    "minPrice": {"amount": {"value": "12.4"}}}}]}}}
  floor модели = minPrice первого отсортированного по цене лота.

  query RecentSales($address: Address!, $model: String!, $limit: Int!) {
    nftSales(input: { collectionAddress: $address, attribute: { name: "Model", value: $model },
                      sortedBy: CREATED_AT, sortDirection: DESC, limit: $limit }) {
      edges { node { nft { address index } price { amount { value } }
                     transactionHash createdAt
                     buyer { address } seller { address } } }
    }
  }
"""
from __future__ import annotations

import asyncio
import datetime as dt
import uuid
from decimal import Decimal
from typing import Any

import orjson
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import update as pg_update
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import cache_set_json, floor_key, recent_sales_key
from core.retry import with_retry
from db.models import Attribute, Collection, ModelSale
from services.collection_discovery import _http_request, _to_decimal

# ---------------------------------------------------------------------------
# GraphQL-запросы (см. докстринг модуля с примерами ответов)
# ---------------------------------------------------------------------------
GQL_FLOOR_PRICE = """
query FloorPrice($address: Address!) {
  nftCollectionFloorPrice(address: $address) {
    marketplace { name }
    amount { value }
  }
}
"""

GQL_MODEL_FLOOR = """
query ModelFloor($address: Address!, $model: String!, $limit: Int!) {
  nftItems(input: {
    collectionAddress: $address,
    attributes: [{ name: "Model", values: [$model] }],
    sortedBy: FLOOR_PRICE,
    sortDirection: ASC,
    limit: $limit
  }) {
    edges { node { address index minPrice { amount { value } } } }
  }
}
"""

GQL_RECENT_SALES = """
query RecentSales($address: Address!, $model: String!, $limit: Int!) {
  nftSales(input: {
    collectionAddress: $address,
    attribute: { name: "Model", value: $model },
    sortedBy: CREATED_AT,
    sortDirection: DESC,
    limit: $limit
  }) {
    edges {
      node {
        nft { address index }
        price { amount { value } }
        transactionHash
        createdAt
        buyer { address }
        seller { address }
      }
    }
  }
}
"""

GQL_COLLECTION_ITEMS = """
query CollectionItems($address: Address!, $limit: Int!) {
  nftItems(input: { collectionAddress: $address, limit: $limit }) {
    edges { node { metadata { attributes { name value } } } }
  }
}
"""


SALES_CACHE_LIMIT = 10  # сколько последних продаж держим в Redis


class FloorAndSalesUpdater:
    """
    Фоновый апдейтер флоров/продаж. DI session+redis — для тестируемости
    (unit-тесты подменяют ._graphql / используют FakeRedis).
    """

    def __init__(
        self,
        session: AsyncSession,
        redis: Any,
        settings: Settings | None = None,
        discovery: Any | None = None,
    ) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings or get_settings()
        self._rate = asyncio.Semaphore(self.settings.api_max_concurrency)
        # Ленивый импорт по атрибуту => тесты могут присвоить mock до первого вызова
        self._discovery = discovery
        # Dedupe «в полёте»: не пишем одну и ту же продажу дважды из разных циклов
        self._seen_sale_hashes: set[str] = set()

    # ------------------------------------------------------------------
    # Джоба 1: floors (каждые N сек)
    # ------------------------------------------------------------------
    async def update_floors(self) -> None:
        """Общий floor коллекции + floor по каждой модели -> Redis (TTL 30s) + БД."""
        rows = (
            await self.session.execute(
                select(Collection.id, Collection.name, Collection.contract_address).where(
                    ~Collection.contract_address.startswith("tg_gift:")
                )
            )
        ).all()
        if not rows:
            logger.debug("update_floors: коллекций с EQ-адресом в БД нет — пропускаю цикл")
            return

        for coll_uuid, coll_name, contract in rows:
            cid = str(coll_uuid)
            # --- общий floor коллекции ---
            try:
                data = await self._graphql(GQL_FLOOR_PRICE, {"address": contract},
                                           context=f"floor[{coll_name}]")
                fp = ((data.get("nftCollectionFloorPrice") or {}).get("amount") or {}).get("value")
                floor = _to_decimal(fp)
            except Exception as exc:  # noqa: BLE001 — одна коллекция не роняет цикл
                logger.warning(f"update_floors: floor для '{coll_name}' не получен: {exc!r}")
                continue

            await cache_set_json(self.redis, floor_key(cid), float(floor), ttl=self.settings.floor_cache_ttl)
            await self._db_update_collection_floor(coll_uuid, floor)

            # --- floor по моделям ---
            models = (
                await self.session.execute(
                    select(Attribute.id, Attribute.value).where(
                        Attribute.collection_id == coll_uuid, Attribute.type == "model"
                    )
                )
            ).all()
            for attr_uuid, model_value in models:
                try:
                    mdata = await self._graphql(
                        GQL_MODEL_FLOOR,
                        {"address": contract, "model": model_value, "limit": 1},
                        context=f"model_floor[{coll_name}:{model_value}]",
                    )
                    edges = ((mdata.get("nftItems") or {}).get("edges")) or []
                    if not edges:
                        continue
                    raw = (((edges[0].get("node") or {}).get("minPrice") or {}).get("amount") or {}).get("value")
                    mfloor = _to_decimal(raw)
                except Exception as exc:  # noqa: BLE001
                    logger.debug(f"update_floors: модель '{model_value}' ({coll_name}): {exc!r}")
                    continue

                await cache_set_json(
                    self.redis, floor_key(cid, model_value), float(mfloor), ttl=self.settings.floor_cache_ttl
                )
                await self._db_update_attribute_floor(attr_uuid, mfloor)

        await self.session.commit()
        logger.info(f"update_floors: обновлено коллекций: {len(rows)}")

    async def _db_update_collection_floor(self, coll_id: uuid.UUID, floor: Decimal) -> None:
        """UPDATE по PK (не INSERT — запись гарантированно существует, мы её только что прочитали)."""
        await self.session.execute(
            pg_update(Collection.__table__)
            .where(Collection.__table__.c.id == coll_id)
            .values(floor_price=floor, last_updated=func.now())
        )

    async def _db_update_attribute_floor(self, attr_id: uuid.UUID, floor: Decimal) -> None:
        await self.session.execute(
            pg_update(Attribute.__table__)
            .where(Attribute.__table__.c.id == attr_id)
            .values(floor_price=floor)
        )

    # ------------------------------------------------------------------
    # Джоба 2: recent sales (каждые N сек)
    # ------------------------------------------------------------------
    async def update_recent_sales(self) -> None:
        """
        Последние продажи по моделям:
          Redis recent_sales:{cid}:{model} = JSON-список (TTL 30s)
          БД model_sales — INSERT c ON CONFLICT DO NOTHING (уникальность по tx-hash ниже).
        """
        rows = (
            await self.session.execute(
                select(Collection.id, Collection.name, Collection.contract_address)
                .join(Attribute, Attribute.collection_id == Collection.id)
                .where(Attribute.type == "model", ~Collection.contract_address.startswith("tg_gift:"))
                .distinct()
            )
        ).all()

        total_new = 0
        for coll_uuid, coll_name, contract in rows:
            cid = str(coll_uuid)
            models = (
                await self.session.execute(
                    select(Attribute.value).where(
                        Attribute.collection_id == coll_uuid, Attribute.type == "model"
                    )
                )
            ).scalars().all()

            for model_value in models:
                try:
                    data = await self._graphql(
                        GQL_RECENT_SALES,
                        {"address": contract, "model": model_value, "limit": SALES_CACHE_LIMIT},
                        context=f"sales[{coll_name}:{model_value}]",
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug(f"update_recent_sales: '{coll_name}:{model_value}': {exc!r}")
                    continue

                edges = ((data.get("nftSales") or {}).get("edges")) or []
                sales: list[dict[str, Any]] = []
                for e in edges:
                    node = e.get("node") or {}
                    tx = node.get("transactionHash")
                    if not tx:
                        continue
                    sale = {
                        "tx": tx,
                        "nft_address": (node.get("nft") or {}).get("address"),
                        "nft_index": (node.get("nft") or {}).get("index"),
                        "price": str((((node.get("price") or {}).get("amount") or {}).get("value")) or "0"),
                        "marketplace": "getgems",
                        "buyer_ton": (node.get("buyer") or {}).get("address"),
                        "seller_ton": (node.get("seller") or {}).get("address"),
                        "sold_at": node.get("createdAt"),
                    }
                    sales.append(sale)
                    # Новая продажа (не видели в этом процессе) -> в БД
                    if tx not in self._seen_sale_hashes:
                        self._seen_sale_hashes.add(tx)
                        await self._db_insert_sale(coll_uuid, model_value, sale)
                        total_new += 1

                # В кэш кладём топ-N даже если дубли — TTL короткий, данные свежие
                if sales:
                    await cache_set_json(
                        self.redis,
                        recent_sales_key(cid, model_value),
                        sales[:SALES_CACHE_LIMIT],
                        ttl=self.settings.sales_cache_ttl,
                    )

        await self.session.commit()
        # Защита от разрастания множества dedupe
        if len(self._seen_sale_hashes) > 50_000:
            self._seen_sale_hashes.clear()
        logger.info(f"update_recent_sales: коллекций: {len(rows)}, новых продаж в БД: {total_new}")

    async def _db_insert_sale(self, coll_id: uuid.UUID, model: str, sale: dict[str, Any]) -> None:
        sold_at_raw = sale.get("sold_at")
        sold_at: dt.datetime = dt.datetime.now(dt.timezone.utc)
        if isinstance(sold_at_raw, str):
            try:
                sold_at = dt.datetime.fromisoformat(sold_at_raw.replace("Z", "+00:00"))
            except ValueError:
                pass
        await self.session.execute(
            pg_insert(ModelSale.__table__)
            .values(
                collection_id=coll_id,
                model=model,
                nft_index=_to_int(sale.get("nft_index")),
                nft_address=sale.get("nft_address"),
                price=_to_decimal(sale.get("price")),
                marketplace=sale.get("marketplace") or "getgems",
                buyer_ton=sale.get("buyer_ton"),
                seller_ton=sale.get("seller_ton"),
                sold_at=sold_at,
            )
            .on_conflict_do_nothing()  # страховка; основной dedupe — по _seen_sale_hashes
        )

    # ------------------------------------------------------------------
    # Джоба 3: discovery (раз в час)
    # ------------------------------------------------------------------
    async def update_collection_meta(self) -> None:
        """Полный цикл обнаружения коллекций/атрибутов (services/collection_discovery)."""
        if self._discovery is None:
            from services.collection_discovery import CollectionDiscovery

            self._discovery = CollectionDiscovery(session=self.session, redis=self.redis,
                                                   settings=self.settings)
        stats = await self._discovery.discover_all()
        logger.info(f"update_collection_meta: {stats}")

    async def update_attribute_counts(self) -> None:
        """
        Пересчёт items_count атрибутов (для процентов редкости).

        Берём выборку rarity_sample_size NFT коллекции, считаем вхождения
        каждого трейта и пишем в attributes.items_count.
        Процент редкости = attr.items_count / collection.items_count * 100.
        """
        from collections import Counter

        from core.config import map_trait_type

        rows = (
            await self.session.execute(
                select(Collection.id, Collection.name, Collection.contract_address).where(
                    ~Collection.contract_address.startswith("tg_gift:")
                )
            )
        ).all()

        for coll_uuid, coll_name, contract in rows:
            try:
                data = await self._graphql(
                    GQL_COLLECTION_ITEMS,
                    {"address": contract, "limit": self.settings.rarity_sample_size},
                    context=f"counts[{coll_name}]",
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"update_attribute_counts: '{coll_name}': {exc!r}")
                continue

            edges = ((data.get("nftItems") or {}).get("edges")) or []
            counter: Counter[tuple[str, str]] = Counter()
            for e in edges:
                node = e.get("node") or {}
                for attr in ((node.get("metadata") or {}).get("attributes")) or []:
                    attr_type = map_trait_type(attr.get("name") or "")
                    if attr_type:
                        counter[(attr_type, attr.get("value") or "")] += 1

            if not counter:
                continue

            attrs = (
                await self.session.execute(
                    select(Attribute).where(Attribute.collection_id == coll_uuid)
                )
            ).scalars().all()
            for a in attrs:
                a.items_count = counter.get((a.type, a.value), 0)

        await self.session.commit()
        logger.info(f"update_attribute_counts: коллекций: {len(rows)}")


    # ------------------------------------------------------------------
    # GraphQL-хелпер (подменяется в тестах)
    # ------------------------------------------------------------------
    async def _graphql(self, query: str, variables: dict[str, Any], context: str = "") -> dict[str, Any]:
        """POST GraphQL через общий rate-limit + retry. Бросает RuntimeError при errors."""

        async def _post() -> dict[str, Any]:
            raw = await _http_request(
                self._rate, "POST", self.settings.getgems_api_url,
                json_body={"query": query, "variables": variables},
                context=context,
            )
            payload = orjson.loads(raw)
            if isinstance(payload, dict) and payload.get("errors"):
                raise RuntimeError(f"GraphQL errors [{context}]: {payload['errors'][:2]}")
            return payload.get("data", {}) if isinstance(payload, dict) else {}

        return await with_retry(_post, context=f"graphql:{context}")

    # ------------------------------------------------------------------
    # Планировщик: периодический запуск задач с graceful shutdown
    # ------------------------------------------------------------------
    async def run_periodic(self, stop_event: asyncio.Event) -> None:
        """
        Три параллельных цикла с разными периодами. Останавливается по stop_event
        (ShutdownManager). Исключение одной задачи НЕ убивает остальные — логируется.
        """

        async def _loop(name: str, coro_factory: Any, period: float) -> None:
            logger.info(f"[updater:{name}] старт, период {period}s")
            while not stop_event.is_set():
                try:
                    await coro_factory()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"[updater:{name}] итерация упала: {exc!r}", exc_info=True)
                # Ждём период ИЛИ сигнал остановки — что раньше
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=period)
                except asyncio.TimeoutError:
                    pass
            logger.info(f"[updater:{name}] остановлен")

        s = self.settings
        await asyncio.gather(
            _loop("floors", self.update_floors, s.floor_update_interval),
            _loop("sales", self.update_recent_sales, s.sales_update_interval),
            _loop("meta", self.update_collection_meta, s.discovery_interval),
            _loop("counts", self.update_attribute_counts, s.counts_update_interval),
        )


def _to_int(value: Any) -> int | None:
    """'8681'/8681/None -> int|None (GraphQL отдаёт index строкой)."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Точка входа сервиса: python -m services.updater
# ---------------------------------------------------------------------------
async def _amain() -> None:  # pragma: no cover - интеграционный путь
    from core.logging import setup_logging
    from core.redis_client import create_redis
    from core.shutdown import ShutdownManager
    from db.engine import dispose_engine, session_scope

    setup_logging()
    redis = create_redis()
    sm = ShutdownManager(timeout=15)

    async def _main() -> None:
        async with session_scope() as s:
            updater = FloorAndSalesUpdater(session=s, redis=redis)
            await updater.run_periodic(sm._stop_event)  # noqa: SLF001 - внутренний event

    try:
        await sm.run(_main)
    finally:
        await redis.aclose()
        await dispose_engine()
        logger.info("updater остановлен")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(_amain())
