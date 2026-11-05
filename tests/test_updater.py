# /workspace/tests/test_updater.py — unit-тесты МОДУЛЯ 5 (ЭТАП 2)
"""
Что проверяем (без реальной сети):
  1. update_floors(): мок GraphQL-ответов -> запись Redis-ключей
     floor:{cid} и floor:{cid}:{model} + корректные TTL (30s) + БД collections/attributes.
  2. update_recent_sales(): мок nftSales -> recent_sales:{cid}:{model} (TTL 30s),
     INSERT в model_sales, dedupe повторных tx при втором прогоне.
  3. _to_decimal: защита от мусорных значений цены.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import orjson
import pytest
from sqlalchemy import func, select

from core.config import Settings
from core.redis_client import floor_key, recent_sales_key
from db.models import Attribute, Collection, ModelSale
from services.updater import FloorAndSalesUpdater

pytestmark = pytest.mark.asyncio


class FakeRedis:
    """Мини-заглушка redis.asyncio.Redis: set/get с учётом TTL."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.ttls: dict[str, int | None] = {}

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        self.store[key] = value
        self.ttls[key] = ex

    async def get(self, key: str) -> bytes | None:
        return self.store.get(key)


CONTRACT = "EQDTwwzDZK39EYHdBcnd_A6CnpQQGwRqLXjxcJkeN2vPXLkY"


async def seed_collection(session: Any) -> tuple[uuid.UUID, uuid.UUID]:
    """Тестовые данные: коллекция Teddy + модель Brown Bear."""
    coll_id = uuid.uuid4()
    attr_id = uuid.uuid4()
    session.add_all(
        [
            Collection(id=coll_id, name="Teddy", contract_address=CONTRACT, source="getgems"),
            Attribute(id=attr_id, collection_id=coll_id, type="model", value="Brown Bear"),
            # Фоновая «шумовая» запись без EQ-адреса — апдейтер обязан её пропускать
            Collection(id=uuid.uuid4(), name="RawGift", contract_address="tg_gift:RawGift", source="telegram_api"),
        ]
    )
    await session.commit()
    return coll_id, attr_id


def make_updater(session: Any, redis: FakeRedis, graphql_responses: dict[str, Any]) -> FloorAndSalesUpdater:
    """
    Апдейтер с подменённым ._graphql: ключом ответа служит подстрока context'а
    ('floor[', 'model_floor[', 'sales[').
    """
    st = Settings(api_max_concurrency=2, floor_cache_ttl=30, sales_cache_ttl=30)
    up = FloorAndSalesUpdater(session=session, redis=redis, settings=st)

    async def fake_graphql(query: str, variables: dict[str, Any], context: str = "") -> dict[str, Any]:
        for prefix, payload in graphql_responses.items():
            if context.startswith(prefix):
                if isinstance(payload, Exception):
                    raise payload
                return payload
        raise AssertionError(f"нет мока для context={context!r}")

    up._graphql = fake_graphql  # type: ignore[method-assign]
    return up


# ---------------------------------------------------------------------------
# 1. Floors: Redis-ключи + TTL + БД
# ---------------------------------------------------------------------------
async def test_update_floors_writes_redis_and_db(session: Any) -> None:
    coll_id, attr_id = await seed_collection(session)
    r = FakeRedis()
    responses = {
        # Ответ GetGems nftCollectionFloorPrice (см. докстринг services/updater.py)
        "floor[": {"nftCollectionFloorPrice": {"marketplace": {"name": "getgems"},
                                               "amount": {"value": "5.5"}}},
        # Ответ nftItems c сортировкой по FLOOR_PRICE: первый edge — минимальный листинг модели
        "model_floor[": {"nftItems": {"edges": [{"node": {"address": "EQitem1", "index": "8681",
                                                          "minPrice": {"amount": {"value": "12.4"}}}}]}},
    }
    up = make_updater(session, r, responses)

    await up.update_floors()

    # --- Redis: общий floor коллекции ---
    fk = floor_key(str(coll_id))
    assert fk in r.store, f"ключ {fk} не записан"
    assert orjson.loads(r.store[fk]) == 5.5
    assert r.ttls[fk] == 30, "TTL флора должен быть 30 сек"

    # --- Redis: floor модели ---
    mk = floor_key(str(coll_id), "Brown Bear")
    assert orjson.loads(r.store[mk]) == 12.4
    assert r.ttls[mk] == 30

    # --- БД: значения обновлены ---
    db_coll = (await session.execute(select(Collection).where(Collection.id == coll_id))).scalar_one()
    assert db_coll.floor_price == Decimal("5.5")
    db_attr = (await session.execute(select(Attribute).where(Attribute.id == attr_id))).scalar_one()
    assert db_attr.floor_price == Decimal("12.4")

    # tg_gift-коллекция не должна получить floor-ключ
    assert floor_key("tg-nothing") not in r.store


async def test_update_floors_survives_graphql_error(session: Any) -> None:
    """Сбой GraphQL по одной коллекции — не исключение из цикла, Redis не трогается."""
    coll_id, _ = await seed_collection(session)
    r = FakeRedis()
    up = make_updater(session, r, {"floor[": RuntimeError("getgems 503")})
    await up.update_floors()  # не должно бросить
    assert floor_key(str(coll_id)) not in r.store


# ---------------------------------------------------------------------------
# 2. Recent sales: Redis + model_sales + dedupe
# ---------------------------------------------------------------------------
async def test_update_recent_sales_redis_db_dedupe(session: Any) -> None:
    coll_id, _ = await seed_collection(session)
    r = FakeRedis()
    sales_payload = {
        # Ответ nftSales (пример структуры — см. докстринг модуля)
        "sales[": {
            "nftSales": {
                "edges": [
                    {"node": {
                        "nft": {"address": "EQitem8681", "index": "8681"},
                        "price": {"amount": {"value": "3503"}},
                        "transactionHash": "tx-aaa",
                        "createdAt": "2026-11-03T12:00:00Z",
                        "buyer": {"address": "EQbuyer"},
                        "seller": {"address": "EQseller"},
                    }},
                    {"node": {
                        "nft": {"address": "EQitem7", "index": "7"},
                        "price": {"amount": {"value": "210"}},
                        "transactionHash": "tx-bbb",
                        "createdAt": "2026-11-02T09:30:00+00:00",
                        "buyer": {"address": "EQbuyer2"},
                        "seller": {"address": "EQseller2"},
                    }},
                ]
            }
        }
    }
    up = make_updater(session, r, sales_payload)

    await up.update_recent_sales()

    # --- Redis: recent_sales:{cid}:{model}, TTL 30s, топ по убыванию времени ---
    key = recent_sales_key(str(coll_id), "Brown Bear")
    assert key in r.store
    assert r.ttls[key] == 30
    cached = orjson.loads(r.store[key])
    assert len(cached) == 2
    assert cached[0]["tx"] == "tx-aaa" and Decimal(cached[0]["price"]) == Decimal("3503")

    # --- БД: обе продажи записаны ---
    count = (await session.execute(select(func.count()).select_from(ModelSale))).scalar()
    assert count == 2
    row = (
        await session.execute(select(ModelSale).where(ModelSale.nft_index == 8681))
    ).scalar_one()
    assert row.price == Decimal("3503")
    assert row.marketplace == "getgems"
    assert row.seller_ton == "EQseller"
    assert row.sold_at.year == 2026  # ISO-дата распарсится

    # --- Повторный прогон: dedupe по transactionHash, дубликатов в БД нет ---
    await up.update_recent_sales()
    count2 = (await session.execute(select(func.count()).select_from(ModelSale))).scalar()
    assert count2 == 2, "повторная обработка тех же tx не должна плодить model_sales"
    # но кэш перезаписан (свежие данные)
    assert r.ttls[key] == 30


# ---------------------------------------------------------------------------
# 3. Утилиты
# ---------------------------------------------------------------------------
def test_to_decimal_guards() -> None:
    from services.collection_discovery import _to_decimal

    assert _to_decimal("5.5") == Decimal("5.5")
    assert _to_decimal(7) == Decimal("7")
    assert _to_decimal(None) == Decimal(0)
    assert _to_decimal("garbage") == Decimal(0)
    assert _to_decimal(float("nan")) == Decimal(0)


def test_to_int() -> None:
    from services.updater import _to_int

    assert _to_int("8681") == 8681
    assert _to_int(7) == 7
    assert _to_int(None) is None
    assert _to_int("abc") is None


# ---------------------------------------------------------------------------
# 4. Одна AsyncSession + параллельные джобы: замок исключает InvalidRequestError
# ---------------------------------------------------------------------------
async def test_parallel_jobs_share_session_without_errors(session: Any) -> None:
    import asyncio

    await seed_collection(session)
    r = FakeRedis()
    responses = {
        "floor[": {"nftCollectionFloorPrice": {"amount": {"value": "5.5"}}},
        "model_floor[": {"nftItems": {"edges": []}},
        "sales[": {"nftSales": {"edges": []}},
        "counts[": {"nftItems": {"edges": []}},
    }
    up = make_updater(session, r, responses)

    async def slow_graphql(query: str, variables: dict[str, Any], context: str = "") -> dict[str, Any]:
        await asyncio.sleep(0)  # отдаём управление, провоцируя гонку
        for prefix, payload in responses.items():
            if context.startswith(prefix):
                return payload
        raise AssertionError(context)

    up._graphql = slow_graphql  # type: ignore[method-assign]
    await asyncio.gather(
        up.update_floors(), up.update_recent_sales(), up.update_attribute_counts(),
        up.update_floors(), up.update_recent_sales(),
    )
