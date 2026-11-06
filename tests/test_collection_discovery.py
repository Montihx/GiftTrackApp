# /workspace/tests/test_collection_discovery.py — unit-тесты МОДУЛЯ 1 (ЭТАП 2)
"""
Что проверяем (без реальной сети и блокчейна):
  1. fetch_telegram_gifts() — мок Bot API getAvailableGifts, маппинг Gift->Collection.
  2. Маппинг алиасов trait_type -> model/background/symbol/pattern (+ unmapped -> None).
  3. normalize_metadata_uri: https | ipfs:// | ipfs://ipfs/ через gateway.
  4. Парсинг metadata JSON из https- и ipfs-источников (мок aiohttp.ClientSession.get
     на уровне КЛАССА — перехватываются оба URL: оригинальный и ipfs-шлюз).
  5. discover_all(): upsert идемпотентен — повторный запуск НЕ создаёт дубликаты,
     items_count обновляется.
"""
from __future__ import annotations

import base64
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import aiohttp
import orjson
from sqlalchemy import func, select

from core.config import TRAIT_ALIASES, map_trait_type
from db.models import Attribute, Collection
from services.collection_discovery import (
    CollectionDiscovery,
    DiscoveredAttribute,
    DiscoveredCollection,
    decode_data_uri,
    normalize_metadata_uri,
)


# ---------------------------------------------------------------------------
# Вспомогательные моки
# ---------------------------------------------------------------------------
class FakeRedis:
    """Минимальная Redis-заглушка: хранит значения и TTL."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.ttls: dict[str, int] = {}

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex

    async def get(self, key: str) -> bytes | None:
        return self.store.get(key)


TG_GIFTS_RESPONSE = {
    "ok": True,
    "result": {
        "gifts": [
            {
                "sticker": {"emoji": "🧸", "set_name": "teddyv2_Telegrambots"},
                "total_count": 300000,
                "available_count": 57645,
                "upgrade_stars_cost": 35000,
                "stars_price": 5000,
                "base_name": "Teddy",
                "successor": {"name": "Teddy v2", "total_count": 5000},
            },
            {
                "sticker": {"emoji": "🎂"},
                "total_count": 100000,
                "stars_price": 750,
                "base_name": "Cake",
            },
        ]
    },
}

METADATA_HTTPS = {
    "name": "Teddy #1",
    "image": "https://cdn.example.com/1.png",
    "attributes": [
        {"trait_type": "Model", "value": "Brown Bear"},
        {"trait_type": "Bg", "value": "Red Scarf"},
        {"trait_type": "Icon", "value": "Fire"},
        {"trait_type": "Decoration", "value": "Pixel"},
        {"trait_type": "Rarity", "value": "Legendary"},  # unmapped — должен игнорироваться
    ],
}


def make_disc(session: Any, redis: Any | None = None) -> CollectionDiscovery:
    """Discovery с дефолтными настройками; сеть в тестах моками не трогаем напрямую."""
    from core.config import Settings

    st = Settings(
        bot_token="123456:TEST",
        telegram_api_base="http://localhost.test",
        attributes_sample_size=3,
        api_max_concurrency=2,
    )
    return CollectionDiscovery(session=session, redis=redis, settings=st)


# ---------------------------------------------------------------------------
# 1. Маппинг алиасов
# ---------------------------------------------------------------------------
def test_trait_alias_mapping() -> None:
    assert map_trait_type("Model") == "model"
    assert map_trait_type("character") == "model"
    assert map_trait_type(" Character ") == "model"
    assert map_trait_type("Background") == "background"
    assert map_trait_type("bg") == "background"
    assert map_trait_type("Symbol") == "symbol"
    assert map_trait_type("ICON") == "symbol"
    assert map_trait_type("Pattern") == "pattern"
    assert map_trait_type("decoration") == "pattern"
    assert map_trait_type("Rarity") is None
    assert map_trait_type("") is None
    # Словарь соответствует ТЗ
    assert TRAIT_ALIASES == {
        "model": ["model", "character"],
        "background": ["background", "bg"],
        "symbol": ["symbol", "icon"],
        "pattern": ["pattern", "decoration"],
    }


# ---------------------------------------------------------------------------
# 2. Нормализация metadata URI + data-URI
# ---------------------------------------------------------------------------
def test_normalize_metadata_uri() -> None:
    gw = "https://ipfs.io/ipfs/"
    assert normalize_metadata_uri("ipfs://QmAbc123", gw) == "https://ipfs.io/ipfs/QmAbc123"
    # двойной префикс ipfs://ipfs/... не должен давать /ipfs/ipfs/
    assert normalize_metadata_uri("ipfs://ipfs/QmAbc123", gw) == "https://ipfs.io/ipfs/QmAbc123"
    assert normalize_metadata_uri("https://meta.io/1.json", gw) == "https://meta.io/1.json"
    assert normalize_metadata_uri("  http://meta.io/1.json ", gw) == "http://meta.io/1.json"
    assert normalize_metadata_uri("ton://whatever", gw) is None
    # data-URI: скачиванию не подлежит, декодируется отдельно
    payload = base64.b64encode(orjson.dumps({"a": 1})).decode()
    assert decode_data_uri(f"data:application/json;base64,{payload}") == orjson.dumps({"a": 1})


# ---------------------------------------------------------------------------
# 3. fetch_telegram_gifts (мок Bot API)
# ---------------------------------------------------------------------------
async def test_fetch_telegram_gifts_mapping(session: Any) -> None:
    disc = make_disc(session)

    async def fake_get_json(self: CollectionDiscovery, url: str, context: str = "", **kw: Any) -> dict[str, Any]:
        assert "getAvailableGifts" in url
        assert url.startswith("http://localhost.test/bot123456:TEST/")
        return TG_GIFTS_RESPONSE["result"]

    disc._get_json = fake_get_json.__get__(disc)  # type: ignore[method-assign]

    gifts = await disc.fetch_telegram_gifts()
    assert len(gifts) == 2
    teddy = gifts[0]
    assert teddy.name == "Teddy v2"  # successor.name приоритетнее base_name
    assert teddy.contract_address == "tg_gift:Teddy"
    assert teddy.source == "telegram_api"
    assert teddy.floor_price == Decimal(5000)  # stars_price
    assert teddy.items_count == 300000  # total_count
    cake = gifts[1]
    assert cake.name == "Cake" and cake.source == "telegram_api"


# ---------------------------------------------------------------------------
# 4. Парсинг атрибутов NFT (мок blockchain + мок aiohttp.ClientSession на уровне КЛАССА)
# ---------------------------------------------------------------------------
class FakeCell:
    """Мини-заглушка pytoniq Cell/Slice для цепочки get_nft_data -> get_nft_content."""

    def __init__(self, uri: str) -> None:
        self._uri = uri

    def begin_parse(self) -> "FakeCell":
        return self

    def load_str(self) -> str:
        return self._uri


class FakeBlockchain:
    """
    Заглушка pytoniq LiteClient: run_method('addr', method_id, *args).
    Имитируем цепочку get_nft_address_by_index -> get_nft_data -> get_nft_content.
    """

    def __init__(self, content_uri: str) -> None:
        self.content_uri = content_uri
        self.calls: list[tuple[str, int]] = []

    async def run_method(self, addr: str, method_id: int, *args: Any) -> Any:
        self.calls.append((addr, method_id))
        if method_id == 0x4B3F5F89:  # get_nft_address_by_index -> адрес NFT
            return f"EQnft{args[0]}"
        if method_id == 0x57CF1C6E:  # get_nft_data -> (queryId, owner, content_cell)
            return [0, "EQowner", FakeCell("content-cell-placeholder")]
        if method_id == 0x73C607DF:  # get_nft_content -> Slice со строкой metadata URI
            return FakeCell(self.content_uri)
        raise AssertionError(f"неизвестный method_id {method_id:#x}")


class FakeResponse:
    """Заглушка ответа aiohttp (async context manager) с телом metadata JSON."""

    def __init__(self, body: bytes, status: int = 200) -> None:
        self._body = body
        self.status = status

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    async def read(self) -> bytes:
        return self._body

    async def text(self) -> str:
        return self._body.decode()

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(None, (), status=self.status)  # type: ignore[arg-type]


def patch_aiohttp_session_get(body: bytes, fetched: list[str]):
    """
    Мок aiohttp.ClientSession.get/request НА УРОВНЕ КЛАССА — перехватываются ВСЕ
    запросы модуля (и оригинальный https-URL, и ipfs:// через шлюз ipfs.io),
    независимо от того, какой экземпляр сессии создан внутри fetch-обёртки.
    Возвращает context manager с двумя патчами (get и request).
    """

    class _Ctx(FakeResponse):
        pass

    @asynccontextmanager
    async def fake_request(self: Any, method: str, url: str, **kw: Any):
        # фиксируем только GET-выгрузки metadata
        if str(method).upper() == "GET":
            fetched.append(url)
        yield _Ctx(body)

    async def fake_get(self: Any, url: str, **kw: Any):
        fetched.append(url)
        return _Ctx(body)

    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(patch.object(aiohttp.ClientSession, "get", fake_get))
    stack.enter_context(patch.object(aiohttp.ClientSession, "request", fake_request))
    return stack


async def test_parse_attributes_https_and_ipfs(session: Any) -> None:
    for uri, expected_url in (
        ("https://meta.example.com/teddy/0.json", "https://meta.example.com/teddy/0.json"),
        ("ipfs://QmMetaHash000", "https://ipfs.io/ipfs/QmMetaHash000"),
    ):
        disc = make_disc(session)
        disc.blockchain = FakeBlockchain(uri)

        fetched: list[str] = []
        with patch_aiohttp_session_get(orjson.dumps(METADATA_HTTPS), fetched):
            coll = DiscoveredCollection(name="Teddy", contract_address="EQCollTest", source="getgems")
            attrs = await disc.parse_collection_attributes(coll)

        # 3 NFT выборки -> ровно 3 скачивания по НОРМАЛИЗОВАННОМУ URL
        assert len(fetched) == 3 and set(fetched) == {expected_url}, \
            f"metadata должен качаться по {expected_url}, реально: {fetched}"
        # уникальные трейты: 4 (Rarity отброшен как unmapped)
        got = {(a.type, a.value) for a in attrs}
        assert got == {
            ("model", "Brown Bear"),
            ("background", "Red Scarf"),
            ("symbol", "Fire"),
            ("pattern", "Pixel"),
        }


async def test_parse_attributes_data_uri_no_network(session: Any) -> None:
    """data:application/json;base64 распаковывается ончейн-значением без HTTP."""
    disc = make_disc(session)
    disc.blockchain = FakeBlockchain("unused")
    payload = base64.b64encode(orjson.dumps(METADATA_HTTPS)).decode()
    meta = await disc._fetch_metadata_json(f"data:application/json;base64,{payload}")
    assert meta["name"] == "Teddy #1"


# ---------------------------------------------------------------------------
# 5. discover_all: upsert + идемпотентность
# ---------------------------------------------------------------------------
async def test_discover_all_upsert_idempotent(session: Any) -> None:
    disc = make_disc(session, redis=FakeRedis())

    # Сеть полностью отсекаем подменой источников
    async def fake_gifts(self: CollectionDiscovery) -> list[DiscoveredCollection]:
        return [
            DiscoveredCollection(name="Teddy", contract_address="tg_gift:Teddy",
                                 source="telegram_api", floor_price=Decimal(5000), items_count=300000),
            DiscoveredCollection(name="UniqueGiftXYZ", contract_address="tg_gift:UniqueGiftXYZ",
                                 source="telegram_api", floor_price=Decimal(100), items_count=10),
        ]

    async def fake_getgems(self: CollectionDiscovery) -> list[DiscoveredCollection]:
        return [
            DiscoveredCollection(name="Teddy", contract_address="EQDTwwzDZK39EYHdBcnd",
                                 source="getgems", icon_url="https://cdn/icon.png",
                                 floor_price=Decimal("5.5"), items_count=42,
                                 attributes=[DiscoveredAttribute(type="model", value="Brown Bear")]),
        ]

    disc.fetch_telegram_gifts = fake_gifts.__get__(disc)  # type: ignore[method-assign]
    disc.fetch_getgems_collections = fake_getgems.__get__(disc)  # type: ignore[method-assign]

    stats1 = await disc.discover_all()
    # Merge по имени: tg_gift:Teddy сопоставлен с EQ-коллекцией GetGems -> 2 коллекции всего
    assert stats1 == {"collections": 2, "attributes": 1}

    rows = (await session.execute(select(Collection))).scalars().all()
    by_addr = {r.contract_address: r for r in rows}
    assert set(by_addr) == {"EQDTwwzDZK39EYHdBcnd", "tg_gift:UniqueGiftXYZ"}
    teddy = by_addr["EQDTwwzDZK39EYHdBcnd"]
    assert teddy.source == "getgems" and teddy.items_count == 42
    assert teddy.floor_price == Decimal("5.5")

    attrs = (await session.execute(select(Attribute))).scalars().all()
    assert len(attrs) == 1 and attrs[0].type == "model" and attrs[0].value == "Brown Bear"

    # --- повторный запуск: тот же объём данных, items_count обновился, дубликатов нет ---
    async def fake_getgems_updated(self: CollectionDiscovery) -> list[DiscoveredCollection]:
        c = await fake_getgems(self)
        c[0].items_count = 999
        return c

    disc.fetch_getgems_collections = fake_getgems_updated.__get__(disc)  # type: ignore[method-assign]
    stats2 = await disc.discover_all()
    assert stats2["collections"] == 2

    count = (await session.execute(select(func.count()).select_from(Collection))).scalar()
    assert count == 2, "повторный discovery не должен плодить коллекции"
    attr_count = (await session.execute(select(func.count()).select_from(Attribute))).scalar()
    assert attr_count == 1, "повторный discovery не должен плодить атрибуты"
    teddy2 = (
        await session.execute(select(Collection).where(Collection.contract_address == "EQDTwwzDZK39EYHdBcnd"))
    ).scalar_one()
    assert teddy2.items_count == 999, "items_count должен обновиться"


async def test_collection_attrs_cached_in_redis(session: Any) -> None:
    r = FakeRedis()
    disc = make_disc(session, redis=r)

    async def no_gifts(self: CollectionDiscovery) -> list[DiscoveredCollection]:
        return []

    async def one_coll(self: CollectionDiscovery) -> list[DiscoveredCollection]:
        return [DiscoveredCollection(name="Rose", contract_address="EQRose",
                                     source="getgems", floor_price=Decimal("2.4"), items_count=7)]

    disc.fetch_telegram_gifts = no_gifts.__get__(disc)  # type: ignore[method-assign]
    disc.fetch_getgems_collections = one_coll.__get__(disc)  # type: ignore[method-assign]

    await disc.discover_all()

    # Ключ collection_attrs:{uuid} записан с TTL 1h и содержит данные коллекции
    keys = [k for k in r.store if k.startswith("collection_attrs:")]
    assert len(keys) == 1
    cid = keys[0].split(":", 1)[1]
    uuid.UUID(cid)  # валидный UUID
    assert r.ttls[keys[0]] == 3600
    cached = orjson.loads(r.store[keys[0]])
    assert cached["name"] == "Rose" and cached["contract_address"] == "EQRose"
