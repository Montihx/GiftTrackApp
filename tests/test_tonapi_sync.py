# tests/test_tonapi_sync.py — TonAPI-синхронизация: парсинг, diff снимков, запись в БД, listener-market
from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import orjson
from sqlalchemy import func, select

from core.config import Settings
from core.redis_client import STREAM_NFT_LISTINGS
from db.models import Attribute, Collection, FloorSnapshot, NftOwnershipHistory, ProcessedNft, Sale
from services.market.listener import MarketListener
from services.tonapi_sync import ItemState, TonApiSync, diff_states, parse_item, to_state

NOW = dt.datetime(2026, 11, 5, 12, 0, tzinfo=dt.timezone.utc)
ADDR = "EQ" + "A" * 46
NANO = 10**9


def raw_item(addr: str, index: int, owner: str, model: str = "Obelisk", price: float | None = None,
             seller: str | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "address": addr, "index": index, "owner": {"address": owner},
        "metadata": {"attributes": [{"trait_type": "Model", "value": model},
                                    {"trait_type": "Backdrop", "value": "Silver Blue"},
                                    {"trait_type": "Symbol", "value": "Moon"}]},
    }
    if price is not None:
        item["owner"] = {"address": "0:sale"}
        item["sale"] = {"address": "0:sale", "owner": {"address": seller or owner},
                        "market": {"name": "GetGems"},
                        "price": {"value": str(int(price * NANO)), "token_name": "TON"}}
    return item


def state(addr: str, owner: str, price: float | None = None) -> ItemState:
    return ItemState(addr, 1, owner, Decimal(str(price)) if price is not None else None, {})


def test_parse_item_traits_and_effective_owner() -> None:
    it = parse_item(raw_item("0:a", 5, "0:seller", price=5, seller="0:seller"))
    assert it is not None
    assert it.owner == "0:seller" and it.price == Decimal("5") and it.market == "getgems"
    assert it.attrs == {"model": "Obelisk", "background": "Silver Blue", "symbol": "Moon"}
    unlisted = parse_item(raw_item("0:b", 6, "0:own"))
    assert unlisted is not None and unlisted.owner == "0:own" and unlisted.price is None
    assert parse_item({"index": 1}) is None


def test_bootstrap_has_no_events() -> None:
    d = diff_states(None, [state("a", "o1", 5)])
    assert len(d.new_items) == 1 and not d.listings and not d.sales and not d.transfers


def test_new_listing_detected() -> None:
    d = diff_states(to_state([state("a", "o1")]), [state("a", "o1", 5)])
    assert [i.address for i in d.listings] == ["a"] and not d.sales and not d.transfers


def test_sale_detected_when_listed_item_changes_owner() -> None:
    d = diff_states(to_state([state("a", "o1", 5)]), [state("a", "o2")])
    assert [(i.address, p) for i, p in d.sales] == [("a", Decimal("5"))]
    assert len(d.transfers) == 1 and not d.listings


def test_buy_and_relist_gives_sale_and_listing() -> None:
    d = diff_states(to_state([state("a", "o1", 5)]), [state("a", "o2", 9)])
    assert len(d.sales) == 1 and [i.price for i in d.listings] == [Decimal("9")]


def test_delist_and_plain_transfer() -> None:
    d = diff_states(to_state([state("a", "o1", 5), state("b", "o1")]), [state("a", "o1"), state("b", "o2")])
    assert not d.sales and not d.listings
    assert [i.address for i, _ in d.transfers] == ["b"]


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}
        self.stream: list[tuple[str, dict[str, str]]] = []

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        self.kv[key] = value

    async def get(self, key: str) -> bytes | None:
        return self.kv.get(key)

    async def xadd(self, stream: str, entry: dict[str, str], maxlen: int | None = None,
                   approximate: bool = True) -> bytes:
        self.stream.append((stream, entry))
        return b"1-0"


class FakeClient:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] | None = []

    async def fetch_collection_items(self, address: str) -> list[dict[str, Any]] | None:
        return self.items


async def _setup(session: Any) -> tuple[Collection, FakeRedis, FakeClient, TonApiSync]:
    coll = Collection(name="Teddy", contract_address=ADDR, source="seed")
    session.add(coll)
    await session.commit()
    redis, client = FakeRedis(), FakeClient()
    return coll, redis, client, TonApiSync(session, redis, Settings(tonapi_key="k"), client=client)  # type: ignore[arg-type]


async def test_bootstrap_then_sale_and_listing(session: Any) -> None:
    coll, redis, client, sync = await _setup(session)
    cid = coll.id
    client.items = [raw_item("0:a", 1, "0:o1", price=5, seller="0:o1"), raw_item("0:b", 2, "0:o2"),
                    raw_item("0:c", 3, "0:o3", model="Pepe")]
    await sync.sync_collection(cid, "Teddy", ADDR, now=NOW)

    nfts = {n.nft_address: n for n in (await session.execute(select(ProcessedNft))).scalars().all()}
    assert set(nfts) == {"0:a", "0:b", "0:c"} and nfts["0:a"].collection_id == cid
    assert nfts["0:a"].model == "Obelisk" and nfts["0:a"].current_owner_ton == "0:o1"
    attr = (await session.execute(select(Attribute).where(Attribute.type == "model", Attribute.value == "Obelisk"))).scalar_one()
    assert attr.items_count == 2 and Decimal(str(attr.floor_price)) == Decimal("5")
    await session.refresh(coll)
    assert coll.items_count == 3 and Decimal(str(coll.floor_price)) == Decimal("5")
    assert (await session.execute(select(func.count()).select_from(FloorSnapshot))).scalar_one() == 1
    assert (await session.execute(select(func.count()).select_from(Sale))).scalar_one() == 0
    assert redis.stream == []

    client.items = [raw_item("0:a", 1, "0:buyer"), raw_item("0:b", 2, "0:o2", price=7),
                    raw_item("0:c", 3, "0:o3", model="Pepe")]
    await sync.sync_collection(cid, "Teddy", ADDR, now=NOW + dt.timedelta(seconds=15))

    sale = (await session.execute(select(Sale))).scalar_one()
    assert sale.item_address == "0:a" and Decimal(str(sale.price)) == Decimal("5") and sale.collection_id == cid
    assert len(redis.stream) == 1 and redis.stream[0][0] == STREAM_NFT_LISTINGS
    payload = orjson.loads(redis.stream[0][1]["data"])
    assert payload["nft_address"] == "0:b" and payload["price_ton"] == 7.0
    assert payload["collection_id"] == str(cid) and payload["attributes"]["model"] == "Obelisk"
    await session.refresh(coll)
    assert Decimal(str(coll.floor_price)) == Decimal("7")
    assert (await session.execute(select(func.count()).select_from(FloorSnapshot))).scalar_one() == 2
    session.expire_all()
    a = await session.get(ProcessedNft, "0:a")
    assert a is not None and a.current_owner_ton == "0:buyer"
    hist = (await session.execute(select(NftOwnershipHistory).where(NftOwnershipHistory.nft_address == "0:a")
                                  .order_by(NftOwnershipHistory.id))).scalars().all()
    assert [h.owner_ton for h in hist] == ["0:o1", "0:buyer"]

    await sync.sync_collection(cid, "Teddy", ADDR, now=NOW + dt.timedelta(seconds=30))
    assert (await session.execute(select(func.count()).select_from(Sale))).scalar_one() == 1
    assert len(redis.stream) == 1


async def test_oversized_collection_is_skipped(session: Any) -> None:
    coll, redis, client, sync = await _setup(session)
    client.items = None
    assert await sync.sync_collection(coll.id, "Teddy", ADDR, now=NOW) is None
    assert sync.stats["skipped"] == 1 and not redis.kv


async def test_market_listener_uses_tonapi_when_key_set(session: Any) -> None:
    session.add_all([Collection(name="Teddy", contract_address=ADDR, source="seed"),
                     Collection(name="Placeholder", contract_address="tg_gift:x", source="seed")])
    await session.commit()
    listener = MarketListener(session=session, redis=FakeRedis(), settings=Settings(tonapi_key="k"))
    assert listener._tonapi_sync is not None
    fake = FakeClient()
    fake.items = [raw_item("0:a", 1, "0:o1")]
    listener._tonapi_sync.client = fake  # type: ignore[assignment]
    await listener.process_collections()
    assert (await session.execute(select(ProcessedNft))).scalar_one().nft_address == "0:a"


def test_market_listener_without_key_keeps_getgems() -> None:
    listener = MarketListener(session=None, redis=None, settings=Settings(tonapi_key=""))  # type: ignore[arg-type]
    assert listener._tonapi_sync is None
