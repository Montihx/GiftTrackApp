# tests/test_tonapi_sync.py — TonAPI-синхронизация: парсинг, diff снимков, запись в БД, listener-market
from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import orjson
from sqlalchemy import func, select

from core.config import Settings
from core.redis_client import STREAM_NFT_LISTINGS, STREAM_NFT_SALES
from db.models import Attribute, Collection, FloorSnapshot, NftOwnershipHistory, ProcessedNft, Sale
from services.market.listener import MarketListener
from services.tonapi_sync import (
    ItemState,
    TonApiSync,
    diff_states,
    normalize_market,
    parse_item,
    to_state,
)

NOW = dt.datetime(2026, 11, 5, 12, 0, tzinfo=dt.timezone.utc)
ADDR = "EQ" + "A" * 46
NANO = 10**9


def raw_item(addr: str, index: int, owner: str, model: str = "Obelisk", price: float | None = None,
             seller: str | None = None, owner_name: str | None = None) -> dict[str, Any]:
    """Форма как у живого TonAPI (2026-11): index — хэш, номер в metadata.name, цена в "Gram"."""
    item: dict[str, Any] = {
        "address": addr, "index": -9057005072502437270 + index,
        "owner": {"address": owner, **({"name": owner_name} if owner_name else {})},
        "metadata": {"name": f"Teddy #{index}",
                     "attributes": [{"trait_type": "Model", "value": model},
                                    {"trait_type": "Backdrop", "value": "Silver Blue"},
                                    {"trait_type": "Symbol", "value": "Moon"}]},
    }
    if price is not None:
        item["owner"] = {"address": "0:sale"}
        item["sale"] = {"address": "0:sale", "owner": {"address": seller or owner},
                        "market": {"name": "Getgems Sales"},
                        "price": {"currency_type": "native", "value": str(int(price * NANO)),
                                  "decimals": 9, "token_name": "Gram"}}
    return item


def state(addr: str, owner: str, price: float | None = None) -> ItemState:
    return ItemState(addr, 1, owner, Decimal(str(price)) if price is not None else None, {})


def test_parse_item_traits_and_effective_owner() -> None:
    it = parse_item(raw_item("0:a", 5, "0:seller", price=5, seller="0:seller"))
    assert it is not None
    assert it.owner == "0:seller" and it.price == Decimal("5") and it.market == "getgems"
    assert it.index == 5 and it.name == "Teddy #5"
    assert it.attrs == {"model": "Obelisk", "background": "Silver Blue", "symbol": "Moon"}
    unlisted = parse_item(raw_item("0:b", 6, "0:own"))
    assert unlisted is not None and unlisted.owner == "0:own" and unlisted.price is None
    assert parse_item({"index": 1}) is None


def test_parse_item_minter_owner_is_unknown_and_index_fallback() -> None:
    it = parse_item(raw_item("0:m", 7, "0:minter", owner_name="Fragment Gift Minter"))
    assert it is not None and it.owner is None
    no_name = parse_item({"address": "0:x", "index": 42})
    assert no_name is not None and no_name.index == 42
    huge = parse_item({"address": "0:y", "index": 2**70})
    assert huge is not None and huge.index == 0


def test_normalize_market() -> None:
    assert normalize_market("Getgems Sales") == "getgems"
    assert normalize_market("Marketapp Marketplace") == "marketapp"
    assert normalize_market("Some Market") == "some"
    assert normalize_market(None) == "tonapi"


def test_bootstrap_has_no_events() -> None:
    d = diff_states(None, [state("a", "o1", 5)])
    assert len(d.new_items) == 1 and not d.listings and not d.sales and not d.transfers


def test_new_listing_detected() -> None:
    d = diff_states(to_state([state("a", "o1")]), [state("a", "o1", 5)])
    assert [i.address for i in d.listings] == ["a"] and not d.sales and not d.transfers


def test_sale_detected_when_listed_item_changes_owner() -> None:
    d = diff_states(to_state([state("a", "o1", 5)]), [state("a", "o2")])
    assert [(i.address, p, prev.owner) for i, p, prev in d.sales] == [("a", Decimal("5"), "o1")]
    assert len(d.transfers) == 1 and not d.listings


def test_buy_and_relist_gives_sale_and_listing() -> None:
    d = diff_states(to_state([state("a", "o1", 5)]), [state("a", "o2", 9)])
    assert len(d.sales) == 1 and [i.price for i in d.listings] == [Decimal("9")]


def test_price_change_is_new_listing_same_price_is_not() -> None:
    prev = to_state([state("a", "o1", 5), state("b", "o1", 5)])
    d = diff_states(prev, [state("a", "o1", 4), state("b", "o1", 5)])
    assert [i.address for i in d.listings] == ["a"] and not d.sales


def test_delist_and_plain_transfer() -> None:
    d = diff_states(to_state([state("a", "o1", 5), state("b", "o1")]), [state("a", "o1"), state("b", "o2")])
    assert not d.sales and not d.listings
    assert [i.address for i, _ in d.transfers] == ["b"]


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}
        self.h: dict[str, dict[str, bytes]] = {}
        self.sets: dict[str, set[str]] = {}
        self.stream: list[tuple[str, dict[str, str]]] = []

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        self.kv[key] = value

    async def get(self, key: str) -> bytes | None:
        return self.kv.get(key)

    async def hset(self, key: str, mapping: dict[str, bytes]) -> None:
        self.h.setdefault(key, {}).update(mapping)

    async def hmget(self, key: str, fields: list[str]) -> list[bytes | None]:
        return [self.h.get(key, {}).get(f) for f in fields]

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        return {k.encode(): v for k, v in self.h.get(key, {}).items()}

    async def hdel(self, key: str, *fields: str) -> None:
        for f in fields:
            self.h.get(key, {}).pop(f, None)

    async def expire(self, key: str, ttl: int) -> None:
        pass

    async def sismember(self, key: str, member: str) -> bool:
        return member in self.sets.get(key, set())

    async def sadd(self, key: str, member: str) -> None:
        self.sets.setdefault(key, set()).add(member)

    async def xadd(self, stream: str, entry: dict[str, str], maxlen: int | None = None,
                   approximate: bool = True) -> bytes:
        self.stream.append((stream, entry))
        return b"1-0"

    def events(self, stream: str) -> list[dict[str, Any]]:
        return [orjson.loads(e["data"]) for s, e in self.stream if s == stream]


class FakeClient:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []
        self.calls: list[int] = []

    async def fetch_ton_usd(self) -> float | None:
        return 2.0

    async def fetch_account_nfts(self, account: str, offset: int, limit: int = 1000) -> list[dict[str, Any]]:
        return []

    async def fetch_items_page(self, address: str, offset: int, limit: int = 1000) -> list[dict[str, Any]]:
        self.calls.append(offset)
        return self.items[offset:offset + limit]


async def _setup(session: Any) -> tuple[Collection, FakeRedis, FakeClient, TonApiSync]:
    coll = Collection(name="Teddy", contract_address=ADDR, source="seed")
    session.add(coll)
    await session.commit()
    redis, client = FakeRedis(), FakeClient()
    return coll, redis, client, TonApiSync(session, redis, Settings(tonapi_key="k"), client=client)  # type: ignore[arg-type]


async def _setup_paged(session: Any) -> tuple[Collection, FakeRedis, FakeClient, TonApiSync]:
    """page_limit=2, 1 страница за цикл: коллекция из 5 items проходится за 3 цикла."""
    coll, redis, client, _ = await _setup(session)
    sync = TonApiSync(session, redis, Settings(tonapi_key="k", tonapi_pages_per_cycle=1),  # type: ignore[arg-type]
                      client=client, page_limit=2)  # type: ignore[arg-type]
    return coll, redis, client, sync


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
    listings = redis.events(STREAM_NFT_LISTINGS)
    assert len(listings) == 1
    payload = listings[0]
    assert payload["nft_address"] == "0:b" and payload["price_ton"] == 7.0
    assert payload["collection_id"] == str(cid) and payload["attributes"]["model"] == "Obelisk"
    assert payload["nft_index"] == 2 and payload["marketplace"] == "getgems"
    sold = redis.events(STREAM_NFT_SALES)
    assert len(sold) == 1 and sold[0]["nft_address"] == "0:a" and sold[0]["price_ton"] == 5.0
    assert sold[0]["seller_ton"] == "0:o1" and sold[0]["buyer_ton"] == "0:buyer"
    assert sold[0]["marketplace"] == "getgems" and sold[0]["nft_name"] == "Teddy #1"
    assert sold[0]["attributes"]["model"] == "Obelisk" and sold[0]["collection_name"] == "Teddy"
    assert sold[0]["price_usd"] == 10.0
    recent = orjson.loads(redis.kv[f"recent_sales:{cid}:Obelisk"])
    assert recent[0]["nft_index"] == 1 and recent[0]["price"] == 5.0 and recent[0]["marketplace"] == "getgems"
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
    assert len(redis.stream) == 2


async def test_large_collection_scanned_across_cycles(session: Any) -> None:
    coll, redis, client, sync = await _setup_paged(session)
    cid = coll.id
    client.items = [raw_item(f"0:{k}", k, f"0:o{k}", price=10 + k) for k in range(5)]
    for step in range(3):  # первый проход: 3 цикла по 2 items, floor только в конце
        await sync.sync_collection(cid, "Teddy", ADDR, now=NOW + dt.timedelta(seconds=15 * step))
        await session.refresh(coll)
        assert (coll.floor_price is None) == (step < 2)
    assert client.calls == [0, 2, 4]
    assert Decimal(str(coll.floor_price)) == Decimal("10") and coll.items_count == 5
    assert redis.stream == []  # bootstrap без событий

    # второй проход: #0 продан (floor растёт), #3 перевыставлен дешевле
    client.items[0] = raw_item("0:0", 0, "0:buyer")
    client.items[3] = raw_item("0:3", 3, "0:o3", price=1)
    for step in range(3):
        await sync.sync_collection(cid, "Teddy", ADDR, now=NOW + dt.timedelta(minutes=1, seconds=15 * step))
    await session.refresh(coll)
    assert Decimal(str(coll.floor_price)) == Decimal("1")
    assert [e["nft_address"] for e in redis.events(STREAM_NFT_SALES)] == ["0:0"]
    assert [e["price_ton"] for e in redis.events(STREAM_NFT_LISTINGS)] == [1.0]
    assert (await session.execute(select(func.count()).select_from(Sale))).scalar_one() == 1


async def test_burned_items_dropped_after_pass(session: Any) -> None:
    coll, redis, client, sync = await _setup(session)
    client.items = [raw_item("0:a", 1, "0:o1", price=2), raw_item("0:b", 2, "0:o2", price=8)]
    await sync.sync_collection(coll.id, "Teddy", ADDR, now=NOW)
    client.items = [raw_item("0:b", 2, "0:o2", price=8)]  # 0:a сожжён — его цена не должна держать floor
    await sync.sync_collection(coll.id, "Teddy", ADDR, now=NOW + dt.timedelta(seconds=15))
    await session.refresh(coll)
    assert Decimal(str(coll.floor_price)) == Decimal("8") and coll.items_count == 1
    assert set(redis.h[f"tonapi_sync:{coll.id}:items"]) == {"0:b"}


async def test_market_listener_uses_tonapi_when_key_set(session: Any) -> None:
    session.add_all([Collection(name="Teddy", contract_address=ADDR, source="seed"),
                     Collection(name="Placeholder", contract_address="tg_gift:x", source="seed")])
    await session.commit()
    listener = MarketListener(session=session, redis=FakeRedis(), settings=Settings(tonapi_key="k"))
    assert listener._tonapi_sync is not None
    fake = FakeClient()
    fake.items = [raw_item("0:a", 1, "0:o1")]  # плейсхолдер tg_gift:x пропускается
    listener._tonapi_sync.client = fake  # type: ignore[assignment]
    await listener.process_collections()
    assert (await session.execute(select(ProcessedNft))).scalar_one().nft_address == "0:a"


def test_market_listener_without_key_keeps_getgems() -> None:
    listener = MarketListener(session=None, redis=None, settings=Settings(tonapi_key=""))  # type: ignore[arg-type]
    assert listener._tonapi_sync is None


# --- ложные продажи, компактное состояние, поток реального времени, приоритет --------------------

def test_market_contract_owner_without_sale_is_uncertain_not_a_sale() -> None:
    """Регрессия (живые данные, Berry Box): листинг Marketapp, в снимке нет sale и владелец —
    контракт «Marketapp Deployer». Это не продажа и не новый листинг."""
    listed = parse_item(raw_item("0:a", 1, "0:seller", price=10.5, seller="0:seller"))
    flap = raw_item("0:a", 1, "0:deployer")
    flap["owner"]["is_wallet"] = False
    cur = parse_item(flap)
    assert listed is not None and cur is not None and cur.uncertain
    from services.tonapi_sync import state_to_store

    prev = to_state([listed])
    d = diff_states(prev, [cur])
    assert not d.sales and not d.listings and not d.transfers
    assert state_to_store([cur], prev) == [listed]  # прошлое состояние сохраняется
    back = parse_item(raw_item("0:a", 1, "0:seller", price=10.5, seller="0:seller"))
    assert back is not None and not diff_states(prev, [back]).listings  # «возврат» — не новый листинг


def test_compact_state_roundtrip_and_old_format() -> None:
    from services.tonapi_sync import decode_state, encode_state

    it = ItemState("0:a", 7, "0:o", Decimal("5.5"), {"model": "M", "symbol": "S"}, "getgems", "X #7")
    back, pass_no = decode_state("0:a", encode_state(it, 3))  # type: ignore[misc]
    assert pass_no == 3 and back.attrs == {"model": "M", "symbol": "S"} and back.price == Decimal("5.5")
    assert back.name is None and back.index == 7
    old = orjson.dumps(["0:o", "1", 2, {"model": "M"}, "getgems", "X #2", 1])
    o, p = decode_state("0:b", old)  # type: ignore[misc]
    assert o.attrs == {"model": "M"} and p == 1


async def _ready_collection(session: Any) -> tuple[Collection, FakeRedis, FakeClient, TonApiSync]:
    coll, redis, client, sync = await _setup(session)
    client.items = [raw_item("0:a", 1, "0:o1", price=50, seller="0:o1"), raw_item("0:b", 2, "0:o2", price=80)]
    await sync.sync_collection(coll.id, "Teddy", ADDR, now=NOW)  # первый проход -> ready
    return coll, redis, client, sync


def _api_nft(item: dict[str, Any]) -> dict[str, Any]:
    return {**item, "collection": {"address": ADDR, "name": "Teddys"}}


async def test_stream_apply_item_sale_and_instant_floor_drop(session: Any) -> None:
    coll, redis, _client, sync = await _ready_collection(session)
    sold = raw_item("0:a", 1, "0:buyer")
    sold["owner"]["is_wallet"] = True
    d = await sync.apply_item(_api_nft(sold), now=NOW)
    assert d is not None and [p for _i, p, _pr in d.sales] == [Decimal("50")]
    assert redis.events(STREAM_NFT_SALES)[0]["nft_name"] == "Teddy #1"

    cheap = await sync.apply_item(_api_nft(raw_item("0:b", 2, "0:o2", price=20)), now=NOW)
    assert cheap is not None and [i.price for i in cheap.listings] == [Decimal("20")]
    await session.refresh(coll)
    assert Decimal(str(coll.floor_price)) == Decimal("20")  # флор снижен сразу, без прохода
    attr = (await session.execute(select(Attribute).where(Attribute.value == "Obelisk"))).scalar_one()
    assert Decimal(str(attr.floor_price)) == Decimal("20")

    other = raw_item("0:z", 9, "0:x")
    assert await sync.apply_item({**other, "collection": {"address": "0:" + "f" * 64}}) is None


async def test_stream_ignored_before_first_pass(session: Any) -> None:
    _coll, _redis, _client, sync = await _setup(session)
    assert await sync.apply_item(_api_nft(raw_item("0:a", 1, "0:o1", price=5))) is None


async def test_stream_handle_data_and_process(session: Any) -> None:
    from services.tonapi_stream import TonApiStream

    _coll, redis, _client, sync = await _ready_collection(session)

    class Client(FakeClient):
        async def fetch_nft(self, address: str) -> dict[str, Any]:
            return _api_nft(raw_item("0:b", 2, "0:o2", price=60))

    sync.client = Client()  # type: ignore[assignment]
    stream = TonApiStream(sync, Settings(tonapi_key="k", tonapi_stream_delay=0))
    stream.handle_data('{"account_id":"0:B","lt":1,"tx_hash":"h"}')
    stream.handle_data('{"account_id":"0:b","lt":2,"tx_hash":"h2"}')  # дубль в очереди
    stream.handle_data('{"account_id":"0:unknown"}')
    stream.handle_data("not json")
    assert stream.queue.qsize() == 2 and stream.stats["events"] == 3
    assert await stream.process_one(stream.queue.get_nowait()) is True
    assert await stream.process_one(stream.queue.get_nowait()) is False  # не наш NFT
    assert [e["price_ton"] for e in redis.events(STREAM_NFT_LISTINGS)] == [60.0]


async def test_hot_collections_scanned_every_cycle_cold_every_nth(session: Any) -> None:
    from db.models import Filter, User

    hot = Collection(name="Hot", contract_address="EQ" + "H" * 46, source="seed")
    cold = Collection(name="Cold", contract_address="EQ" + "C" * 46, source="seed")
    session.add_all([hot, cold, User(tg_id=1)])
    await session.flush()
    session.add(Filter(user_id=1, collection_id=hot.id, is_active=True))
    await session.commit()
    sync = TonApiSync(session, FakeRedis(), Settings(tonapi_key="k", tonapi_cold_every=3,  # type: ignore[arg-type]
                                                     tonapi_discovery_enabled=False), client=FakeClient())  # type: ignore[arg-type]
    seen: list[str] = []

    async def fake_sync(coll_id: Any, name: str, address: str, now: Any = None) -> None:
        seen.append(name)

    sync.sync_collection = fake_sync  # type: ignore[method-assign]
    for _ in range(4):
        await sync.process_collections()
    assert seen == ["Hot", "Cold", "Hot", "Hot", "Hot", "Cold"]


async def test_stream_rechecks_unindexed_item_once(session: Any) -> None:
    import asyncio

    from services.tonapi_stream import TonApiStream

    _coll, redis, _client, sync = await _ready_collection(session)
    calls = {"n": 0}

    class Client(FakeClient):
        async def fetch_nft(self, address: str) -> dict[str, Any]:
            calls["n"] += 1
            if calls["n"] == 1:  # индексатор не успел: владелец — контракт, sale нет
                item = raw_item("0:b", 2, "0:salecontract")
                item["owner"]["is_wallet"] = False
                return _api_nft(item)
            return _api_nft(raw_item("0:b", 2, "0:o2", price=33))

    sync.client = Client()  # type: ignore[assignment]
    stream = TonApiStream(sync, Settings(tonapi_key="k", tonapi_stream_delay=0, tonapi_stream_recheck=0.01))
    await stream.process_one("0:b")
    assert redis.events(STREAM_NFT_LISTINGS) == []
    await asyncio.gather(*list(stream._rechecks))
    assert calls["n"] == 2 and stream.stats["rechecked"] == 1
    assert [e["price_ton"] for e in redis.events(STREAM_NFT_LISTINGS)] == [33.0]
