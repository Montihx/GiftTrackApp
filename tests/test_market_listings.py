"""Новые листинги MRKT/Portals: разбор, bootstrap, предохранитель, события в nft_listings."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import orjson

from core.config import Settings
from core.redis_client import STREAM_NFT_LISTINGS
from db.models import Collection
from services.market_listings import MarketListingWatcher, parse_mrkt, parse_portals


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, Any] = {}
        self.stream: list[tuple[str, dict[str, str]]] = []

    async def exists(self, key: str) -> int:
        return int(key in self.kv)

    async def set(self, key: str, value: Any, nx: bool = False, ex: int | None = None) -> bool | None:
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    async def xadd(self, stream: str, entry: dict[str, str], maxlen: int | None = None, approximate: bool = True) -> bytes:
        self.stream.append((stream, entry))
        return b"1-0"


class FakeFloors:
    def __init__(self) -> None:
        self._pause_until = {"mrkt": 0.0, "portals": 0.0}
        self.responses: dict[str, Any] = {}
        self.logged: list[str] = []

    async def _authed(self, source: str, method: str, url: str, **kw: Any) -> Any:
        return self.responses[source]

    def _log_sample(self, source: str, items: list[Any]) -> None:
        self.logged.append(source)


def mrkt(i: int, price: int = 7_000_000_000, title: str = "Plush Pepe") -> dict[str, Any]:
    return {"id": f"m{i}", "collectionTitle": title, "number": i, "salePrice": price, "modelName": "Amalgam",
            "backdropName": "Dark Lilac", "symbolName": "Wolf Rage"}


def portals(i: int, price: str = "7.5") -> dict[str, Any]:
    return {"id": f"p{i}", "name": "Plush Pepe", "external_collection_number": i, "price": price,
            "attributes": [{"type": "model", "value": "Amalgam"}, {"type": "backdrop", "value": "Black"},
                           {"type": "symbol", "value": "Cap"}]}


def test_parsers() -> None:
    m = parse_mrkt(mrkt(5))
    assert m == {"id": "m5", "title": "Plush Pepe", "number": 5, "price": 7.0,
                 "attributes": {"model": "Amalgam", "background": "Dark Lilac", "symbol": "Wolf Rage"}}
    p = parse_portals(portals(9))
    assert p and p["price"] == 7.5 and p["attributes"] == {"model": "Amalgam", "background": "Black", "symbol": "Cap"}
    assert parse_mrkt({"id": "x"}) is None and parse_portals({}) is None


async def test_bootstrap_then_new_listings_and_burst_guard(session: Any) -> None:
    coll = Collection(name="Plush Pepe", contract_address="EQmkt_listing", source="seed")
    session.add(coll)
    await session.commit()

    @asynccontextmanager
    async def scope() -> Any:
        yield session

    floors, redis = FakeFloors(), FakeRedis()
    w = MarketListingWatcher(floors, redis, scope, Settings(market_listings_max_new=5))  # type: ignore[arg-type]
    floors.responses["mrkt"] = {"gifts": [mrkt(2), mrkt(1)], "cursor": None}
    assert await w.poll_source("mrkt") == 0 and not redis.stream  # первый опрос только запоминает

    floors.responses["mrkt"] = {"gifts": [mrkt(3, price=9_500_000_000), mrkt(2), mrkt(1)], "cursor": None}
    assert await w.poll_source("mrkt") == 1
    ev = orjson.loads(redis.stream[0][1]["data"])
    assert redis.stream[0][0] == STREAM_NFT_LISTINGS and ev["marketplace"] == "mrkt" and ev["price_ton"] == 9.5
    assert ev["nft_name"] == "Plush Pepe #3" and ev["link"] == "https://t.me/nft/PlushPepe-3" and ev["collection_id"] == str(coll.id)
    assert ev["attributes"]["model"] == "Amalgam" and ev["seller_ton"] == ""
    assert await w.poll_source("mrkt") == 0  # повтор — без дублей

    floors.responses["mrkt"] = {"gifts": [mrkt(100 + i) for i in range(10)], "cursor": None}  # 10 > max_new=5
    assert await w.poll_source("mrkt") == 0 and w.stats["mrkt"]["suppressed"] == 10

    floors.responses["portals"] = {"results": [portals(1)]}
    assert await w.poll_source("portals") == 0
    floors.responses["portals"] = {"results": [portals(2), portals(1)]}
    assert await w.poll_source("portals") == 1
    assert orjson.loads(redis.stream[-1][1]["data"])["marketplace"] == "portals"

    floors.responses["mrkt"] = {"gifts": [mrkt(300, title="Unknown Gift")], "cursor": None}
    assert await w.poll_source("mrkt") == 0 and w.stats["mrkt"]["unknown_collection"] == 1  # нет в нашем каталоге
