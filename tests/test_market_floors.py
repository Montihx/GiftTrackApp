import pytest

from services.market_floors import MarketBlocked, MarketFloorWatcher, extract_price, min_price


def test_extract_price_units() -> None:
    assert extract_price({"price": 12.5}) == 12.5
    assert extract_price({"salePrice": 15_000_000_000}) == 15.0  # нано -> монета
    assert extract_price({"price": {"amount": "7"}}) == 7.0
    assert extract_price({"price": 0}) is None and extract_price({}) is None
    assert min_price([{"price": 9}, {"price": 4}, "x"]) == 4


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}

    async def set(self, key, value, ex=None):
        self.kv[key] = value


class S:
    market_floors_ttl = 600
    market_floors_interval = 0.5
    market_floors_sources = "mrkt,portals"


def make() -> MarketFloorWatcher:
    return MarketFloorWatcher(None, FakeRedis(), None, S())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_poll_writes_floor() -> None:
    w = make()

    async def fake(name):
        return 33.0

    w._mrkt_floor = fake  # type: ignore[method-assign]
    await w.poll_source("mrkt", "c1", "Plush Pepe")
    assert b"33.0" in w.redis.kv["market_floor:mrkt:c1"] and w.stats["mrkt"]["ok"] == 1


@pytest.mark.asyncio
async def test_block_pauses_source_without_bypass() -> None:
    w = make()
    calls = 0

    async def blocked(name):
        nonlocal calls
        calls += 1
        raise MarketBlocked("HTTP 403")

    w._portals_floor = blocked  # type: ignore[method-assign]
    await w.poll_source("portals", "c1", "A")
    await w.poll_source("portals", "c2", "B")  # на паузе — запрос не делается
    assert calls == 1 and w.stats["portals"]["errors"] == 1 and not w.redis.kv
