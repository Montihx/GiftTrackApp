# tests/test_gift_discovery.py — автообнаружение коллекций Telegram Gifts (деплоер = Fragment Gift Minter)
from __future__ import annotations

from typing import Any

from sqlalchemy import select

from core.config import Settings
from db.models import Collection
from services.gift_discovery import (
    MINTER_ADDRESS,
    GiftCollectionDiscovery,
    gift_name_from_item,
    raw_address,
)
from tests.test_tonapi_sync import FakeRedis

REAL = "0:" + "a1" * 32
FAKE = "0:" + "b2" * 32
KNOWN = "0:46fa0e9a864014196a5e7d66f1f83ffdb10f2859bbf2ea9baeabbf14d9ce0d50"  # Plush Pepes
KNOWN_EQ = "EQBG-g6ahkAUGWpefWbx-D_9sQ8oWbvy6puuq78U2c4NUDFS"


def nft(coll: str, name: str) -> dict[str, Any]:
    return {"address": "0:x", "collection": {"address": coll}, "metadata": {"name": name}}


class FakeTonApi:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages
        self.offsets: list[int] = []
        self.verified: list[str] = []

    async def fetch_account_nfts(self, account: str, offset: int, limit: int = 1000) -> list[dict[str, Any]]:
        assert account == MINTER_ADDRESS
        self.offsets.append(offset)
        idx = offset // limit
        return self.pages[idx] if idx < len(self.pages) else []

    async def fetch_deployer(self, address: str) -> str | None:
        self.verified.append(address)
        return MINTER_ADDRESS if address == REAL else "0:" + "cc" * 32

    async def fetch_collection(self, address: str) -> dict[str, Any]:
        return {"metadata": {"name": "Spiced Wines", "image": "https://nft.fragment.com/collection/spicedwine.webp"},
                "metadata_status": {"url": "https://nft.fragment.com/collection/spicedwine.json"}}


def _settings(**kw: Any) -> Settings:
    return Settings(tonapi_key="k", tonapi_discovery_pages=2, **kw)


def test_helpers() -> None:
    assert gift_name_from_item("Spiced Wine #123") == "Spiced Wine"
    assert gift_name_from_item("Durov’s Cap  # 7 ") == "Durov’s Cap"
    assert gift_name_from_item(None) is None
    assert raw_address(KNOWN_EQ) == KNOWN
    assert raw_address(KNOWN.upper().replace("0X", "0x")) == KNOWN


async def test_adds_only_verified_new_collections(session: Any) -> None:
    session.add(Collection(name="Plush Pepe", contract_address=KNOWN_EQ, source="seed"))
    await session.commit()
    redis = FakeRedis()
    client = FakeTonApi([[nft(KNOWN, "Plush Pepe #1"), nft(REAL, "Spiced Wine #5")], [nft(FAKE, "Plush Pepe #9")]])
    disc = GiftCollectionDiscovery(session, redis, _settings(), client, page_limit=2)  # type: ignore[arg-type]

    res = await disc.run()
    assert res == {"seen": 3, "candidates": 2, "added": 1, "backfilled": 0, "rejected": 1}
    assert client.offsets == [0, 2]  # вторая страница короткая -> курсор по кругу
    new = (await session.execute(select(Collection).where(Collection.source == "tonapi"))).scalar_one()
    assert new.name == "Spiced Wine" and new.contract_address.startswith("EQ")
    assert raw_address(new.contract_address) == REAL
    assert new.icon_url == "https://nft.fragment.com/collection/spicedwine.webp"
    assert redis.kv["gift_discovery:cursor"] == b"0"

    # повтор: известные пропускаются, отклонённая не проверяется заново
    client.verified.clear()
    res = await disc.run()
    assert res["added"] == 0 and res["rejected"] == 0 and client.verified == []


async def test_placeholder_backfilled_by_name(session: Any) -> None:
    session.add(Collection(name="Spiced Wine", contract_address="tg_gift:spiced_wine", source="seed"))
    await session.commit()
    client = FakeTonApi([[nft(REAL, "Spiced Wine #5")]])
    disc = GiftCollectionDiscovery(session, FakeRedis(), _settings(), client, page_limit=2)  # type: ignore[arg-type]
    res = await disc.run()
    assert res["backfilled"] == 1 and res["added"] == 0
    c = (await session.execute(select(Collection))).scalar_one()
    assert raw_address(c.contract_address) == REAL and c.source == "seed"


async def test_maybe_run_respects_interval_and_switch(session: Any) -> None:
    client = FakeTonApi([[]])
    disc = GiftCollectionDiscovery(session, FakeRedis(), _settings(tonapi_discovery_interval=3600),  # type: ignore[arg-type]
                                   client, page_limit=2)
    assert await disc.maybe_run() is not None
    assert await disc.maybe_run() is None  # интервал ещё не прошёл
    assert disc.due(now=10**9)
    off = GiftCollectionDiscovery(session, FakeRedis(), _settings(tonapi_discovery_enabled=False),  # type: ignore[arg-type]
                                  client, page_limit=2)
    assert not off.due() and await off.maybe_run() is None
