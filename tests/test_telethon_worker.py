# /workspace/tests/test_telethon_worker.py — тесты cold path (скан bio)
from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import orjson
import pytest
from pytoniq_core import Address
from sqlalchemy import select

from db.models import User, WalletTgMap
from services.resolver.telethon_worker import (
    BIO_INDEX_KEY,
    BioScanner,
    extract_ton_addresses,
    normalize_address,
)

RAW = "0:" + "ab" * 32
FRIENDLY = Address(RAW).to_str(is_bounceable=False)
BOUNCE = Address(RAW).to_str(is_bounceable=True)


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict = {}
        self.hashes: dict = {}

    async def exists(self, k): return 1 if k in self.kv else 0
    async def set(self, k, v, ex=None): self.kv[k] = v
    async def get(self, k): return self.kv.get(k)
    async def hset(self, name, k, v): self.hashes.setdefault(name, {})[k] = v
    async def hget(self, name, k): return self.hashes.get(name, {}).get(k)


def test_extract_all_formats():
    text = f"my wallet {FRIENDLY} also {RAW} and {BOUNCE} ton://transfer/{FRIENDLY}"
    assert extract_ton_addresses(text) == {RAW}


def test_extract_ignores_garbage():
    assert extract_ton_addresses("EQ" + "x" * 46) == set()
    assert extract_ton_addresses(None) == set()
    assert normalize_address("not-an-address") is None


@pytest.mark.asyncio
async def test_scan_username_indexes_bio(session):
    redis = FakeRedis()
    client_calls = []

    async def client(req):
        client_calls.append(req)
        return SimpleNamespace(
            full_user=SimpleNamespace(about=f"hi {FRIENDLY}"), users=[SimpleNamespace(id=777)]
        )

    scanner = BioScanner(client, redis, None)
    assert await scanner.scan_username("wimp") == 1
    stored = orjson.loads(redis.hashes[BIO_INDEX_KEY][RAW])
    assert stored == {"tg_user_id": 777, "username": "wimp"}
    assert await redis.exists("bio_scanned:wimp")


@pytest.mark.asyncio
async def test_resolve_queued_writes_db_and_cache(session):
    redis = FakeRedis()
    await redis.hset(BIO_INDEX_KEY, RAW, orjson.dumps({"tg_user_id": 777, "username": "wimp"}))

    @asynccontextmanager
    async def scope():
        yield session

    # redis-кэш пишется через core.redis_client.cache_set_json (redis.set(key, bytes, ex=ttl))
    scanner = BioScanner(None, redis, scope)
    res = await scanner.resolve_queued(FRIENDLY)
    assert res["username"] == "wimp" and res["source"] == "telethon"
    await session.flush()
    row = (await session.execute(select(WalletTgMap))).scalar_one()
    assert row.ton_address == FRIENDLY and row.tg_user_id == 777 and row.source == "telethon"


@pytest.mark.asyncio
async def test_resolve_queued_miss(session):
    scanner = BioScanner(None, FakeRedis(), None)
    assert await scanner.resolve_queued(FRIENDLY) is None
    assert await scanner.resolve_queued("garbage") is None


@pytest.mark.asyncio
async def test_candidates_skip_recently_scanned(session):
    session.add_all([User(tg_id=1, username="a"), User(tg_id=2, username="@b"), User(tg_id=3, username=None)])
    await session.flush()
    redis = FakeRedis()
    redis.kv["bio_scanned:a"] = b"1"
    scanner = BioScanner(None, redis, None)
    assert await scanner.candidates(session) == ["b"]
