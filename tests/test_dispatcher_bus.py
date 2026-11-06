"""Шина событий диспетчера: consumer group, XACK, повторы, DLQ, дедупликация, забор зависших."""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import orjson

from core.config import Settings
from core.redis_client import DISPATCHER_GROUP, STREAM_DLQ, STREAM_NFT_LISTINGS, STREAM_NFT_SALES
from services.dispatcher import NotificationDispatcher


class FakeBusRedis:
    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}
        self.acked: list[tuple[str, str]] = []
        self.dlq: list[dict[str, Any]] = []
        self.groups: list[str] = []
        self.inbox: list[tuple[str, str, dict[str, Any]]] = []  # (stream, id, fields) — ещё не прочитано
        self.stale: list[tuple[str, str, dict[str, Any]]] = []  # pending у «упавшего» потребителя

    async def xgroup_create(self, stream: str, group: str, id: str = "$", mkstream: bool = False) -> None:
        if stream in self.groups:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        self.groups.append(stream)

    async def set(self, key: str, value: bytes, nx: bool = False, ex: int | None = None) -> bool | None:
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    async def delete(self, key: str) -> None:
        self.kv.pop(key, None)

    async def xack(self, stream: str, group: str, msg_id: str) -> None:
        assert group == DISPATCHER_GROUP
        self.acked.append((stream, msg_id))

    async def xadd(self, stream: str, fields: dict[str, Any], maxlen: int | None = None, approximate: bool = True) -> bytes:
        assert stream == STREAM_DLQ
        self.dlq.append(fields)
        return b"1-0"

    async def xreadgroup(self, group: str, consumer: str, streams: dict[str, str], count: int = 50, block: int = 0) -> Any:
        by_stream: dict[str, list[Any]] = {}
        for stream, msg_id, fields in self.inbox:
            by_stream.setdefault(stream, []).append((msg_id.encode(), fields))
        self.inbox = []
        await asyncio.sleep(0.01)  # как настоящий BLOCK: отдаём управление циклу событий
        return [(s.encode(), msgs) for s, msgs in by_stream.items()]

    async def xautoclaim(self, stream: str, group: str, consumer: str, min_idle_time: int, start_id: str, count: int) -> Any:
        mine = [(i, f) for s, i, f in self.stale if s == stream]
        self.stale = [t for t in self.stale if t[0] != stream]
        return (b"0-0", [(i.encode(), f) for i, f in mine], [])


def make(redis: FakeBusRedis, **kw: Any) -> NotificationDispatcher:
    settings = Settings(dispatch_retry_base=0, **kw)
    d = NotificationDispatcher(bot=AsyncMock(), session=AsyncMock(), redis=redis, resolver=AsyncMock(), settings=settings)
    return d


def event(**p: Any) -> dict[str, Any]:
    return {b"data": orjson.dumps({"nft_address": "0:a", "price_ton": 5, **p})}


async def test_success_acks_and_duplicate_is_not_processed_twice() -> None:
    r = FakeBusRedis()
    d = make(r)
    d.process_sale = AsyncMock()  # type: ignore[method-assign]
    await d._process_with_retries(STREAM_NFT_SALES, "1-0", event())
    await d._process_with_retries(STREAM_NFT_SALES, "2-0", event())  # то же событие (повторная доставка)
    assert d.process_sale.await_count == 1 and d.stats["dedup"] == 1
    assert r.acked == [(STREAM_NFT_SALES, "1-0"), (STREAM_NFT_SALES, "2-0")] and not r.dlq


async def test_poison_event_goes_to_dlq_after_max_attempts_and_does_not_stop() -> None:
    r = FakeBusRedis()
    d = make(r, dispatch_max_attempts=3)
    d.process_listing = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
    await d._process_with_retries(STREAM_NFT_LISTINGS, "5-0", event())
    assert d.process_listing.await_count == 3  # ровно 3 попытки
    assert r.dlq and r.dlq[0]["stream"] == STREAM_NFT_LISTINGS and "boom" in r.dlq[0]["reason"]
    assert r.acked == [(STREAM_NFT_LISTINGS, "5-0")]  # после DLQ сообщение подтверждено — поток не залипает
    # отметка дедупа снята: повторная доставка не будет принята за дубль
    assert not r.kv


async def test_broken_payload_without_data_is_dlq_and_acked() -> None:
    r = FakeBusRedis()
    d = make(r)
    await d._process_with_retries(STREAM_NFT_SALES, "9-0", {b"x": b"y"})
    assert r.dlq[0]["reason"] == "нет поля data" and r.acked == [(STREAM_NFT_SALES, "9-0")]


async def test_run_loop_reads_group_claims_stale_and_stops() -> None:
    r = FakeBusRedis()
    r.inbox = [(STREAM_NFT_SALES, "1-0", event(nft_address="0:new"))]
    r.stale = [(STREAM_NFT_LISTINGS, "0-1", event(nft_address="0:old", price_ton=7))]
    d = make(r)
    d.process_sale = AsyncMock()  # type: ignore[method-assign]
    d.process_listing = AsyncMock()  # type: ignore[method-assign]
    stop = asyncio.Event()

    async def stopper() -> None:
        await asyncio.sleep(0.1)
        stop.set()

    await asyncio.gather(d.run_loop(stop), stopper())
    assert len(r.groups) == 3  # группы созданы для всех трёх потоков
    assert d.process_sale.await_count == 1 and d.process_listing.await_count == 1  # новое + забранное зависшее
    assert (STREAM_NFT_LISTINGS, "0-1") in r.acked and (STREAM_NFT_SALES, "1-0") in r.acked
