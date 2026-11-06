# tests/test_owner_lookup.py — владелец подарка по slug (Redis-очередь dispatcher <-> resolver) и фильтр «только с контактом»
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import orjson

from db.models import Collection, Filter, User
from services.owner_lookup import ALIVE_KEY, CACHE_KEY, QUEUE_KEY, OwnerLookupWorker, gift_slug, request_owner


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}
        self.queue: list[str] = []

    async def get(self, key: str) -> bytes | None:
        return self.kv.get(key)

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        self.kv[key] = value

    async def lpush(self, key: str, value: str) -> None:
        assert key == QUEUE_KEY
        self.queue.append(value)


def test_gift_slug() -> None:
    assert gift_slug("Plush Pepe #727") == "PlushPepe-727"
    assert gift_slug("Durov’s Cap #1") == "DurovsCap-1"
    assert gift_slug("Jack-in-the-Box #12") == "JackintheBox-12"
    assert gift_slug("no number") is None and gift_slug(None) is None


async def test_request_owner_cache_alive_and_answer() -> None:
    r = FakeRedis()
    assert await request_owner(r, "A-1", timeout=0.05, poll=0.01) is None and r.queue == []  # воркера нет — не ждём
    r.kv[CACHE_KEY.format(slug="A-2")] = orjson.dumps({"tg_user_id": 5, "username": "vasya"})
    assert (await request_owner(r, "A-2"))["username"] == "vasya"
    r.kv[CACHE_KEY.format(slug="A-3")] = orjson.dumps({})
    assert await request_owner(r, "A-3") is None  # известный «скрыт»

    r.kv[ALIVE_KEY] = b"1"

    async def worker() -> None:
        await asyncio.sleep(0.03)
        r.kv[CACHE_KEY.format(slug="A-4")] = orjson.dumps({"tg_user_id": 7, "username": "petya", "name": "Petya"})

    task = asyncio.create_task(worker())
    got = await request_owner(r, "A-4", timeout=1.0, poll=0.01)
    await task
    assert r.queue == ["A-4"] and got is not None and got["username"] == "petya"
    assert await request_owner(r, "A-5", timeout=0.05, poll=0.01) is None  # воркер не ответил


async def test_worker_caches_found_and_hidden() -> None:
    from telethon.tl import types

    r = FakeRedis()
    w = OwnerLookupWorker(None, r)
    calls: list[str] = []

    async def fake_lookup(slug: str) -> dict[str, Any]:
        calls.append(slug)
        return {"tg_user_id": 9, "username": "u", "name": "U"} if slug == "X-1" else {}

    w.lookup = fake_lookup  # type: ignore[method-assign]
    assert (await w.handle("X-1"))["username"] == "u"
    assert await w.handle("X-2") == {}
    await w.handle("X-1")  # из кэша
    assert calls == ["X-1", "X-2"] and w.stats == {"lookups": 2, "found": 1, "hidden": 1, "flood_waits": 0}
    assert orjson.loads(r.kv[CACHE_KEY.format(slug="X-2")]) == {}
    assert types.StarGiftUnique is not None  # telethon-типы доступны воркеру


async def test_filter_require_owner_gates_ownerless_alerts(session: Any) -> None:
    from services.dispatcher import NotificationDispatcher

    coll = Collection(name="Plush Pepe", contract_address="EQgate_owner", source="seed")
    session.add_all([coll, User(tg_id=21), User(tg_id=22), User(tg_id=23)])
    await session.flush()
    session.add_all([Filter(user_id=21, collection_id=coll.id, is_active=True),  # require_owner по умолчанию
                     Filter(user_id=22, collection_id=coll.id, is_active=True, require_owner=False,
                            require_owner_upgrade=False),
                     # листинги без контакта можно, апгрейды — только с контактом (две отдельные галочки)
                     Filter(user_id=23, collection_id=coll.id, is_active=True, require_owner=False)])
    await session.commit()

    bot, redis, resolver = AsyncMock(), AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=None)
    resolver.resolve = AsyncMock(return_value={})  # контакта нет
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=resolver)
    base = {"collection_id": str(coll.id), "marketplace": "getgems", "nft_name": "Plush Pepe #5", "nft_index": 5,
            "price_ton": 10, "seller_ton": "0:seller", "attributes": {}}
    await d.process_listing(base)
    assert [c[0][0] for c in bot.send_message.call_args_list] == [22, 23]  # только те, кто разрешил слать листинги без контакта

    bot.send_message.reset_mock()
    resolver.resolve = AsyncMock(return_value={"tg_user_id": 77, "username": "seller"})
    await d.process_listing(base)
    assert sorted(c[0][0] for c in bot.send_message.call_args_list) == [21, 22, 23]  # контакт есть — получают все

    bot.send_message.reset_mock()
    resolver.resolve = AsyncMock(return_value={})
    await d.process_upgrade({"collection_id": str(coll.id), "nft_address": "0:n", "index": 5, "owner_ton": "",
                             "metadata": {"name": "Plush Pepe #5", "model": "Amalgam", "symbol": "Moon"}})
    assert [c[0][0] for c in bot.send_message.call_args_list] == [22]
    assert "АПГРЕЙД" in bot.send_message.call_args[0][1] and "Символ: Moon" in bot.send_message.call_args[0][1]


def test_value_from_info_units() -> None:
    from types import SimpleNamespace

    from services.owner_lookup import value_from_info

    ton = value_from_info(SimpleNamespace(currency="ton", value=3_801_000_000_000, floor_price=3_000_000_000_000,
                                          average_price=None, last_sale_price=None, listed_count=12))
    assert ton == {"currency": "TON", "value": 3801.0, "floor": 3000.0, "average": None, "last_sale": None, "listed": 12}
    usd = value_from_info(SimpleNamespace(currency="USD", value=410050, floor_price=None, average_price=None,
                                          last_sale_price=380000, listed_count=None))
    assert usd is not None and usd["value"] == 4100.5 and usd["last_sale"] == 3800.0
    assert value_from_info(SimpleNamespace(currency="TON", value=None)) is None


async def test_number_and_price_ranges_filter_alerts(session: Any) -> None:
    from services.dispatcher import NotificationDispatcher, gift_number

    assert gift_number("Plush Pepe #727") == 727 and gift_number(None, "PlushPepe-42") == 42 and gift_number("x") is None

    coll = Collection(name="Plush Pepe", contract_address="EQnum_price", source="seed")
    session.add_all([coll, User(tg_id=41), User(tg_id=42), User(tg_id=43)])
    await session.flush()
    session.add_all([
        Filter(user_id=41, collection_id=coll.id, is_active=True, require_owner=False, number_max=100),
        Filter(user_id=42, collection_id=coll.id, is_active=True, require_owner=False, price_min=50),
        Filter(user_id=43, collection_id=coll.id, is_active=True, require_owner=False, number_min=1, price_max=20),
    ])
    await session.commit()
    bot, redis, resolver = AsyncMock(), AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=None)
    resolver.resolve = AsyncMock(return_value={})
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=resolver)
    base = {"collection_id": str(coll.id), "marketplace": "getgems", "nft_name": "Plush Pepe #5", "nft_index": 5,
            "price_ton": 10, "seller_ton": "0:seller", "attributes": {}}
    await d.process_listing(base)  # №5, цена 10 -> #41 (номер<=100) и #43 (номер>=1, цена<=20); #42 нужна цена>=50
    assert sorted(c[0][0] for c in bot.send_message.call_args_list) == [41, 43]
    bot.send_message.reset_mock()
    await d.process_listing({**base, "nft_name": "Plush Pepe #500", "price_ton": 60})
    assert sorted(c[0][0] for c in bot.send_message.call_args_list) == [42]


async def test_any_gift_filter_matches_every_collection(session: Any) -> None:
    from services.dispatcher import NotificationDispatcher

    coll = Collection(name="Plush Pepe", contract_address="EQany_gift", source="seed")
    session.add_all([coll, User(tg_id=51)])
    await session.flush()
    session.add(Filter(user_id=51, collection_id=None, is_active=True, require_owner=False))  # «Любой подарок»
    await session.commit()
    bot, redis, resolver = AsyncMock(), AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=None)
    resolver.resolve = AsyncMock(return_value={})
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=resolver)
    await d.process_listing({"collection_id": str(coll.id), "marketplace": "getgems", "nft_name": "Plush Pepe #5",
                             "nft_index": 5, "price_ton": 10, "seller_ton": "0:seller", "attributes": {}})
    assert [c[0][0] for c in bot.send_message.call_args_list] == [51]


async def test_price_range_applies_to_sales(session: Any) -> None:
    from services.dispatcher import NotificationDispatcher

    coll = Collection(name="Snoop Dogg", contract_address="EQsale_price", source="seed")
    session.add_all([coll, User(tg_id=61), User(tg_id=62)])
    await session.flush()
    session.add_all([Filter(user_id=61, collection_id=None, is_active=True, price_min=100),  # «Любой подарок», только дорогие
                     Filter(user_id=62, collection_id=None, is_active=True)])
    await session.commit()
    bot, redis, resolver = AsyncMock(), AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=None)
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=resolver)
    sale = {"collection_id": str(coll.id), "nft_address": "0:n", "nft_name": "Snoop Dogg #5", "price_ton": 6.0,
            "marketplace": "getgems", "attributes": {}}
    await d.process_sale(sale)
    assert [c[0][0] for c in bot.send_message.call_args_list] == [62]  # дешёвая продажа не доходит до фильтра «от 100»
    bot.send_message.reset_mock()
    await d.process_sale({**sale, "price_ton": 250.0})
    assert sorted(c[0][0] for c in bot.send_message.call_args_list) == [61, 62]


async def test_event_without_matching_filter_skips_owner_lookup(session: Any) -> None:
    from services.dispatcher import NotificationDispatcher

    coll = Collection(name="Plush Pepe", contract_address="EQprecheck_a", source="seed")
    other = Collection(name="Scared Cat", contract_address="EQprecheck_b", source="seed")
    session.add_all([coll, other, User(tg_id=71)])
    await session.flush()
    session.add(Filter(user_id=71, collection_id=other.id, is_active=True, require_owner=False))  # подписан на другую
    await session.commit()
    bot, redis, resolver = AsyncMock(), AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=b"1")  # воркер владельцев «жив»: без предпроверки событие бы ждало ответ
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=resolver)
    await d.process_listing({"collection_id": str(coll.id), "marketplace": "getgems", "nft_name": "Plush Pepe #5",
                             "nft_index": 5, "price_ton": 10, "seller_ton": "0:seller", "attributes": {}})
    await d.process_upgrade({"collection_id": str(coll.id), "nft_address": "0:n", "index": 5, "owner_ton": "0:o",
                             "metadata": {"name": "Plush Pepe #5"}})
    resolver.resolve.assert_not_awaited()  # адрес продавца не резолвим
    redis.lpush.assert_not_awaited()  # и воркер Telegram не просим
    bot.send_message.assert_not_awaited()
    assert d.stats["listings"] == 1 and d.stats["upgrades"] == 1
