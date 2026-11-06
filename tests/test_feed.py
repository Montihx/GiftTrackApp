"""Лента сигналов: запись карточки при отправке алерта и чтение по типам."""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import orjson

from core.config import Settings
from db.models import Collection, Filter, User
from services.dispatcher import NotificationDispatcher
from services.feed import FEED_MAX, push_card, read_cards


class ListRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[bytes]] = {}
        self.kv: dict[str, Any] = {}

    async def lpush(self, key: str, value: bytes) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        self.lists[key] = self.lists[key][start:end + 1]

    async def expire(self, key: str, ttl: int) -> None:
        pass

    async def lrange(self, key: str, start: int, end: int) -> list[bytes]:
        return self.lists.get(key, [])[start:end + 1]

    async def get(self, key: str) -> Any:
        return self.kv.get(key)

    async def incr(self, key: str) -> int:
        self.kv[key] = int(self.kv.get(key, 0)) + 1
        return self.kv[key]

    async def set(self, key: str, value: Any, ex: int | None = None, nx: bool = False) -> bool:
        self.kv[key] = value
        return True


async def test_feed_push_read_filter_and_trim() -> None:
    r = ListRedis()
    for i in range(FEED_MAX + 5):
        await push_card(r, 1, {"kind": "listing" if i % 2 else "upgrade", "title": f"G #{i}"})
    cards = await read_cards(r, 1, limit=100)
    assert len(r.lists["feed:1"]) == FEED_MAX and cards[0]["title"] == f"G #{FEED_MAX + 4}"  # новые первыми, хвост обрезан
    assert all(c["kind"] == "upgrade" for c in await read_cards(r, 1, "upgrade", 10))
    assert await read_cards(r, 2) == []


async def test_dispatcher_writes_feed_card_on_send(session: Any) -> None:
    coll = Collection(name="Plush Pepe", contract_address="EQfeed_a", source="seed")
    session.add_all([coll, User(tg_id=51)])
    await session.flush()
    session.add(Filter(user_id=51, collection_id=coll.id, is_active=True))
    await session.commit()
    r = ListRedis()
    d = NotificationDispatcher(bot=AsyncMock(), session=session, redis=r, resolver=AsyncMock(), settings=Settings())
    await d.process_upgrade({"type": "mint", "source": "telegram", "collection_id": str(coll.id), "nft_address": "PlushPepe-9",
                             "index": 9, "owner_ton": "", "slug": "PlushPepe-9", "owner_tg_id": 7, "owner_username": "seller",
                             "owner_name": "S", "metadata": {"name": "Plush Pepe #9", "model": "Amalgam", "background": "Black"}})
    card = orjson.loads(r.lists["feed:51"][0])
    assert card["kind"] == "upgrade" and card["title"] == "Plush Pepe #9" and card["owner"]["username"] == "seller"
    assert card["attrs"] == {"model": "Amalgam", "background": "Black"} and card["link"] == "https://t.me/nft/PlushPepe-9"
