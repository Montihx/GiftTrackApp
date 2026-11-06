"""Апгрейды из Telegram: стартовая точка, новые номера -> события с контактом владельца, пропуск старых, дедуп."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import orjson
from telethon.tl import functions, types

from core.config import Settings
from core.redis_client import STREAM_NFT_UPGRADES
from services.dispatcher import NotificationDispatcher
from services.telegram_upgrades import TelegramUpgradeWatcher, slug_for
from tests.test_telegram_market import FakeRedis, unique


class StubRedis(FakeRedis):
    async def get(self, key: str) -> Any:
        return self.kv.get(key)


class FakeClient:
    def __init__(self, existing: dict[int, dict[str, Any]], issued: int) -> None:
        self.existing = existing  # номер -> параметры unique()
        self.issued = issued
        self.slugs: list[str] = []

    async def __call__(self, request: Any) -> Any:
        assert isinstance(request, functions.payments.GetUniqueStarGiftRequest)
        self.slugs.append(request.slug)
        num = int(request.slug.rsplit("-", 1)[1])
        if num not in self.existing:
            raise RuntimeError("STARGIFT_SLUG_INVALID")
        gift = unique(num, owner=self.existing[num].get("owner", 777))
        gift.availability_issued = self.issued
        return types.payments.UniqueStarGift(gift=gift, chats=[], users=[types.User(id=777, username="seller777")])


def make(client: FakeClient, **kw: Any) -> tuple[TelegramUpgradeWatcher, StubRedis]:
    redis = StubRedis()
    market = SimpleNamespace(collections={"c1": ("Plush Pepe", "EQ")}, hot=["c1"], cold=[])
    return TelegramUpgradeWatcher(client, redis, market, Settings(**kw)), redis


def test_slug_for() -> None:
    assert slug_for("Plush Pepe", 727) == "PlushPepe-727" and slug_for("Durov’s Cap", 1) == "DurovsCap-1"


async def test_bootstrap_then_new_upgrades_with_owner_contact() -> None:
    client = FakeClient({1: {}, 2500: {}}, issued=2500)
    w, redis = make(client)
    assert await w.poll_collection("c1") == 0  # первый опрос — только стартовая точка, без событий
    assert redis.kv["tg_up:last:c1"] == b"2500" and client.slugs == ["PlushPepe-1"]

    client.existing[2501] = {}
    client.existing[2502] = {"owner": None}  # владелец скрыт
    client.issued = 2502
    assert await w.poll_collection("c1") == 2
    events = [orjson.loads(e["data"]) for s, e in redis.stream if s == STREAM_NFT_UPGRADES]
    assert [e["index"] for e in events] == [2501, 2502]
    assert events[0]["source"] == "telegram" and events[0]["owner_tg_id"] == 777 and events[0]["owner_username"] == "seller777"
    assert events[0]["metadata"]["name"] == "Plush Pepe #2501" and events[0]["metadata"]["model"] == "Amalgam"
    assert events[1]["owner_tg_id"] is None  # скрытый владелец — без контакта
    assert redis.kv["tg_up:last:c1"] == b"2502"
    assert await w.poll_collection("c1") == 0  # следующего номера ещё нет


async def test_long_gap_skips_old_upgrades() -> None:
    client = FakeClient({n: {} for n in range(2501, 2601)}, issued=2600)
    w, redis = make(client, tg_upgrades_skip_gap=20, tg_upgrades_max_per_poll=5)
    redis.kv["tg_up:last:c1"] = b"2500"  # простой: накопилось 100 апгрейдов
    assert await w.poll_collection("c1") == 0 and w.stats["skipped_gap"] > 0
    assert int(redis.kv["tg_up:last:c1"]) >= 2595  # перепрыгнули к свежим, старые не рассылаем
    assert not [e for s, e in redis.stream if s == STREAM_NFT_UPGRADES]


async def test_dispatcher_uses_known_owner_and_dedups_telegram_vs_onchain(session: Any) -> None:
    from db.models import Collection, Filter, User

    coll = Collection(name="Plush Pepe", contract_address="EQtg_up", source="seed")
    session.add_all([coll, User(tg_id=91)])
    await session.flush()
    session.add(Filter(user_id=91, collection_id=coll.id, is_active=True))  # require_owner_upgrade=True по умолчанию
    await session.commit()
    bot, redis, resolver = AsyncMock(), AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=None)
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=resolver)
    ev = {"type": "mint", "source": "telegram", "collection_id": str(coll.id), "nft_address": "PlushPepe-2501", "index": 2501,
          "owner_ton": "", "slug": "PlushPepe-2501", "owner_tg_id": 777, "owner_username": "seller777",
          "owner_name": "Seller", "metadata": {"name": "Plush Pepe #2501", "model": "Amalgam"}}
    await d.process_upgrade(ev)
    assert bot.send_message.await_count == 1  # контакт есть -> алерт уходит несмотря на require_owner_upgrade
    text = bot.send_message.call_args[0][1]
    assert "@seller777" in text and "АПГРЕЙД" in text
    kb = bot.send_message.call_args[1]["reply_markup"]
    assert kb.inline_keyboard[0][0].url == "https://t.me/seller777"

    # то же улучшение из блокчейна (другой адрес) — ключ дедупа тот же: (коллекция, номер)
    onchain = {**ev, "source": None, "nft_address": "0:abc", "metadata": {"name": "Plush Pepe #2501"}}
    assert d._dedup_key(STREAM_NFT_UPGRADES, ev) == d._dedup_key(STREAM_NFT_UPGRADES, onchain)
