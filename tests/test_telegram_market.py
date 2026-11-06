# tests/test_telegram_market.py — листинги маркета подарков Telegram (MTProto, Telethon-типы)
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import orjson
from telethon.tl import functions, types

from core.config import Settings
from core.redis_client import STREAM_NFT_LISTINGS
from db.models import Collection, Filter, User
from services.telegram_market import TelegramMarketWatcher, norm_title, parse_resale_gift


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}
        self.sets: dict[str, set[str]] = {}
        self.stream: list[tuple[str, dict[str, str]]] = []

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        self.kv[key] = value

    async def exists(self, key: str) -> int:
        return int(key in self.sets)

    async def sismember(self, key: str, member: str) -> bool:
        return member in self.sets.get(key, set())

    async def sadd(self, key: str, member: str) -> None:
        self.sets.setdefault(key, set()).add(member)

    async def expire(self, key: str, ttl: int) -> None:
        pass

    async def xadd(self, stream: str, entry: dict[str, str], maxlen: int | None = None,
                   approximate: bool = True) -> bytes:
        self.stream.append((stream, entry))
        return b"1-0"


def unique(num: int, stars: int | None = 1500, ton: float | None = None, owner: int | None = 777) -> types.StarGiftUnique:
    doc = types.DocumentEmpty(id=0)
    rarity = types.StarGiftAttributeRarity(permille=10)
    amounts: list[Any] = []
    if stars is not None:
        amounts.append(types.StarsAmount(amount=stars, nanos=0))
    if ton is not None:
        amounts.append(types.StarsTonAmount(amount=int(ton * 10**9)))
    return types.StarGiftUnique(
        id=num, gift_id=5555, title="Plush Pepe", slug=f"PlushPepe-{num}", num=num,
        attributes=[types.StarGiftAttributeModel(name="Amalgam", document=doc, rarity=rarity),
                    types.StarGiftAttributeBackdrop(name="Dark Lilac", backdrop_id=1, center_color=0, edge_color=0,
                                                    pattern_color=0, text_color=0, rarity=rarity),
                    types.StarGiftAttributePattern(name="Wolf Rage", document=doc, rarity=rarity)],
        availability_issued=2500, availability_total=2500,
        owner_id=types.PeerUser(user_id=owner) if owner else None, resell_amount=amounts,
    )


class FakeClient:
    def __init__(self) -> None:
        self.resale: list[types.StarGiftUnique] = []
        self.requests: list[Any] = []

    async def __call__(self, request: Any) -> Any:
        self.requests.append(request)
        if isinstance(request, functions.payments.GetStarGiftsRequest):
            gift = types.StarGift(id=5555, sticker=types.DocumentEmpty(id=0), stars=100, convert_stars=80,
                                  title="Plush Pepe", resell_min_stars=1200, availability_resale=321)
            return types.payments.StarGifts(hash=1, gifts=[gift], chats=[], users=[])
        if isinstance(request, functions.payments.GetUniqueStarGiftRequest):
            num = int(request.slug.rsplit("-", 1)[1])
            return types.payments.UniqueStarGift(gift=unique(num, owner=888), chats=[],
                                                 users=[types.User(id=888, username="hidden_in_list")])
        if isinstance(request, functions.payments.GetResaleStarGiftsRequest):
            users = [types.User(id=777, username="seller777")]
            return types.payments.ResaleStarGifts(count=len(self.resale), gifts=self.resale, chats=[], users=users)
        raise AssertionError(request)


def test_norm_title_and_parse() -> None:
    assert norm_title("Durov’s  Cap") == norm_title("durov's cap")
    item = parse_resale_gift(unique(7, stars=1500, ton=12.5), {777: types.User(id=777, username="u")})
    assert item is not None and item["attributes"] == {"model": "Amalgam", "background": "Dark Lilac", "symbol": "Wolf Rage"}
    assert item["price_stars"] == 1500 and item["price_ton"] == 12.5
    assert item["seller_tg_id"] == 777 and item["seller_username"] == "u" and item["slug"] == "PlushPepe-7"
    assert parse_resale_gift(types.StarGift(id=1, sticker=types.DocumentEmpty(id=0), stars=1, convert_stars=1), {}) is None


async def test_catalog_poll_bootstrap_then_new_listing(session: Any) -> None:
    coll = Collection(name="Plush Pepe", contract_address="EQBG-g6ahkAUGWpefWbx-D_9sQ8oWbvy6puuq78U2c4NUDFS", source="seed")
    cold = Collection(name="Unknown Gift", contract_address="EQ" + "Z" * 46, source="seed")
    session.add_all([coll, cold, User(tg_id=1)])
    await session.flush()
    session.add(Filter(user_id=1, collection_id=coll.id, is_active=True))
    await session.commit()

    @asynccontextmanager
    async def scope() -> Any:
        yield session

    redis, client = FakeRedis(), FakeClient()
    w = TelegramMarketWatcher(client, redis, scope, Settings())
    assert await w.refresh_catalog() == 1  # «Unknown Gift» нет в каталоге Telegram
    assert w.hot == [coll.id] and orjson.loads(redis.kv[f"tg_market:floor:{coll.id}"])["min_stars"] == 1200
    assert w.next_collection() == coll.id

    client.resale = [unique(1), unique(2)]
    assert await w.poll_collection(coll.id) == 0 and redis.stream == []  # первый опрос — без событий
    client.resale = [unique(3, stars=None, ton=40.0), unique(1), unique(2, stars=1400)]  # новый + смена цены
    assert await w.poll_collection(coll.id) == 2
    events = [orjson.loads(e["data"]) for s, e in redis.stream if s == STREAM_NFT_LISTINGS]
    assert [(e["nft_index"], e["price_ton"], e["price_stars"]) for e in events] == [(3, 40.0, None), (2, None, 1400)]
    assert events[0]["marketplace"] == "telegram" and events[0]["seller_tg_id"] == 777
    assert events[0]["link"] == "https://t.me/nft/PlushPepe-3" and events[0]["collection_id"] == str(coll.id)
    assert events[0]["attributes"]["symbol"] == "Wolf Rage"
    assert await w.poll_collection(coll.id) == 0  # повтор — без дублей
    req = client.requests[-1]
    assert isinstance(req, functions.payments.GetResaleStarGiftsRequest) and req.gift_id == 5555


async def test_dispatcher_telegram_listing_uses_owner_account(session: Any) -> None:
    from unittest.mock import AsyncMock

    from services.dispatcher import NotificationDispatcher

    coll = Collection(name="Plush Pepe", contract_address="EQtgm_disp", source="seed")
    session.add_all([coll, User(tg_id=5)])
    await session.flush()
    session.add(Filter(user_id=5, collection_id=coll.id, is_active=True))
    await session.commit()
    bot, resolver, redis = AsyncMock(), AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=None)
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=resolver)
    await d.process_listing({"collection_id": str(coll.id), "marketplace": "telegram", "nft_name": "Plush Pepe #3",
                             "nft_index": 3, "price_ton": None, "price_stars": 15000, "seller_ton": "",
                             "seller_tg_id": 777, "seller_username": "seller777",
                             "link": "https://t.me/nft/PlushPepe-3", "attributes": {"model": "Amalgam"}})
    resolver.resolve.assert_not_called()
    text, kwargs = bot.send_message.call_args[0][1], bot.send_message.call_args[1]
    assert "Telegram Market" in text and "⭐ 15 000" in text and "t.me/nft/PlushPepe-3" in text and "@seller777" in text
    assert kwargs["reply_markup"] is not None  # «Написать владельцу»


async def test_owner_missing_in_list_is_enriched_by_slug(session: Any) -> None:
    coll = Collection(name="Plush Pepe", contract_address="EQenrich_tgm", source="seed")
    session.add(coll)
    await session.commit()

    @asynccontextmanager
    async def scope() -> Any:
        yield session

    redis, client = FakeRedis(), FakeClient()
    w = TelegramMarketWatcher(client, redis, scope, Settings())
    await w.refresh_catalog()
    client.resale = [unique(1)]
    await w.poll_collection(coll.id)  # bootstrap
    client.resale = [unique(1), unique(9, owner=None)]  # в списке владелец не указан
    assert await w.poll_collection(coll.id) == 1
    ev = orjson.loads(redis.stream[-1][1]["data"])
    assert ev["seller_tg_id"] == 888 and ev["seller_username"] == "hidden_in_list"
    assert any(isinstance(r, functions.payments.GetUniqueStarGiftRequest) for r in client.requests)


def test_channel_owner() -> None:
    g = unique(5, owner=None)
    g.owner_id = types.PeerChannel(channel_id=42)
    chat = types.Channel(id=42, title="Gift Shop", photo=types.ChatPhotoEmpty(), date=None, username="giftshop")
    item = parse_resale_gift(g, {}, {42: chat})
    assert item is not None and item["seller_username"] == "giftshop" and item["seller_name"] == "Gift Shop"


async def test_dispatcher_hidden_owner_and_buttons(session: Any) -> None:
    from unittest.mock import AsyncMock

    from services.dispatcher import NotificationDispatcher

    coll = Collection(name="Plush Pepe", contract_address="EQhidden_disp", source="seed")
    session.add_all([coll, User(tg_id=6)])
    await session.flush()
    session.add(Filter(user_id=6, collection_id=coll.id, is_active=True, require_owner=False))
    await session.commit()
    bot, redis = AsyncMock(), AsyncMock()
    redis.get = AsyncMock(return_value=None)
    d = NotificationDispatcher(bot=bot, session=session, redis=redis, resolver=AsyncMock())
    base = {"collection_id": str(coll.id), "marketplace": "telegram", "nft_name": "Plush Pepe #4", "nft_index": 4,
            "price_ton": 10, "seller_ton": "", "link": "https://t.me/nft/PlushPepe-4", "attributes": {}}
    await d.process_listing({**base, "seller_tg_id": None, "seller_username": None, "seller_name": None})
    text, kb = bot.send_message.call_args[0][1], bot.send_message.call_args[1]["reply_markup"]
    assert "скрыт настройками приватности" in text
    assert [b.text for row in kb.inline_keyboard for b in row] == ["🎁 Открыть подарок"]
    await d.process_listing({**base, "seller_tg_id": 5, "seller_username": "vasya", "seller_name": "Vasya"})
    kb = bot.send_message.call_args[1]["reply_markup"]
    assert kb.inline_keyboard[0][0].url == "https://t.me/vasya" and kb.inline_keyboard[1][0].text == "🎁 Открыть подарок"


def test_seen_key_ignores_floating_ton_equivalent() -> None:
    from services.telegram_market import seen_key

    a = parse_resale_gift(unique(1, stars=1500, ton=20.0), {})
    b = parse_resale_gift(unique(1, stars=1500, ton=20.7), {})  # тот же листинг, курс сдвинулся
    c = parse_resale_gift(unique(1, stars=1400, ton=19.0), {})  # реальная смена цены
    d = parse_resale_gift(unique(2, stars=None, ton=5.0), {})
    assert a and b and c and d
    assert seen_key(a) == seen_key(b) and seen_key(a) != seen_key(c)
    assert seen_key(d) == "PlushPepe-2|5.0"


async def test_burst_of_new_listings_is_suppressed(session: Any) -> None:
    coll = Collection(name="Plush Pepe", contract_address="EQburst_tgm", source="seed")
    session.add(coll)
    await session.commit()

    @asynccontextmanager
    async def scope() -> Any:
        yield session

    redis, client = FakeRedis(), FakeClient()
    w = TelegramMarketWatcher(client, redis, scope, Settings(tg_market_max_new=3))
    await w.refresh_catalog()
    client.resale = [unique(1)]
    await w.poll_collection(coll.id)  # bootstrap
    client.resale = [unique(n) for n in range(10, 20)]  # внезапно 10 «новых» — сбой дедупликации, не рассылка
    assert await w.poll_collection(coll.id) == 0
    assert redis.stream == [] and w.stats["suppressed"] == 10
    assert await w.poll_collection(coll.id) == 0  # состояние обновлено — повторов нет
    client.resale = [unique(n) for n in range(10, 20)] + [unique(99)]
    assert await w.poll_collection(coll.id) == 1  # а одиночный настоящий листинг проходит
