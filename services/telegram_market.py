# services/telegram_market.py — листинги встроенного маркета подарков Telegram (официальный MTProto API)
"""
Офчейн-маркет перепродажи подарков внутри Telegram (за Stars или TON) не виден в блокчейне.
Официальный и бесплатный способ его читать — методы MTProto через авторизованный аккаунт
(тот же Telethon-аккаунт, что у резолвера, сервис `resolver`):

  * payments.getStarGifts(hash=0) — каталог: id подарка, title, resell_min_stars (флор маркета
    в Stars), availability_resale (число листингов) — по всем коллекциям одним запросом;
  * payments.getResaleStarGifts(gift_id, offset="", limit) — листинги коллекции, новые сверху:
    StarGiftUnique {slug, num, attributes: Model/Backdrop/Pattern, resell_amount: Stars и/или TON,
    owner_id (Telegram-аккаунт владельца), gift_address (адрес NFT, если выведен в блокчейн)}.

Цикл: каталог — раз в tg_market_catalog_interval; листинги — по одной коллекции на запрос раз в
tg_market_interval, сначала «горячие» (фильтры/избранное пользователей), затем по кругу остальные.
Новый листинг или смена цены -> событие в STREAM_NFT_LISTINGS (marketplace="telegram", с seller_tg_id —
«Написать владельцу» работает без резолвера). Первый опрос коллекции — без событий.
FloodWait соблюдается: пауза на указанное Telegram время.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import time
from collections.abc import Callable
from typing import Any

from loguru import logger
from sqlalchemy import select

from core.config import Settings
from core.redis_client import STREAM_NFT_LISTINGS, cache_set_json, stream_publish
from db.models import Collection, Favorite, Filter

SEEN_KEY = "tg_market:seen:{cid}"
FLOOR_KEY = "tg_market:floor:{cid}"
STATE_TTL = 7 * 86400
NANO = 10**9


def norm_title(name: str) -> str:
    return " ".join(name.replace("’", "'").replace("`", "'").lower().split())


def parse_resale_gift(gift: Any, users: dict[int, Any]) -> dict[str, Any] | None:
    """StarGiftUnique -> словарь листинга (None для не-уникальных подарков)."""
    from telethon.tl import types

    if not isinstance(gift, types.StarGiftUnique):
        return None
    attrs: dict[str, str] = {}
    for a in gift.attributes or []:
        if isinstance(a, types.StarGiftAttributeModel):
            attrs["model"] = a.name
        elif isinstance(a, types.StarGiftAttributeBackdrop):
            attrs["background"] = a.name
        elif isinstance(a, types.StarGiftAttributePattern):
            attrs["symbol"] = a.name
    stars = ton = None
    for amount in gift.resell_amount or []:
        if isinstance(amount, types.StarsTonAmount):
            ton = amount.amount / NANO
        elif isinstance(amount, types.StarsAmount):
            stars = amount.amount
    owner_id = gift.owner_id.user_id if isinstance(gift.owner_id, types.PeerUser) else None
    user = users.get(owner_id) if owner_id else None
    return {
        "slug": gift.slug, "num": gift.num, "title": gift.title, "attributes": attrs,
        "price_stars": stars, "price_ton": ton, "seller_tg_id": owner_id,
        "seller_username": getattr(user, "username", None), "gift_address": gift.gift_address,
    }


def seen_key(item: dict[str, Any]) -> str:
    """Слаг + цена: перевыставление по другой цене — новый листинг."""
    return f"{item['slug']}|{item['price_stars']}|{item['price_ton']}"


class TelegramMarketWatcher:
    def __init__(self, client: Any, redis: Any, session_scope: Callable[[], Any], settings: Settings) -> None:
        self.client = client
        self.redis = redis
        self.session_scope = session_scope
        self.settings = settings
        self.gift_ids: dict[Any, int] = {}  # collection_id -> Telegram gift_id
        self.collections: dict[Any, tuple[str, str]] = {}  # collection_id -> (name, contract_address)
        self.hot: list[Any] = []
        self.cold: list[Any] = []
        self._cold_pos = 0
        self._round: list[Any] = []
        self._catalog_at: float | None = None
        self.stats = {"catalog": 0, "polls": 0, "listings": 0, "flood_waits": 0, "errors": 0}

    async def _call(self, request: Any) -> Any:
        from telethon import errors

        try:
            return await self.client(request)
        except errors.FloodWaitError as exc:
            self.stats["flood_waits"] += 1
            logger.warning(f"[tg_market] FloodWait {exc.seconds}s — пауза")
            await asyncio.sleep(exc.seconds + 1)
            return None

    async def refresh_catalog(self) -> int:
        """Каталог подарков Telegram -> соответствие нашим коллекциям + флор маркета (Stars)."""
        from telethon.tl import functions

        res = await self._call(functions.payments.GetStarGiftsRequest(hash=0))
        if res is None:
            return 0
        by_title = {norm_title(g.title): g for g in (getattr(res, "gifts", None) or []) if getattr(g, "title", None)}
        async with self.session_scope() as session:
            rows = (await session.execute(
                select(Collection.id, Collection.name, Collection.contract_address)
            )).all()
            hot_rows = await session.execute(
                select(Filter.collection_id).where(Filter.is_active.is_(True), Filter.collection_id.is_not(None))
                .union(select(Favorite.collection_id).where(Favorite.collection_id.is_not(None)))
            )
            hot = {r[0] for r in hot_rows.all()}
        self.gift_ids.clear()
        for cid, name, address in rows:
            g = by_title.get(norm_title(name))
            if g is None:
                continue
            self.gift_ids[cid] = g.id
            self.collections[cid] = (name, address)
            await cache_set_json(self.redis, FLOOR_KEY.format(cid=cid), {
                "min_stars": getattr(g, "resell_min_stars", None),
                "listed": getattr(g, "availability_resale", None),
            }, ttl=self.settings.tg_market_catalog_interval * 3)
        self.hot = [c for c in self.gift_ids if c in hot]
        self.cold = [c for c in self.gift_ids if c not in hot]
        self._catalog_at = time.monotonic()
        self.stats["catalog"] += 1
        logger.info(f"[tg_market] каталог: {len(self.gift_ids)} коллекций, горячих {len(self.hot)}")
        return len(self.gift_ids)

    def next_collection(self) -> Any | None:
        """Очередь опроса: все горячие + следующие tg_market_cold_per_round холодных, по кругу."""
        if not self._round:
            k = self.settings.tg_market_cold_per_round
            cold = self.cold[self._cold_pos:self._cold_pos + k]
            if self.cold:
                self._cold_pos = (self._cold_pos + k) % len(self.cold)
            self._round = list(self.hot) + cold
        return self._round.pop(0) if self._round else None

    async def poll_collection(self, cid: Any, now: dt.datetime | None = None) -> int:
        """Новые листинги коллекции -> события. Возвращает число опубликованных."""
        from telethon.tl import functions

        now = now or dt.datetime.now(dt.timezone.utc)
        res = await self._call(functions.payments.GetResaleStarGiftsRequest(
            gift_id=self.gift_ids[cid], offset="", limit=self.settings.tg_market_page,
        ))
        if res is None:
            return 0
        self.stats["polls"] += 1
        users = {u.id: u for u in getattr(res, "users", None) or []}
        items = [i for i in (parse_resale_gift(g, users) for g in getattr(res, "gifts", None) or []) if i]
        key = SEEN_KEY.format(cid=cid)
        bootstrap = not await self.redis.exists(key)
        new = [i for i in items if not await self.redis.sismember(key, seen_key(i))]
        for i in items:
            await self.redis.sadd(key, seen_key(i))
        await self.redis.expire(key, STATE_TTL)
        if bootstrap:
            return 0
        name, address = self.collections[cid]
        for i in new:
            await stream_publish(self.redis, STREAM_NFT_LISTINGS, {
                "type": "listing", "marketplace": "telegram", "collection_id": str(cid),
                "collection_address": address, "nft_address": i["gift_address"] or i["slug"],
                "nft_index": i["num"], "nft_name": f"{i['title'] or name} #{i['num']}",
                "price_ton": i["price_ton"], "price_stars": i["price_stars"],
                "seller_ton": "", "seller_tg_id": i["seller_tg_id"], "seller_username": i["seller_username"],
                "attributes": i["attributes"], "link": f"https://t.me/nft/{i['slug']}",
                "listed_at": now.isoformat(), "tx_hash": None,
            })
        self.stats["listings"] += len(new)
        if new:
            logger.info(f"[tg_market] {name}: новых листингов {len(new)}")
        return len(new)

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("[tg_market] старт наблюдения за маркетом подарков Telegram")
        while not stop_event.is_set():
            try:
                due = (self._catalog_at is None or
                       time.monotonic() - self._catalog_at >= self.settings.tg_market_catalog_interval)
                if due:
                    await self.refresh_catalog()
                cid = self.next_collection()
                if cid is not None:
                    await self.poll_collection(cid)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — одна ошибка не останавливает наблюдение
                self.stats["errors"] += 1
                logger.warning(f"[tg_market] итерация упала: {exc!r}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.settings.tg_market_interval)
            except asyncio.TimeoutError:
                pass
        logger.info(f"[tg_market] остановлен, статистика: {self.stats}")
