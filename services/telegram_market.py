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
from db.models import Attribute, Collection, Favorite, Filter

SEEN_KEY = "tg_market:seen:{cid}"
FLOOR_KEY = "tg_market:floor:{cid}"
STATS_KEY = "tg_market:stats:{cid}"  # {"issued": N, "listed": N, "ton_per_star": x} — для API и уведомлений
RATE_KEY = "tg_market:rate"  # GRAM за 1 Star (медиана по листингам, где есть обе цены)
STATE_TTL = 7 * 86400
NANO = 10**9


def norm_title(name: str) -> str:
    return " ".join(name.replace("’", "'").replace("`", "'").lower().split())


def parse_resale_gift(gift: Any, users: dict[int, Any], chats: dict[int, Any] | None = None) -> dict[str, Any] | None:
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
    owner_id = username = None
    name = getattr(gift, "owner_name", None)
    if isinstance(gift.owner_id, types.PeerUser):
        owner_id = gift.owner_id.user_id
        user = users.get(owner_id)
        username = getattr(user, "username", None)
        if user is not None and not name:
            name = " ".join(x for x in (getattr(user, "first_name", None), getattr(user, "last_name", None)) if x) or None
    elif isinstance(gift.owner_id, types.PeerChannel):  # владелец — канал: пишем через его @username
        chat = (chats or {}).get(gift.owner_id.channel_id)
        username = getattr(chat, "username", None)
        name = name or getattr(chat, "title", None)
    return {
        "slug": gift.slug, "num": gift.num, "title": gift.title, "attributes": attrs,
        "price_stars": stars, "price_ton": ton, "seller_tg_id": owner_id,
        "seller_username": username, "seller_name": name, "gift_address": gift.gift_address,
        "issued": getattr(gift, "availability_issued", None),
    }


def attribute_rows(attrs: list[Any]) -> list[tuple[str, str]]:
    """Атрибуты улучшения подарка Telegram -> (тип, значение) для таблицы attributes.

    Узор подарка в интерфейсе называется «Символ», поэтому Telegram-pattern хранится как symbol.
    """
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for a in attrs:
        cls = type(a).__name__
        typ = {"StarGiftAttributeModel": "model", "StarGiftAttributeBackdrop": "background",
               "StarGiftAttributePattern": "symbol"}.get(cls)
        name = getattr(a, "name", None)
        if typ and name and (typ, name) not in seen:
            seen.add((typ, name))
            out.append((typ, str(name)))
    return out


def ton_per_star(items: list[dict[str, Any]]) -> float | None:
    """Курс из листингов, где Telegram показывает обе цены (цена в TON — пересчёт из Stars)."""
    ratios = sorted(i["price_ton"] / i["price_stars"] for i in items if i["price_ton"] and i["price_stars"])
    return ratios[len(ratios) // 2] if ratios else None


def has_owner(item: dict[str, Any]) -> bool:
    return bool(item["seller_tg_id"] or item["seller_username"])


def seen_key(item: dict[str, Any]) -> str:
    """Слаг + БАЗОВАЯ цена: перевыставление по другой цене — новый листинг.

    Цена в TON у листинга за Stars — пересчёт по текущему курсу и «плавает», поэтому сравниваем по
    Stars; по TON — только для листингов, выставленных за TON (resell_ton_only).
    """
    base = item["price_stars"] if item["price_stars"] is not None else item["price_ton"]
    return f"{item['slug']}|{base}"


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
        self.stats = {"catalog": 0, "polls": 0, "listings": 0, "flood_waits": 0, "errors": 0, "seeded": 0}
        self._seeded: set[Any] = set()

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

    async def seed_attributes(self, limit: int = 3) -> int:
        """Новая коллекция без атрибутов: модели/фоны/символы берём у Telegram (список улучшения подарка).

        Пока ончейн-скан не дошёл до коллекции, её страница пуста — а список моделей нужен фильтрам.
        Заполняются только коллекции, у которых атрибутов ещё нет, не больше `limit` за вызов;
        ончейн-синхронизация потом дополнит те же строки (уникальность collection+type+value).
        """
        from telethon.tl import functions

        done = 0
        async with self.session_scope() as session:
            have = {r[0] for r in (await session.execute(select(Attribute.collection_id).distinct())).all()}
        for cid, gift_id in list(self.gift_ids.items()):
            if done >= limit:
                break
            if cid in have or cid in self._seeded:
                continue
            self._seeded.add(cid)
            res = await self._call(functions.payments.GetStarGiftUpgradeAttributesRequest(gift_id=gift_id))
            rows = attribute_rows(getattr(res, "attributes", None) or [])
            if not rows:
                continue
            async with self.session_scope() as session:
                for typ, value in rows:
                    session.add(Attribute(collection_id=cid, type=typ, value=value))
                await session.flush()
            done += 1
            self.stats["seeded"] += 1
            logger.info(f"[tg_market] {self.collections[cid][0]}: атрибутов из Telegram: {len(rows)}")
        return done

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
        chats = {c.id: c for c in getattr(res, "chats", None) or []}
        items = [i for i in (parse_resale_gift(g, users, chats) for g in getattr(res, "gifts", None) or []) if i]
        # Статистика коллекции для витрины и уведомлений: число улучшенных (у Telegram их больше, чем в блокчейне),
        # число листингов и курс Stars -> монета
        rate = ton_per_star(items)
        issued = max((i["issued"] for i in items if i["issued"]), default=None)
        await cache_set_json(self.redis, STATS_KEY.format(cid=cid), {
            "issued": issued, "listed": getattr(res, "count", None), "ton_per_star": rate,
        }, ttl=STATE_TTL)
        if rate:
            await cache_set_json(self.redis, RATE_KEY, rate, ttl=STATE_TTL)
        key = SEEN_KEY.format(cid=cid)
        bootstrap = not await self.redis.exists(key)
        new = [i for i in items if not await self.redis.sismember(key, seen_key(i))]
        for i in items:
            await self.redis.sadd(key, seen_key(i))
        await self.redis.expire(key, STATE_TTL)
        if bootstrap:
            return 0
        if len(new) > self.settings.tg_market_max_new:
            # Реальных новых листингов за один опрос (~минуты) бывает единицы. Десятки — это сбой дедупликации
            # (смена курса, сброс Redis, другая сортировка): не рассылаем, состояние уже обновлено.
            logger.warning(f"[tg_market] {self.collections[cid][0]}: {len(new)} «новых» за опрос — пропускаю (предохранитель)")
            self.stats["suppressed"] = self.stats.get("suppressed", 0) + len(new)
            return 0
        name, address = self.collections[cid]
        # В общем списке Telegram не всегда отдаёт владельца — дозапрашиваем подарок по slug (только новые)
        for i in [i for i in new if not has_owner(i)][:5]:
            await self._enrich_owner(i)
        for i in new:
            await stream_publish(self.redis, STREAM_NFT_LISTINGS, {
                "type": "listing", "marketplace": "telegram", "collection_id": str(cid),
                "collection_address": address, "nft_address": i["gift_address"] or i["slug"],
                "nft_index": i["num"], "nft_name": f"{i['title'] or name} #{i['num']}",
                "price_ton": i["price_ton"], "price_stars": i["price_stars"],
                "seller_ton": "", "seller_tg_id": i["seller_tg_id"], "seller_username": i["seller_username"],
                "seller_name": i["seller_name"],
                "attributes": i["attributes"], "link": f"https://t.me/nft/{i['slug']}",
                "listed_at": now.isoformat(), "tx_hash": None,
            })
        self.stats["listings"] += len(new)
        if new:
            logger.info(f"[tg_market] {name}: новых листингов {len(new)}")
        return len(new)

    async def _enrich_owner(self, item: dict[str, Any]) -> None:
        from telethon.tl import functions

        try:
            res = await self._call(functions.payments.GetUniqueStarGiftRequest(slug=item["slug"]))
        except Exception as exc:  # noqa: BLE001 — не критично: уведомление уйдёт без владельца
            logger.debug(f"[tg_market] владелец {item['slug']}: {exc!r}")
            return
        if res is None:
            return
        users = {u.id: u for u in getattr(res, "users", None) or []}
        chats = {c.id: c for c in getattr(res, "chats", None) or []}
        full = parse_resale_gift(res.gift, users, chats)
        if full is not None:
            for k in ("seller_tg_id", "seller_username", "seller_name"):
                item[k] = item[k] or full[k]

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("[tg_market] старт наблюдения за маркетом подарков Telegram")
        while not stop_event.is_set():
            try:
                due = (self._catalog_at is None or
                       time.monotonic() - self._catalog_at >= self.settings.tg_market_catalog_interval)
                if due:
                    await self.refresh_catalog()
                    await self.seed_attributes()
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
