# /workspace/services/dispatcher.py — рассылка алертов подписчикам
"""
Dispatcher: слушает Redis Streams (nft_upgrades, nft_listings), собирает данные
(владелец, floor prices, recent sales, owner history), фильтрует подписчиков
по их фильтрам и шлёт HTML-уведомления с кнопкой "Написать владельцу".

Формат сообщения для АПГРЕЙДА (mint):
    🚨 АПГРЕЙД
    🎭 Scared Cat #1285
    Модель: 🕷 Obelisk
    Фон: 🌑 Silver Blue
    Узор: 🌙 Moon

    👤 Владелец: @wimp
    🔗 GetGems
    [💬 Написать владельцу]

Формат сообщения для ЛИСТИНГА:
    ✅ ЛИСТИНГ
    🎭 Scared Cat #1285 на GetGems за 2400.0 TON
    Модель: 🕷 Obelisk
    Фон: 🌑 Silver Blue
    Узор: 🌙 Moon

    Последние владельцы:
    @wimp - 2 месяца, 7 дней назад
    @sadness - 11 месяцев назад

    Флор гифта: 155.0 TON
    Флор модели: 1500.0 TON

    Последние продажи модели:
    #8681 - 3503.0 TON - GetGems - 1 день назад
    #3343 - 3433.0 TON - GetGems - 1 день назад

Формат сообщения для ПРОДАЖИ (stream nft_sales, TonAPI-sync):
    💸 GIFT SOLD!
    🎁 Plush Pepe #2675
    Model: Amalgam
    Backdrop: Dark Lilac
    Symbol: Wolf Rage

    💰 Price: 2500 TON (≈ $2 727)
    🏪 Sold on GetGems
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import orjson
import uuid
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions
from loguru import logger
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import (
    DISPATCHER_GROUP,
    STREAM_DLQ,
    STREAM_NFT_LISTINGS,
    STREAM_NFT_SALES,
    STREAM_NFT_UPGRADES,
    cache_get_json,
    floor_key,
    owner_history_key,
    recent_sales_key,
)
from db.models import Attribute, Filter, User
from services.rate_limit import SendRateLimiter
from services.user_settings import can_receive
from services.owner_lookup import gift_slug, request_owner
from services.resolver.resolver import OwnerResolver


MARKET_TITLES = {
    "getgems": "GetGems", "marketapp": "Marketapp", "fragment": "Fragment", "portals": "Portals",
    "tonnel": "Tonnel", "mrkt": "MRKT", "xgift": "XGift", "tonapi": "TON", "telegram": "Telegram Market",
}


def _coin() -> str:
    return get_settings().coin_label


def _fmt_money(amount: float | None, currency: str | None) -> str:
    """Сумма с единицей: TON -> название монеты (GRAM), Stars -> ⭐, фиат -> $/код."""
    if amount is None:
        return "—"
    cur = (currency or "TON").upper()
    if cur == "TON":
        return f"{_fmt_num(amount)} {_coin()}"
    if cur == "XTR":
        return f"⭐ {_fmt_num(amount)}"
    if cur == "USD":
        return f"${_fmt_num(amount)}"
    return f"{_fmt_num(amount)} {cur}"


def _valuation_lines(info: dict[str, Any]) -> list[str]:
    v = (info or {}).get("value") or {}
    if v.get("value") is None:
        return []
    cur = v.get("currency")
    lines = [f"💎 Оценка Telegram: ≈ <b>{_fmt_money(v['value'], cur)}</b>"]
    if v.get("last_sale") is not None:
        lines.append(f"Последняя продажа: {_fmt_money(v['last_sale'], cur)}")
    return lines


def nft_url(addr: str | None = None, name: str | None = None) -> str | None:
    """Ссылка на подарок — только Telegram (t.me/nft/Slug-N); без номера в названии ссылки нет (сторонних сайтов не даём)."""
    slug = gift_slug(name)
    return f"https://t.me/nft/{slug}" if slug else None


def _linked(title: str, url: str | None, bold: bool = False) -> str:
    text = f"<b>{title}</b>" if bold else title
    return f"<a href='{url}'>{text}</a>" if url else text


def gift_number(name: str | None, slug: str | None = None) -> int | None:
    """Номер подарка: «Plush Pepe #727» или slug «PlushPepe-727»."""
    import re

    for src, pat in ((name, r"#\s*(\d+)"), (slug, r"-(\d+)$")):
        m = re.search(pat, src or "")
        if m:
            return int(m.group(1))
    return None


def _in_range(value: float | int | None, lo: Any, hi: Any) -> bool:
    """Диапазон фильтра; если он задан, а значение неизвестно — не шлём (как «Флор модели»)."""
    if lo is None and hi is None:
        return True
    if value is None:
        return False
    return not ((lo is not None and value < float(lo)) or (hi is not None and value > float(hi)))


def _has_contact(owner: dict[str, Any]) -> bool:
    return bool(owner.get("username") or owner.get("tg_user_id"))


def market_title(slug: str | None) -> str:
    return MARKET_TITLES.get((slug or "").lower(), slug or "?")


def _fmt_num(value: float) -> str:
    """2727.5 -> '2 727.5', 5.0 -> '5'."""
    text = f"{value:,.2f}".rstrip("0").rstrip(".")
    return text.replace(",", " ")


def _parse_uuid(raw: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(raw)) if raw else None
    except (ValueError, AttributeError):
        return None


def format_sale_message(payload: dict[str, Any]) -> str:
    """Текст уведомления «GIFT SOLD!» по событию из STREAM_NFT_SALES."""
    attrs = payload.get("attributes") or {}
    name = payload.get("nft_name") or f"{payload.get('collection_name') or 'Gift'} #{payload.get('nft_index', '?')}"
    addr = payload.get("nft_address", "")
    lines = ["💸 <b>GIFT SOLD!</b>", f"🎁 {_linked(name, nft_url(addr, name), bold=True)}"]
    for key, label in (("model", "Model"), ("background", "Backdrop"), ("symbol", "Symbol")):
        if attrs.get(key):
            lines.append(f"{label}: {attrs[key]}")
    lines.append("")
    price = float(payload.get("price_ton") or 0)
    usd = payload.get("price_usd")
    usd_part = f" (≈ ${_fmt_num(float(usd))})" if usd else ""
    lines.append(f"💰 Price: <b>{_fmt_num(price)} {_coin()}</b>{usd_part}")
    lines.append(f"🏪 Sold on {market_title(payload.get('marketplace'))}")
    return "\n".join(lines)


def _format_time_ago(ts: dt.datetime | None) -> str:
    """Форматирует timedelta в человекочитаемый вид ('2 месяца, 7 дней назад')."""
    if ts is None:
        return "неизвестно"
    delta = dt.datetime.now(dt.timezone.utc) - ts
    days = delta.days
    if days < 1:
        return "сегодня"
    if days < 30:
        return f"{days} дн. назад"
    months = days // 30
    rem_days = days % 30
    if months < 12:
        return f"{months} мес., {rem_days} дн. назад" if rem_days else f"{months} мес. назад"
    years = months // 12
    rem_months = months % 12
    return f"{years} г., {rem_months} мес. назад" if rem_months else f"{years} г. назад"


class NotificationDispatcher:
    """
    Слушает Redis Streams и рассылает алерты подписчикам.

    DI: bot (aiogram.Bot), session (AsyncSession), redis, resolver (OwnerResolver).
    """

    def __init__(
        self,
        bot: Bot,
        session: AsyncSession,
        redis: Any,
        resolver: OwnerResolver,
        settings: Settings | None = None,
    ) -> None:
        self.bot = bot
        self.session = session
        self.redis = redis
        self.resolver = resolver
        self.settings = settings or get_settings()
        self.stats = {"upgrades": 0, "listings": 0, "sales": 0, "sent": 0, "errors": 0}
        self.limiter = SendRateLimiter(
            redis, global_per_sec=self.settings.send_rate_global,
            user_per_sec=self.settings.send_rate_user_sec, user_per_min=self.settings.send_rate_user_min,
        )

    async def process_upgrade(self, payload: dict[str, Any]) -> None:
        """Обработка события апгрейда (mint)."""
        nft_address = payload.get("nft_address", "")
        owner_ton = payload.get("owner_ton", "")
        metadata = payload.get("metadata") or {}
        collection_id_raw = payload.get("collection_id")

        # Конвертируем collection_id из str в uuid.UUID
        collection_id = None
        if collection_id_raw:
            try:
                collection_id = uuid.UUID(str(collection_id_raw))
            except (ValueError, AttributeError):
                pass

        # Владелец: кошелёк -> резолвер; для только что улучшенного подарка владелец (кошелёк минтера) ничего не
        # даёт — тогда Telegram по slug подарка
        name = metadata.get("name") or f"NFT #{payload.get('index', '?')}"
        slug = payload.get("slug") or gift_slug(name)
        if not await self._has_candidates(collection_id, metadata, "upgrade", number=gift_number(name, slug)):
            self.stats["upgrades"] += 1
            return
        # Апгрейд из Telegram уже несёт контакт владельца (owner_tg_id/owner_username/owner_name)
        known = {"tg_user_id": payload.get("owner_tg_id"), "username": payload.get("owner_username"),
                 "name": payload.get("owner_name")} if payload.get("source") == "telegram" else None
        owner, info = await self._find_owner(owner_ton or "", slug, known)
        owner_link = self._owner_link(owner, owner_ton or "")
        tme = f"https://t.me/nft/{slug}" if slug else None

        # Формируем сообщение
        lines = ["🚨 <b>АПГРЕЙД</b>"]
        lines.append(f"🎭 <b>{name}</b>")
        if metadata.get("model"):
            lines.append(f"Модель: {metadata['model']}")
        if metadata.get("background"):
            lines.append(f"Фон: {metadata['background']}")
        if metadata.get("symbol"):
            lines.append(f"Символ: {metadata['symbol']}")
        if metadata.get("pattern"):
            lines.append(f"Узор: {metadata['pattern']}")
        lines.extend(await self._rarity_and_supply_lines(collection_id, metadata))
        lines.extend(_valuation_lines(info))
        lines.append("")
        lines.append(f"👤 Владелец: {owner_link}")

        text = "\n".join(lines)
        keyboard = self._owner_keyboard(owner, gift_link=tme)

        await self._send_to_subscribers(
            collection_id, metadata, text, keyboard, has_contact=_has_contact(owner), preview_url=tme,
            kind="upgrade", number=gift_number(name, slug),
        )
        self.stats["upgrades"] += 1


    async def process_listing(self, payload: dict[str, Any]) -> None:
        """Обработка события листинга."""
        nft_address = payload.get("nft_address", "")
        seller_ton = payload.get("seller_ton", "")
        attributes = payload.get("attributes") or {}
        collection_id_raw = payload.get("collection_id")
        price = payload.get("price_ton", 0.0)
        marketplace = payload.get("marketplace", "getgems")

        # Конвертируем collection_id из str в uuid.UUID
        collection_id = None
        if collection_id_raw:
            try:
                collection_id = uuid.UUID(str(collection_id_raw))
            except (ValueError, AttributeError):
                pass

        # Продавец: маркет Telegram сразу даёт аккаунт владельца, иначе — резолвим TON-адрес
        slug = payload.get("slug") or gift_slug(payload.get("nft_name"))
        if not slug and "t.me/nft/" in str(payload.get("link") or ""):
            slug = str(payload["link"]).rsplit("/", 1)[-1]
        known = ({"tg_user_id": payload.get("seller_tg_id"), "username": payload.get("seller_username"),
                  "name": payload.get("seller_name")} if marketplace == "telegram" else None)
        title_for_number = payload.get("nft_name") or f"#{payload.get('nft_index', '?')}"
        if not await self._has_candidates(
            collection_id, attributes, "listing", number=gift_number(title_for_number, slug),
            price=float(price) if price else None,
        ):
            self.stats["listings"] += 1
            return  # никому не нужен — не ждём воркер Telegram и не собираем текст
        owner, info = await self._find_owner(seller_ton, slug, known)
        owner_link = self._owner_link(owner, seller_ton)
        tme = f"https://t.me/nft/{slug}" if slug else None

        # Собираем аналитику из Redis
        floor_coll = await cache_get_json(self.redis, floor_key(str(collection_id) if collection_id else ""))
        model_value = attributes.get("model")
        floor_model = (
            await cache_get_json(self.redis, floor_key(str(collection_id) if collection_id else "", model_value))
            if model_value and collection_id
            else None
        )
        recent_sales = (
            await cache_get_json(self.redis, recent_sales_key(str(collection_id) if collection_id else "", model_value))
            if model_value and collection_id
            else None
        )
        owner_history = await cache_get_json(self.redis, owner_history_key(nft_address))

        # Формируем сообщение
        lines = ["✅ <b>ЛИСТИНГ</b>"]
        title = payload.get("nft_name") or f"#{payload.get('nft_index', '?')}"
        link = payload.get("link") if str(payload.get("link") or "").startswith("https://t.me/") else None
        link = link or nft_url(nft_address, payload.get("nft_name"))  # ссылки только на Telegram
        lines.append(f"🎭 {_linked(title, link)} на {market_title(marketplace)}")
        stars = payload.get("price_stars")
        if price:
            lines.append(f"💰 <b>{_fmt_num(float(price))} {_coin()}</b>" + (f" · ⭐ {stars:,}".replace(",", " ") if stars else ""))
        elif stars:
            lines.append(f"💰 <b>⭐ {stars:,}</b>".replace(",", " "))
        if attributes.get("model"):
            lines.append(f"Модель: {attributes['model']}")
        if attributes.get("background"):
            lines.append(f"Фон: {attributes['background']}")
        if attributes.get("symbol"):
            lines.append(f"Символ: {attributes['symbol']}")
        if attributes.get("pattern"):
            lines.append(f"Узор: {attributes['pattern']}")
        lines.extend(await self._rarity_and_supply_lines(collection_id, attributes))
        lines.extend(_valuation_lines(info))
        lines.append("")
        lines.append(f"👤 Продавец: {owner_link}")


        if owner_history:
            lines.append("")
            lines.append("<b>Последние владельцы:</b>")
            for h in owner_history[:3]:
                uname = h.get("username") or h.get("owner_ton", "")[:12]
                lines.append(f"@{uname} - {_format_time_ago(h.get('sold_at'))}")

        if floor_coll is not None:
            lines.append("")
            lines.append(f"Флор гифта: <b>{floor_coll} {_coin()}</b>")
        if floor_model is not None:
            lines.append(f"Флор модели: <b>{floor_model} {_coin()}</b>")
        
        if recent_sales:
            prices = [float(s.get("price", 0)) for s in recent_sales if s.get("price")]
            if prices:
                prices.sort()
                median = prices[len(prices) // 2]
                lines.append(f"📊 Медиана продаж модели: <b>~{median} {_coin()}</b>")

        if recent_sales:
            lines.append("")
            lines.append("<b>Последние продажи модели:</b>")
            for s in recent_sales[:3]:
                sold_at = s.get("sold_at")
                sold_dt = None
                if isinstance(sold_at, str):
                    try:
                        sold_dt = dt.datetime.fromisoformat(sold_at.replace("Z", "+00:00"))
                    except ValueError:
                        pass
                lines.append(
                    f"#{s.get('nft_index', '?')} - {s.get('price', '?')} {_coin()} - "
                    f"{market_title(s.get('marketplace'))} - {_format_time_ago(sold_dt)}"
                )

        text = "\n".join(lines)
        keyboard = self._owner_keyboard(owner, gift_link=tme or link)

        await self._send_to_subscribers(
            collection_id, attributes, text, keyboard, has_contact=_has_contact(owner), preview_url=tme,
            kind="listing", number=gift_number(title, slug), price=float(price) if price else None,
        )
        self.stats["listings"] += 1


    async def process_sale(self, payload: dict[str, Any]) -> None:
        """Обработка события продажи -> «GIFT SOLD!» подписчикам коллекции/модели."""
        collection_id = _parse_uuid(payload.get("collection_id"))
        text = format_sale_message(payload)
        # Диапазоны фильтра «Номер» и «Цена» действуют и на продажи (цена продажи), иначе «Любой подарок» заваливает дешёвыми
        price = float(payload.get("price_ton") or 0) or None
        await self._send_to_subscribers(
            collection_id, payload.get("attributes") or {}, text, None,
            kind="sale", number=gift_number(payload.get("nft_name"), payload.get("slug")), price=price,
        )
        self.stats["sales"] += 1

    async def _find_owner(
        self, owner_ton: str, slug: str | None, known: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """(контакт владельца, данные подарка из Telegram: оценка).

        Контакт: известный из события -> кошелёк (резолвер) -> Telegram по slug (воркер resolver). Оценку и
        владельца по slug отдаёт один запрос воркера, поэтому он нужен и когда контакт уже известен.
        """
        owner: dict[str, Any] = dict(known or {})
        if not _has_contact(owner) and owner_ton:
            try:
                owner = {**owner, **(await self.resolver.resolve(owner_ton) or {})}
            except Exception as exc:  # noqa: BLE001 — резолвер не должен ронять уведомление
                logger.debug(f"resolver: {exc!r}")
        info: dict[str, Any] = {}
        if slug and self.settings.owner_lookup_enabled:
            info = await request_owner(
                self.redis, slug, timeout=self.settings.owner_lookup_timeout,
                max_queue=self.settings.owner_lookup_max_queue,
            ) or {}
            if info and not _has_contact(owner):
                owner = {**owner, **{k: v for k, v in info.items() if k != "value"}}
        return owner, info

    async def _match_users(
        self,
        collection_id: str | None,
        attributes: dict[str, Any],
        has_contact: bool = True,
        kind: str = "sale",
        number: int | None = None,
        price: float | None = None,
    ) -> tuple[set[int], int]:
        """(пользователи с подходящими фильтрами, сколько фильтров отсеяно из-за отсутствия контакта).

        Фильтр совпадает, если совпали все заданные в нём поля: модель, фон, символ, узор, диапазоны
        «Флор модели», «Номер» и «Цена» (floor модели: Redis floor:{cid}:{model}, иначе БД).
        """
        stmt = select(Filter).where(Filter.is_active.is_(True))
        if collection_id:
            # фильтр «Любой подарок» (collection_id IS NULL) подходит к любой коллекции
            stmt = stmt.where(or_(Filter.collection_id == collection_id, Filter.collection_id.is_(None)))
        filters = (await self.session.execute(stmt)).scalars().all()

        if not filters:
            logger.debug("Нет активных подписчиков на это событие")
            return set(), 0

        attr_ids = {
            fid for f in filters
            for fid in (f.model_id, f.background_id, f.symbol_id, f.pattern_id) if fid is not None
        }
        values: dict[Any, str] = {}
        if attr_ids:
            rows = await self.session.execute(select(Attribute.id, Attribute.value).where(Attribute.id.in_(attr_ids)))
            values = {aid: val for aid, val in rows.all()}

        model_floor: float | None = None
        if any(f.floor_min is not None or f.floor_max is not None for f in filters):
            model_floor = await self._model_floor(collection_id, attributes.get("model"))

        target_users = set()
        no_contact = 0
        for f in filters:
            if not has_contact and (
                (kind == "listing" and f.require_owner) or (kind == "upgrade" and f.require_owner_upgrade)
            ):
                no_contact += 1
                continue  # фильтр просит слать только с контактом владельца
            if not _in_range(number, f.number_min, f.number_max):
                continue
            if kind in ("listing", "sale") and not _in_range(price, f.price_min, f.price_max):
                continue
            ok = True
            for fid, key in ((f.model_id, "model"), (f.background_id, "background"),
                             (f.symbol_id, "symbol"), (f.pattern_id, "pattern")):
                if fid is not None and fid in values and attributes.get(key) != values[fid]:
                    ok = False
                    break
            if ok and (f.floor_min is not None or f.floor_max is not None):
                if model_floor is None:
                    ok = False  # диапазон задан, а floor модели неизвестен — не шлём
                elif f.floor_min is not None and model_floor < float(f.floor_min):
                    ok = False
                elif f.floor_max is not None and model_floor > float(f.floor_max):
                    ok = False
            if ok:
                target_users.add(f.user_id)
        return await self._subscribed_users(target_users), no_contact

    async def _has_candidates(
        self,
        collection_id: str | None,
        attributes: dict[str, Any],
        kind: str,
        number: int | None = None,
        price: float | None = None,
    ) -> bool:
        """Есть ли хоть один фильтр, которому событие подходит (контакт владельца не учитываем).

        Дешёвая проверка ПЕРЕД поиском владельца: запрос к воркеру Telegram ждёт до owner_lookup_timeout секунд,
        а событий в потоке сотни в минуту — без неё диспетчер (он один и обрабатывает события по очереди)
        застревал на тех, кому они никому не нужны, и алерты приходили с большим опозданием.
        """
        users, _ = await self._match_users(collection_id, attributes, has_contact=True, kind=kind,
                                           number=number, price=price)
        return bool(users)

    async def _send_to_subscribers(
        self,
        collection_id: str | None,
        attributes: dict[str, Any],
        text: str,
        keyboard: InlineKeyboardMarkup | None,
        has_contact: bool = True,
        preview_url: str | None = None,
        kind: str = "sale",
        number: int | None = None,
        price: float | None = None,
    ) -> None:
        """Находит подписчиков с подходящими фильтрами и шлёт им сообщение параллельно."""
        target_users, no_contact = await self._match_users(
            collection_id, attributes, has_contact=has_contact, kind=kind, number=number, price=price
        )
        if not target_users:
            if no_contact:
                # видно в логах, почему алерта нет: у события нет контакта владельца, а фильтр его требует
                self.stats["skipped_no_contact"] = self.stats.get("skipped_no_contact", 0) + 1
                logger.info(
                    f"[dispatcher] {kind}: {no_contact} фильтр(ов) пропущено — нет контакта владельца "
                    f"(всего пропусков {self.stats['skipped_no_contact']})"
                )
            return

        # Отправляем параллельно
        # Превью ссылки: карточка подарка Telegram (t.me/nft/…) с картинкой, а не баннер маркетплейса
        preview = LinkPreviewOptions(url=preview_url, show_above_text=False) if preview_url \
            else LinkPreviewOptions(is_disabled=True)
        tasks = [self._send_limited(uid, text, keyboard, preview) for uid in target_users]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for uid, res in zip(target_users, results, strict=True):
            if isinstance(res, Exception):
                logger.warning(f"Не удалось отправить алерт пользователю {uid}: {res!r}")
                self.stats["errors"] += 1
            else:
                self.stats["sent"] += 1
                await self._count("sent")

        logger.info(f"Алерт отправлен {len(target_users)} подписчикам")

    async def _count(self, name: str) -> None:
        """Суточный счётчик для /stats (Redis, хранится 8 дней)."""
        try:
            key = f"stats:{name}:{dt.datetime.now(dt.timezone.utc):%Y%m%d}"
            await self.redis.incr(key)
            await self.redis.expire(key, 8 * 86400)
        except Exception:  # noqa: BLE001 — статистика не должна ломать доставку
            pass

    async def _send_limited(self, uid: int, text: str, keyboard: Any, preview: Any) -> Any:
        """Отправка с лимитами (ТЗ 8.7): минутный лимит -> сводка «ещё N», 429 -> ждём retry_after, 403 -> не пишем."""
        from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

        blocked = await self.redis.get(f"rl:blocked:{uid}")
        if isinstance(blocked, (bytes, str)):
            return None  # пользователь заблокировал бота
        if not await self.limiter.acquire(uid):
            self.stats["throttled"] = self.stats.get("throttled", 0) + 1
            return None
        over = await self.limiter.pop_over(uid)
        if over:
            await self.bot.send_message(
                uid, f"ℹ️ Ещё {over} сигналов не показано: лимит {self.settings.send_rate_user_min} в минуту. "
                     "Остальное — в ленте мини-приложения.",
            )
        for attempt in (1, 2):
            try:
                return await self.bot.send_message(
                    uid, text, parse_mode="HTML", reply_markup=keyboard, link_preview_options=preview
                )
            except TelegramRetryAfter as exc:
                if attempt == 2:
                    raise
                await asyncio.sleep(exc.retry_after)
            except TelegramForbiddenError:
                await self.redis.set(f"rl:blocked:{uid}", b"1", ex=86400)
                logger.info(f"[dispatcher] пользователь {uid} заблокировал бота — отправка остановлена на сутки")
                return None
        return None

    async def _subscribed_users(self, user_ids: set[int]) -> set[int]:
        """Из user_ids — без забаненных и (если SUBSCRIPTION_REQUIRED=true) только с активной платной подпиской."""
        if not user_ids:
            return user_ids
        users = (await self.session.execute(select(User).where(User.tg_id.in_(user_ids)))).scalars().all()
        blocked = {u.tg_id for u in users if not can_receive(u)}  # бан, пауза /pause, тихие часы /quiet
        user_ids = user_ids - blocked
        if not self.settings.subscription_required or not user_ids:
            return user_ids
        now = dt.datetime.now(dt.timezone.utc)
        rows = (await self.session.execute(
            select(User.tg_id, User.sub_plan, User.sub_expires_at).where(User.tg_id.in_(user_ids))
        )).all()
        ok: set[int] = set()
        for tg_id, plan, expires in rows:
            if expires is not None and expires.tzinfo is None:
                expires = expires.replace(tzinfo=dt.timezone.utc)
            if plan and plan != "free" and expires is not None and expires > now:
                ok.add(tg_id)
        return ok

    async def _model_floor(self, collection_id: Any, model: str | None) -> float | None:
        if collection_id is None or not model:
            return None
        cached = await cache_get_json(self.redis, floor_key(str(collection_id), model))
        if isinstance(cached, (int, float)) and cached > 0:
            return float(cached)
        db_floor = (
            await self.session.execute(
                select(Attribute.floor_price).where(
                    Attribute.collection_id == collection_id, Attribute.type == "model", Attribute.value == model
                )
            )
        ).scalar_one_or_none()
        return float(db_floor) if db_floor else None

    def _owner_link(self, owner: dict[str, Any], fallback_ton: str) -> str:
        """Формирует ссылку на владельца: @username или tg://user?id= или TON-адрес."""
        if owner.get("username"):
            return f"@{owner['username']}"
        label = owner.get("name") or "пользователь"
        if owner.get("tg_user_id"):
            return f"<a href='tg://user?id={owner['tg_user_id']}'>{label}</a>"
        if owner.get("name"):
            return f"{owner['name']} (контакт скрыт)"
        if fallback_ton:
            return f"кошелёк <code>{fallback_ton[:10]}…</code>"  # без ссылки на сторонний сайт
        return "скрыт настройками приватности"

    def _owner_keyboard(self, owner: dict[str, Any], gift_link: str | None = None) -> InlineKeyboardMarkup | None:
        """«Написать владельцу» (t.me/username или tg://user?id=) и «Открыть подарок»."""
        rows: list[list[InlineKeyboardButton]] = []
        if owner.get("username"):
            rows.append([InlineKeyboardButton(text="💬 Написать владельцу", url=f"https://t.me/{owner['username']}")])
        elif owner.get("tg_user_id"):
            rows.append([InlineKeyboardButton(text="💬 Написать владельцу", url=f"tg://user?id={owner['tg_user_id']}")])
        if gift_link and gift_link.startswith("https://t.me/"):
            rows.append([InlineKeyboardButton(text="🎁 Открыть подарок", url=gift_link)])
        return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

    async def _rarity_and_supply_lines(
        self, collection_id: Any, attributes: dict[str, Any]
    ) -> list[str]:
        """
        Строки редкости и наличия для уведомления.

        Редкость = attr.items_count / collection.items_count * 100 (приближённо,
        по выборке rarity_sample_size). Наличие = число улучшенных из Telegram (tg_market:stats), иначе collections.items_count (on-chain).
        """
        if collection_id is None:
            return []

        from db.models import Collection

        coll = (
            await self.session.execute(
                select(Collection.items_count).where(Collection.id == collection_id)
            )
        ).scalar_one_or_none()
        if not coll:
            return []

        # Выпущено: число улучшенных по данным Telegram (оно больше, чем подарков в блокчейне); нет — on-chain
        stats = await cache_get_json(self.redis, f"tg_market:stats:{collection_id}")
        issued = stats.get("issued") if isinstance(stats, dict) else None
        total = int(issued) if isinstance(issued, (int, float)) and issued > coll else coll
        lines = [f"📦 Выпущено: <b>{total:,}</b> NFT".replace(",", " ")]

        for attr_type in ("model", "background", "symbol", "pattern"):
            value = attributes.get(attr_type)
            if not value:
                continue
            attr = (
                await self.session.execute(
                    select(Attribute.items_count).where(
                        Attribute.collection_id == collection_id,
                        Attribute.type == attr_type,
                        Attribute.value == value,
                    )
                )
            ).scalar_one_or_none()
            if attr:
                pct = round(attr / coll * 100, 2) if coll else 0
                label = {"model": "Модель", "background": "Фон", "symbol": "Символ", "pattern": "Узор"}[attr_type]
                lines.append(f"🎲 {label} {value}: <b>{pct}%</b>")

        return lines


    # ---- шина событий: consumer group, XACK, повторы, DLQ, дедупликация (ТЗ 5.2, 5.4) ----
    STREAMS = (STREAM_NFT_UPGRADES, STREAM_NFT_LISTINGS, STREAM_NFT_SALES)

    async def _ensure_groups(self) -> None:
        """Группа создаётся с «$»: накопленный до первого запуска хвост не разбираем, события только новые."""
        for stream in self.STREAMS:
            try:
                await self.redis.xgroup_create(stream, DISPATCHER_GROUP, id="$", mkstream=True)
            except Exception as exc:  # noqa: BLE001
                if "BUSYGROUP" not in str(exc):
                    raise

    def _dedup_key(self, stream: str, payload: dict[str, Any]) -> str:
        addr = payload.get("nft_address") or payload.get("slug") or payload.get("nft_name") or ""
        kind = {STREAM_NFT_UPGRADES: "upgrade", STREAM_NFT_LISTINGS: "listing", STREAM_NFT_SALES: "sale"}[stream]
        if stream == STREAM_NFT_UPGRADES:
            # один апгрейд приходит и из Telegram, и из блокчейна: ключ — (коллекция, номер), а не адрес
            number = gift_number((payload.get("metadata") or {}).get("name"), payload.get("slug"))
            if number is not None and payload.get("collection_id"):
                return f"dedup:ev:upgrade:{payload['collection_id']}:{number}"
        tail = payload.get("tx_hash") or payload.get("price_stars") or payload.get("price_ton") or ""
        return f"dedup:ev:{kind}:{addr}:{tail}"

    async def _handle_message(self, stream: str, payload: dict[str, Any]) -> None:
        """Одно событие. Повторная доставка того же события (рестарт, XAUTOCLAIM) второй алерт не даёт."""
        ok = await self.redis.set(self._dedup_key(stream, payload), b"1", nx=True, ex=self.settings.dispatch_dedup_ttl)
        if not ok:
            self.stats["dedup"] = self.stats.get("dedup", 0) + 1
            return
        try:
            if stream == STREAM_NFT_UPGRADES:
                await self.process_upgrade(payload)
            elif stream == STREAM_NFT_LISTINGS:
                await self.process_listing(payload)
            elif stream == STREAM_NFT_SALES:
                await self.process_sale(payload)
        except BaseException:
            # не обработали — снимаем отметку, чтобы повтор (в процессе или после рестарта) не посчитали дублем
            await self.redis.delete(self._dedup_key(stream, payload))
            raise

    async def _process_with_retries(self, stream: str, msg_id: str, fields: dict[Any, Any]) -> None:
        """До dispatch_max_attempts попыток с экспоненциальной паузой, затем событие в ev:dlq. Всегда завершается XACK."""
        raw = fields.get(b"data") or fields.get("data")
        reason = "нет поля data"
        if raw is not None:
            reason = ""
            for attempt in range(1, self.settings.dispatch_max_attempts + 1):
                try:
                    payload = orjson.loads(raw.decode() if isinstance(raw, bytes) else raw)
                    await self._handle_message(stream, payload)
                    await self._count({"nft_upgrades": "upgrades", "nft_listings": "listings", "nft_sales": "sales"}[stream])
                    reason = ""
                    break
                except asyncio.CancelledError:
                    raise  # не ACK: сообщение останется pending и будет забрано после рестарта
                except Exception as exc:  # noqa: BLE001 — одно событие не останавливает диспетчер
                    reason = repr(exc)
                    self.stats["errors"] += 1
                    logger.warning(f"[dispatcher] {stream} {msg_id}: попытка {attempt} не удалась: {reason}")
                    try:
                        await self.session.rollback()
                    except Exception:  # noqa: BLE001
                        pass
                    if attempt < self.settings.dispatch_max_attempts:
                        await asyncio.sleep(self.settings.dispatch_retry_base * 2 ** (attempt - 1))
        if reason:
            await self.redis.xadd(STREAM_DLQ, {"stream": stream, "id": msg_id, "reason": reason[:500],
                                               "data": raw if raw is not None else ""}, maxlen=10_000, approximate=True)
            self.stats["dlq"] = self.stats.get("dlq", 0) + 1
            logger.error(f"[dispatcher] {stream} {msg_id} -> {STREAM_DLQ}: {reason}")
        await self.redis.xack(stream, DISPATCHER_GROUP, msg_id)

    async def _claim_stale(self, consumer: str) -> int:
        """Сообщения, зависшие у упавшего потребителя (pending дольше dispatch_claim_idle_ms), — забираем и обрабатываем."""
        done = 0
        for stream in self.STREAMS:
            res = await self.redis.xautoclaim(stream, DISPATCHER_GROUP, consumer,
                                              min_idle_time=self.settings.dispatch_claim_idle_ms, start_id="0-0", count=50)
            for msg_id, fields in (res[1] if res else []) or []:
                if fields is None:  # сообщение уже вычищено из потока
                    await self.redis.xack(stream, DISPATCHER_GROUP, msg_id)
                    continue
                await self._process_with_retries(stream, msg_id.decode() if isinstance(msg_id, bytes) else msg_id, fields)
                done += 1
        return done

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Основной цикл: XREADGROUP по потокам событий, XACK после обработки, повторы и ev:dlq."""
        import os
        import time

        consumer = f"c:{os.uname().nodename}:{os.getpid()}"
        logger.info(f"[dispatcher] старт, consumer {consumer}")
        await self._ensure_groups()
        last_claim = 0.0
        while not stop_event.is_set():
            try:
                if time.monotonic() - last_claim > 30:
                    last_claim = time.monotonic()
                    claimed = await self._claim_stale(consumer)
                    if claimed:
                        logger.info(f"[dispatcher] забрано зависших сообщений: {claimed}")
                response = await self.redis.xreadgroup(
                    DISPATCHER_GROUP, consumer, {s: ">" for s in self.STREAMS}, count=50, block=2000
                )
                for stream_name, messages in response or []:
                    stream_name = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    for msg_id, fields in messages:
                        msg_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        await self._process_with_retries(stream_name, msg_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[dispatcher] итерация упала: {exc!r}", exc_info=True)
                self.stats["errors"] += 1
                await asyncio.sleep(1.0)

        logger.info(f"[dispatcher] остановлен, статистика: {self.stats}")
