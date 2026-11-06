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
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import (
    STREAM_NFT_LISTINGS,
    STREAM_NFT_SALES,
    STREAM_NFT_UPGRADES,
    cache_get_json,
    floor_key,
    owner_history_key,
    recent_sales_key,
)
from db.models import Attribute, Filter
from services.owner_lookup import gift_slug, request_owner
from services.resolver.resolver import OwnerResolver


MARKET_TITLES = {
    "getgems": "GetGems", "marketapp": "Marketapp", "fragment": "Fragment", "portals": "Portals",
    "tonnel": "Tonnel", "mrkt": "MRKT", "xgift": "XGift", "tonapi": "TON", "telegram": "Telegram Market",
}


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
    lines = ["💸 <b>GIFT SOLD!</b>", f"🎁 <a href='https://getgems.io/nft/{addr}'><b>{name}</b></a>"]
    for key, label in (("model", "Model"), ("background", "Backdrop"), ("symbol", "Symbol")):
        if attrs.get(key):
            lines.append(f"{label}: {attrs[key]}")
    lines.append("")
    price = float(payload.get("price_ton") or 0)
    usd = payload.get("price_usd")
    usd_part = f" (≈ ${_fmt_num(float(usd))})" if usd else ""
    lines.append(f"💰 Price: <b>{_fmt_num(price)} TON</b>{usd_part}")
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
        owner = await self._find_owner(owner_ton or "", name)
        owner_link = self._owner_link(owner, owner_ton or "")

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
        lines.append("")
        lines.append(f"👤 Владелец: {owner_link}")
        lines.append(f"🔗 <a href='https://getgems.io/nft/{nft_address}'>GetGems</a>")

        text = "\n".join(lines)
        keyboard = self._owner_keyboard(owner)

        await self._send_to_subscribers(collection_id, metadata, text, keyboard, has_contact=_has_contact(owner))
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
        if marketplace == "telegram":
            owner = {"tg_user_id": payload.get("seller_tg_id"), "username": payload.get("seller_username"),
                     "name": payload.get("seller_name")}
        else:
            owner = await self._find_owner(seller_ton, payload.get("nft_name"))
        owner_link = self._owner_link(owner, seller_ton)

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
        link = payload.get("link") or f"https://getgems.io/nft/{nft_address}"
        lines.append(f"🎭 <a href='{link}'>{title}</a> на {market_title(marketplace)}")
        stars = payload.get("price_stars")
        if price:
            lines.append(f"💰 <b>{_fmt_num(float(price))} TON</b>" + (f" · ⭐ {stars:,}".replace(",", " ") if stars else ""))
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
            lines.append(f"Флор гифта: <b>{floor_coll} TON</b>")
        if floor_model is not None:
            lines.append(f"Флор модели: <b>{floor_model} TON</b>")
        
        if recent_sales:
            prices = [float(s.get("price", 0)) for s in recent_sales if s.get("price")]
            if prices:
                prices.sort()
                median = prices[len(prices) // 2]
                lines.append(f"💎 Ценность: <b>~{median} TON</b> (медиана продаж)")

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
                    f"#{s.get('nft_index', '?')} - {s.get('price', '?')} TON - "
                    f"{market_title(s.get('marketplace'))} - {_format_time_ago(sold_dt)}"
                )

        text = "\n".join(lines)
        keyboard = self._owner_keyboard(owner, gift_link=link)

        await self._send_to_subscribers(collection_id, attributes, text, keyboard, has_contact=_has_contact(owner))
        self.stats["listings"] += 1


    async def process_sale(self, payload: dict[str, Any]) -> None:
        """Обработка события продажи -> «GIFT SOLD!» подписчикам коллекции/модели."""
        collection_id = _parse_uuid(payload.get("collection_id"))
        text = format_sale_message(payload)
        await self._send_to_subscribers(collection_id, payload.get("attributes") or {}, text, None)
        self.stats["sales"] += 1

    async def _find_owner(self, owner_ton: str, nft_name: str | None) -> dict[str, Any]:
        """Контакт владельца: сначала кошелёк (резолвер), затем Telegram по slug подарка (воркер resolver)."""
        owner: dict[str, Any] = {}
        if owner_ton:
            try:
                owner = await self.resolver.resolve(owner_ton) or {}
            except Exception as exc:  # noqa: BLE001 — резолвер не должен ронять уведомление
                logger.debug(f"resolver: {exc!r}")
        if not _has_contact(owner) and self.settings.owner_lookup_enabled:
            slug = gift_slug(nft_name)
            if slug:
                found = await request_owner(self.redis, slug, timeout=self.settings.owner_lookup_timeout)
                if found:
                    owner = {**owner, **found}
        return owner

    async def _send_to_subscribers(
        self,
        collection_id: str | None,
        attributes: dict[str, Any],
        text: str,
        keyboard: InlineKeyboardMarkup | None,
        has_contact: bool = True,
    ) -> None:
        """Находит подписчиков с подходящими фильтрами и шлёт им сообщение параллельно.

        Фильтр совпадает, если совпали все заданные в нём поля: модель, фон, символ, узор
        и диапазон «Флор модели от/до» (floor модели: Redis floor:{cid}:{model}, иначе БД).
        """
        stmt = select(Filter).where(Filter.is_active.is_(True))
        if collection_id:
            stmt = stmt.where(Filter.collection_id == collection_id)
        filters = (await self.session.execute(stmt)).scalars().all()

        if not filters:
            logger.debug("Нет активных подписчиков на это событие")
            return

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
        for f in filters:
            if not has_contact and f.require_owner:
                continue  # фильтр просит слать только с контактом владельца
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

        if not target_users:
            return

        # Отправляем параллельно
        tasks = [
            self.bot.send_message(uid, text, parse_mode="HTML", reply_markup=keyboard)
            for uid in target_users
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for uid, res in zip(target_users, results, strict=True):
            if isinstance(res, Exception):
                logger.warning(f"Не удалось отправить алерт пользователю {uid}: {res!r}")
                self.stats["errors"] += 1
            else:
                self.stats["sent"] += 1

        logger.info(f"Алерт отправлен {len(target_users)} подписчикам")

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
            return f"<code>{fallback_ton[:16]}…</code>"
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
        по выборке rarity_sample_size). Наличие = collections.items_count (on-chain).
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

        lines = [f"📦 Выпущено: <b>{coll:,}</b> NFT".replace(",", " ")]

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


    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Основной цикл: читает Redis Streams и обрабатывает события."""
        logger.info("[dispatcher] старт")
        last_ids = {STREAM_NFT_UPGRADES: "$", STREAM_NFT_LISTINGS: "$", STREAM_NFT_SALES: "$"}

        while not stop_event.is_set():
            try:
                # XREAD с блокировкой на 2 сек
                response = await self.redis.xread(last_ids, block=2000, count=50)
                for stream_name, messages in response or []:
                    stream_name = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    for msg_id, fields in messages:
                        msg_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        last_ids[stream_name] = msg_id

                        raw = fields.get(b"data") or fields.get("data")
                        if raw is None:
                            continue
                        raw = raw.decode() if isinstance(raw, bytes) else raw
                        payload = orjson.loads(raw)

                        if stream_name == STREAM_NFT_UPGRADES:
                            await self.process_upgrade(payload)
                        elif stream_name == STREAM_NFT_LISTINGS:
                            await self.process_listing(payload)
                        elif stream_name == STREAM_NFT_SALES:
                            await self.process_sale(payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[dispatcher] итерация упала: {exc!r}", exc_info=True)
                self.stats["errors"] += 1

        logger.info(f"[dispatcher] остановлен, статистика: {self.stats}")
