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
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import orjson
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import (
    STREAM_NFT_LISTINGS,
    STREAM_NFT_UPGRADES,
    cache_get_json,
    floor_key,
    owner_history_key,
    recent_sales_key,
)
from db.models import Attribute, Filter, User
from services.resolver.resolver import OwnerResolver


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
        self.stats = {"upgrades": 0, "listings": 0, "sent": 0, "errors": 0}

    async def process_upgrade(self, payload: dict[str, Any]) -> None:
        """Обработка события апгрейда (mint)."""
        nft_address = payload.get("nft_address", "")
        owner_ton = payload.get("owner_ton", "")
        metadata = payload.get("metadata") or {}
        collection_id = payload.get("collection_id")

        # Резолвим владельца
        owner = await self.resolver.resolve(owner_ton) if owner_ton else {}
        owner_link = self._owner_link(owner, owner_ton)

        # Формируем сообщение
        lines = ["🚨 <b>АПГРЕЙД</b>"]
        name = metadata.get("name") or f"NFT #{payload.get('index', '?')}"
        lines.append(f"🎭 <b>{name}</b>")
        if metadata.get("model"):
            lines.append(f"Модель: {metadata['model']}")
        if metadata.get("background"):
            lines.append(f"Фон: {metadata['background']}")
        if metadata.get("pattern"):
            lines.append(f"Узор: {metadata['pattern']}")
        lines.append("")
        lines.append(f"👤 Владелец: {owner_link}")
        lines.append(f"🔗 <a href='https://getgems.io/nft/{nft_address}'>GetGems</a>")

        text = "\n".join(lines)
        keyboard = self._owner_keyboard(owner)

        await self._send_to_subscribers(collection_id, metadata, text, keyboard)
        self.stats["upgrades"] += 1

    async def process_listing(self, payload: dict[str, Any]) -> None:
        """Обработка события листинга."""
        nft_address = payload.get("nft_address", "")
        seller_ton = payload.get("seller_ton", "")
        attributes = payload.get("attributes") or {}
        collection_id = payload.get("collection_id")
        price = payload.get("price_ton", 0.0)
        marketplace = payload.get("marketplace", "getgems")

        # Резолвим продавца
        owner = await self.resolver.resolve(seller_ton) if seller_ton else {}
        owner_link = self._owner_link(owner, seller_ton)

        # Собираем аналитику из Redis
        floor_coll = await cache_get_json(self.redis, floor_key(collection_id or ""))
        model_value = attributes.get("model")
        floor_model = (
            await cache_get_json(self.redis, floor_key(collection_id or "", model_value))
            if model_value
            else None
        )
        recent_sales = (
            await cache_get_json(self.redis, recent_sales_key(collection_id or "", model_value))
            if model_value
            else None
        )
        owner_history = await cache_get_json(self.redis, owner_history_key(nft_address))

        # Формируем сообщение
        lines = ["✅ <b>ЛИСТИНГ</b>"]
        lines.append(
            f"🎭 <a href='https://getgems.io/nft/{nft_address}'>"
            f"{payload.get('nft_index', '?')}</a> на {marketplace}"
        )
        lines.append(f"💰 <b>{price} TON</b>")
        if attributes.get("model"):
            lines.append(f"Модель: {attributes['model']}")
        if attributes.get("background"):
            lines.append(f"Фон: {attributes['background']}")
        if attributes.get("pattern"):
            lines.append(f"Узор: {attributes['pattern']}")
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
                    f"{s.get('marketplace', '?')} - {_format_time_ago(sold_dt)}"
                )

        text = "\n".join(lines)
        keyboard = self._owner_keyboard(owner)

        await self._send_to_subscribers(collection_id, attributes, text, keyboard)
        self.stats["listings"] += 1

    async def _send_to_subscribers(
        self,
        collection_id: str | None,
        attributes: dict[str, Any],
        text: str,
        keyboard: InlineKeyboardMarkup | None,
    ) -> None:
        """Находит подписчиков с подходящими фильтрами и шлёт им сообщение параллельно."""
        stmt = select(Filter.user_id, Filter.model_id, Filter.background_id).where(
            Filter.is_active.is_(True)
        )
        if collection_id:
            stmt = stmt.where(Filter.collection_id == collection_id)
        rows = (await self.session.execute(stmt)).all()

        if not rows:
            logger.debug("Нет активных подписчиков на это событие")
            return

        # Фильтруем по атрибутам (model/background)
        target_users = set()
        for user_id, model_id, background_id in rows:
            if model_id is not None:
                attr = (
                    await self.session.execute(select(Attribute.value).where(Attribute.id == model_id))
                ).scalar_one_or_none()
                if attr and attributes.get("model") != attr:
                    continue
            if background_id is not None:
                attr = (
                    await self.session.execute(
                        select(Attribute.value).where(Attribute.id == background_id)
                    )
                ).scalar_one_or_none()
                if attr and attributes.get("background") != attr:
                    continue
            target_users.add(user_id)

        if not target_users:
            return

        # Отправляем параллельно
        tasks = [
            self.bot.send_message(uid, text, parse_mode="HTML", reply_markup=keyboard)
            for uid in target_users
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for uid, res in zip(target_users, results):
            if isinstance(res, Exception):
                logger.warning(f"Не удалось отправить алерт пользователю {uid}: {res!r}")
                self.stats["errors"] += 1
            else:
                self.stats["sent"] += 1

        logger.info(f"Алерт отправлен {len(target_users)} подписчикам")

    def _owner_link(self, owner: dict[str, Any], fallback_ton: str) -> str:
        """Формирует ссылку на владельца: @username или tg://user?id= или TON-адрес."""
        if owner.get("username"):
            return f"@{owner['username']}"
        if owner.get("tg_user_id"):
            return f"<a href='tg://user?id={owner['tg_user_id']}'>пользователь</a>"
        return f"<code>{fallback_ton[:16]}…</code>"

    def _owner_keyboard(self, owner: dict[str, Any]) -> InlineKeyboardMarkup | None:
        """Кнопка 'Написать владельцу' если есть tg_user_id."""
        if owner.get("tg_user_id"):
            return InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="💬 Написать владельцу",
                            url=f"tg://user?id={owner['tg_user_id']}",
                        )
                    ]
                ]
            )
        return None

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Основной цикл: читает Redis Streams и обрабатывает события."""
        logger.info("[dispatcher] старт")
        last_ids = {STREAM_NFT_UPGRADES: "$", STREAM_NFT_LISTINGS: "$"}

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
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[dispatcher] итерация упала: {exc!r}", exc_info=True)
                self.stats["errors"] += 1

        logger.info(f"[dispatcher] остановлен, статистика: {self.stats}")
