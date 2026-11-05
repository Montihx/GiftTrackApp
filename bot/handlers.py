# /workspace/bot/handlers.py — Aiogram роутеры (команды бота)
"""Команды бота: /start, /filters, /stats."""
from __future__ import annotations

from aiogram import Bot, Router, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    WebAppInfo,
)
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import get_settings
from db.models import Filter, User

router = Router()


async def get_or_create_user(session: AsyncSession, msg: Message) -> User:
    """Создаёт пользователя в БД при первом обращении."""
    stmt = select(User).where(User.tg_id == msg.from_user.id)
    user = (await session.execute(stmt)).scalar_one_or_none()
    if user is None:
        user = User(
            tg_id=msg.from_user.id,
            username=msg.from_user.username,
            first_name=msg.from_user.first_name,
        )
        session.add(user)
        await session.flush()
    return user


@router.message(Command("start"))
async def cmd_start(msg: Message, session: AsyncSession) -> None:
    """Приветствие + кнопка открытия TWA для настройки фильтров."""
    await get_or_create_user(session, msg)
    settings = get_settings()

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Настроить фильтры",
                    web_app=WebAppInfo(url=settings.twa_url),
                )
            ]
        ]
    )

    await msg.answer(
        "👋 <b>Gift Track Bot</b>\n\n"
        "Отслеживаю апгрейды и листинги Telegram-подарков в реальном времени.\n\n"
        "Нажми кнопку ниже, чтобы настроить фильтры:\n"
        "• Коллекция (Scared Cat, Lucky Locket, ...)\n"
        "• Модель, фон, узор\n"
        "• Диапазон флор-цены\n\n"
        "Как только появится подходящий подарок — пришлю уведомление с кнопкой "
        "'Написать владельцу'.",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


@router.message(Command("filters"))
async def cmd_filters(msg: Message, session: AsyncSession) -> None:
    """Список активных фильтров пользователя."""
    await get_or_create_user(session, msg)

    stmt = select(Filter).where(Filter.user_id == msg.from_user.id, Filter.is_active.is_(True))
    filters = (await session.execute(stmt)).scalars().all()

    if not filters:
        await msg.answer(
            "У тебя пока нет активных фильтров.\nНажми /start и настрой через приложение."
        )
        return

    lines = ["<b>Твои активные фильтры:</b>\n"]
    for f in filters:
        lines.append(f"• Коллекция ID: {f.collection_id or 'любая'}")
        if f.model_id:
            lines.append(f"  Модель ID: {f.model_id}")
        if f.background_id:
            lines.append(f"  Фон ID: {f.background_id}")
        if f.floor_min is not None or f.floor_max is not None:
            lines.append(f"  Флор: {f.floor_min or '—'} … {f.floor_max or '—'} TON")
        lines.append("")

    await msg.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("stats"))
async def cmd_stats(msg: Message, session: AsyncSession) -> None:
    """Статистика пользователя: сколько фильтров, сколько алертов."""
    await get_or_create_user(session, msg)

    stmt = select(func.count()).select_from(Filter).where(Filter.user_id == msg.from_user.id)
    filter_count = (await session.execute(stmt)).scalar_one()

    await msg.answer(
        f"📊 <b>Твоя статистика:</b>\n\n"
        f"Активных фильтров: {filter_count}\n\n"
        f"(Счётчик отправленных алертов ведётся в dispatcher.stats на сервере.)",
        parse_mode="HTML",
    )
    
@router.message(Command("subscribe"))
async def cmd_subscribe(msg: Message, session: AsyncSession) -> None:
    """Оформить премиум: инвойс CryptoBot."""
    await get_or_create_user(session, msg)
    settings = get_settings()
    if not settings.cryptobot_token:
        await msg.answer("Платежи пока не подключены.")
        return
    from services.payments import CryptoBotClient

    client = CryptoBotClient(settings.cryptobot_token)
    invoice = await client.create_invoice(
        amount=settings.subscription_price_ton,
        payload=str(msg.from_user.id),
        description=f"GiftTrack Premium на {settings.subscription_days} дней",
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="💳 Оплатить", url=invoice["pay_url"])]]
    )
    await msg.answer(
        f"💎 Премиум на {settings.subscription_days} дней — {settings.subscription_price_ton} TON.\n"
        "Нажми кнопку и подтверди оплату в CryptoBot.",
        reply_markup=keyboard,
    )

