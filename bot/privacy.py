# bot/privacy.py — приватность: /privacy и /delete_me (удаление данных пользователя)
"""Команды для всех пользователей. Отдельный роутер, чтобы не трогать bot/handlers.py."""
from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy import delete

from bot.msg_utils import sender as _fu, uid as _uid
from db.models import Favorite, Filter, User

privacy_router = Router()

PRIVACY_TEXT = (
    "🔒 Конфиденциальность\n"
    "Бот хранит только ваш Telegram ID, username, фильтры, избранное и статус подписки — чтобы присылать "
    "уведомления. Кошельки и платёжные данные не хранятся; оплата проходит через CryptoBot.\n"
    "Удалить свои данные: /delete_me\n\n"
    "Сигналы носят информационный характер и не являются финансовой рекомендацией. Оценки приблизительны."
)


@privacy_router.message(Command("privacy"))
async def cmd_privacy(msg: Message) -> None:
    await msg.answer(PRIVACY_TEXT)


async def delete_user_data(session, tg_id: int) -> None:
    """Фильтры, избранное и запись пользователя (подписка пропадает вместе с ней)."""
    await session.execute(delete(Filter).where(Filter.user_id == tg_id))
    await session.execute(delete(Favorite).where(Favorite.user_id == tg_id))
    await session.execute(delete(User).where(User.tg_id == tg_id))


@privacy_router.message(Command("delete_me"))
async def cmd_delete_me(msg: Message, session) -> None:
    await delete_user_data(session, _uid(msg))
    await session.flush()
    await msg.answer("🗑 Ваши данные удалены: фильтры, избранное и подписка. Чтобы начать заново, нажмите /start.")
