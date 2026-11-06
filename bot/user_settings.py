# bot/user_settings.py — /status, /pause, /resume, /quiet, /tz (отдельный роутер, bot/handlers.py не тронут)
from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from sqlalchemy import func, select

from bot.msg_utils import sender as _fu, uid as _uid
from db.models import Filter, User
from services.user_settings import describe_status

settings_router = Router()


async def _user(session, msg: Message) -> User:
    user = (await session.execute(select(User).where(User.tg_id == _uid(msg)))).scalar_one_or_none()
    if user is None:
        user = User(tg_id=_uid(msg), username=_fu(msg).username, first_name=_fu(msg).first_name)
        session.add(user)
        await session.flush()
    return user


@settings_router.message(Command("status"))
async def cmd_status(msg: Message, session) -> None:
    user = await _user(session, msg)
    count = (await session.execute(
        select(func.count()).select_from(Filter).where(Filter.user_id == user.tg_id, Filter.is_active.is_(True))
    )).scalar_one()
    await msg.answer(describe_status(user, count))


@settings_router.message(Command("pause"))
async def cmd_pause(msg: Message, session) -> None:
    (await _user(session, msg)).is_paused = True
    await msg.answer("⏸ Уведомления на паузе. Вернуть: /resume")


@settings_router.message(Command("resume"))
async def cmd_resume(msg: Message, session) -> None:
    (await _user(session, msg)).is_paused = False
    await msg.answer("🔔 Уведомления включены")


@settings_router.message(Command("tz"))
async def cmd_tz(msg: Message, cmd: CommandObject, session) -> None:
    """Часовой пояс в часах от UTC: /tz 5 (Алматы), /tz 3 (Москва)."""
    raw = (cmd.args or "").strip().lstrip("+")
    if not raw.lstrip("-").isdigit() or not -12 <= int(raw) <= 14:
        await msg.answer("Формат: /tz <часы от UTC>, например /tz 5")
        return
    (await _user(session, msg)).tz_offset = int(raw)
    await msg.answer(f"🕒 Часовой пояс: UTC{int(raw):+d}")


@settings_router.message(Command("quiet"))
async def cmd_quiet(msg: Message, cmd: CommandObject, session) -> None:
    """Тихие часы: /quiet 23 8 (с 23:00 до 8:00 по вашему часовому поясу), /quiet off."""
    user = await _user(session, msg)
    args = (cmd.args or "").split()
    if args == ["off"]:
        user.quiet_start = user.quiet_end = None
        await msg.answer("🌙 Тихие часы выключены")
        return
    if len(args) == 2 and all(a.isdigit() and 0 <= int(a) <= 23 for a in args) and args[0] != args[1]:
        user.quiet_start, user.quiet_end = int(args[0]), int(args[1])
        await msg.answer(f"🌙 Тихие часы {int(args[0]):02d}:00–{int(args[1]):02d}:00 (UTC{user.tz_offset:+d}). Часовой пояс: /tz")
        return
    await msg.answer("Формат: /quiet <с> <до> (часы 0–23), например /quiet 23 8; выключить: /quiet off")
