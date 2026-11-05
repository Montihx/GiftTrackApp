# /workspace/bot/admin.py — админ-команды (выдача доступа без оплаты)
"""Команды только для ID из settings.admin_tg_ids: /grant, /revoke, /users, /add_collection."""
from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from sqlalchemy import select

from core.config import get_settings
from core.ton_address import normalize_ton_address
from db.models import Collection, User
from services.payments import activate_subscription

admin_router = Router()


def _is_admin(msg: Message) -> bool:
    return get_settings().is_admin(msg.from_user.id)


@admin_router.message(Command("grant"))
async def cmd_grant(msg: Message, cmd: CommandObject, session) -> None:
    """Выдать подписку без оплаты: /grant <tg_id> [дней]."""
    if not _is_admin(msg):
        return
    args = (cmd.args or "").split()
    if not args or not args[0].isdigit():
        await msg.answer("Формат: /grant <tg_id> [дней]")
        return
    tg_id = int(args[0])
    days = int(args[1]) if len(args) > 1 and args[1].isdigit() else get_settings().subscription_days
    expiry = await activate_subscription(session, tg_id, days)
    await msg.answer(f"✅ Подписка для {tg_id} до {expiry:%Y-%m-%d %H:%M} UTC")


@admin_router.message(Command("revoke"))
async def cmd_revoke(msg: Message, cmd: CommandObject, session) -> None:
    """Снять подписку: /revoke <tg_id>."""
    if not _is_admin(msg):
        return
    args = (cmd.args or "").split()
    if not args or not args[0].isdigit():
        await msg.answer("Формат: /revoke <tg_id>")
        return
    tg_id = int(args[0])
    user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalar_one_or_none()
    if user is None:
        await msg.answer("Пользователь не найден")
        return
    user.sub_plan = "free"
    user.sub_expires_at = None
    await session.flush()
    await msg.answer(f"🚫 Подписка снята у {tg_id}")


@admin_router.message(Command("users"))
async def cmd_users(msg: Message, session) -> None:
    """Список пользователей со статусами (первые 50)."""
    if not _is_admin(msg):
        return
    rows = (
        await session.execute(select(User.tg_id, User.username, User.sub_plan, User.sub_expires_at))
    ).all()
    lines = []
    for tg_id, username, plan, expiry in rows[:50]:
        until = f" до {expiry:%Y-%m-%d}" if expiry else ""
        lines.append(f"{tg_id} @{username or '—'} {plan}{until}")
    await msg.answer("\n".join(lines) or "Пусто")


async def add_manual_collection(session, address: str, name: str | None = None) -> tuple[str, Collection]:
    """Идемпотентно добавляет коллекцию с source='manual'.

    Возвращает ("created"|"exists", Collection). Существующую запись не перезаписываем
    (в т.ч. не меняем source у seed/getgems-коллекций).
    """
    existing = (
        await session.execute(select(Collection).where(Collection.contract_address == address))
    ).scalar_one_or_none()
    if existing is not None:
        return "exists", existing
    coll = Collection(
        name=(name or "").strip() or f"Collection {address[:6]}…{address[-4:]}",
        contract_address=address,
        source="manual",
    )
    session.add(coll)
    await session.flush()
    return "created", coll


@admin_router.message(Command("add_collection"))
async def cmd_add_collection(msg: Message, cmd: CommandObject, session) -> None:
    """Добавить коллекцию вручную: /add_collection <address> [name]."""
    if not _is_admin(msg):
        return
    parts = (cmd.args or "").split(maxsplit=1)
    if not parts:
        await msg.answer("Формат: /add_collection <address> [name]")
        return
    address = normalize_ton_address(parts[0])
    if address is None:
        await msg.answer("Некорректный адрес TON (ожидается EQ…/UQ… или 0:hex)")
        return
    status, coll = await add_manual_collection(session, address, parts[1] if len(parts) > 1 else None)
    if status == "exists":
        await msg.answer(f"ℹ️ Уже есть: {coll.name} ({coll.source})")
    else:
        await msg.answer(f"✅ Добавлена: {coll.name}")
