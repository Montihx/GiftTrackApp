# /workspace/bot/admin.py — админ-команды (выдача доступа без оплаты)
"""Команды только для ID из settings.admin_tg_ids: /grant, /revoke, /users, /add_collection,
/ban, /unban, /stats, /dlq, /dlq_clear, /broadcast (+ /broadcast_confirm)."""
from __future__ import annotations

import asyncio
import datetime as dt

from aiogram import Router
from aiogram.filters import BaseFilter, Command, CommandObject
from aiogram.types import Message
from sqlalchemy import func, select

from bot.msg_utils import uid as _uid
from core.config import get_settings
from core.ton_address import normalize_ton_address
from core.redis_client import STREAM_DLQ, create_redis
from db.models import Collection, Filter, User
from services.payments import activate_subscription

admin_router = Router()


class IsAdmin(BaseFilter):
    """Фильтр роутера: не админ — сообщение идёт дальше к обычным командам (например, /stats пользователя)."""

    async def __call__(self, msg: Message) -> bool:
        return msg.from_user is not None and get_settings().is_admin(msg.from_user.id)


admin_router.message.filter(IsAdmin())


def _is_admin(msg: Message) -> bool:
    return get_settings().is_admin(_uid(msg))


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


# ---------- бан, статистика, очередь ошибок, рассылка ----------
def _tg_id_arg(cmd: CommandObject) -> int | None:
    args = (cmd.args or "").split()
    return int(args[0]) if args and args[0].isdigit() else None


@admin_router.message(Command("ban"))
async def cmd_ban(msg: Message, cmd: CommandObject, session) -> None:
    """Забанить: уведомления не приходят, подписка не снимается: /ban <tg_id>."""
    if not _is_admin(msg):
        return
    tg_id = _tg_id_arg(cmd)
    if tg_id is None:
        await msg.answer("Формат: /ban <tg_id>")
        return
    user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalar_one_or_none()
    if user is None:
        await msg.answer("Пользователь не найден")
        return
    user.is_banned = True
    await session.flush()
    await msg.answer(f"⛔ {tg_id} забанен: алерты ему больше не отправляются")


@admin_router.message(Command("unban"))
async def cmd_unban(msg: Message, cmd: CommandObject, session) -> None:
    if not _is_admin(msg):
        return
    tg_id = _tg_id_arg(cmd)
    user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalar_one_or_none() if tg_id else None
    if user is None:
        await msg.answer("Формат: /unban <tg_id> (пользователь должен существовать)")
        return
    user.is_banned = False
    await session.flush()
    await msg.answer(f"✅ {tg_id} разбанен")


async def build_stats(session, redis) -> str:
    """Текст /stats: пользователи, подписки, фильтры, события и алерты за сегодня и вчера, очередь ошибок."""
    now = dt.datetime.now(dt.timezone.utc)
    users = (await session.execute(select(func.count()).select_from(User))).scalar_one()
    paid = (await session.execute(
        select(func.count()).select_from(User).where(User.sub_plan != "free", User.sub_expires_at > now)
    )).scalar_one()
    banned = (await session.execute(select(func.count()).select_from(User).where(User.is_banned.is_(True)))).scalar_one()
    filters = (await session.execute(select(func.count()).select_from(Filter).where(Filter.is_active.is_(True)))).scalar_one()
    lines = [f"👥 Пользователей: {users} (с подпиской {paid}, забанено {banned})", f"🎯 Активных фильтров: {filters}"]
    for label, day in (("сегодня", now), ("вчера", now - dt.timedelta(days=1))):
        vals = []
        for name, title in (("listings", "листингов"), ("sales", "продаж"), ("upgrades", "апгрейдов"), ("sent", "алертов отправлено")):
            raw = await redis.get(f"stats:{name}:{day:%Y%m%d}")
            vals.append(f"{int(raw) if raw else 0} {title}")
        lines.append(f"📊 {label}: " + ", ".join(vals))
    try:
        lines.append(f"🧯 Очередь ошибок nft_dlq: {int(await redis.xlen(STREAM_DLQ))}")
    except Exception:  # noqa: BLE001
        pass
    return "\n".join(lines)


@admin_router.message(Command("stats"))
async def cmd_stats(msg: Message, session) -> None:
    if not _is_admin(msg):
        return
    redis = create_redis()
    try:
        await msg.answer(await build_stats(session, redis))
    finally:
        await redis.aclose()


@admin_router.message(Command("dlq"))
async def cmd_dlq(msg: Message, cmd: CommandObject) -> None:
    """Последние события из nft_dlq: /dlq [сколько, по умолчанию 5]."""
    if not _is_admin(msg):
        return
    n = min(int((cmd.args or "5").split()[0]) if (cmd.args or "5").split()[0].isdigit() else 5, 20)
    redis = create_redis()
    try:
        rows = await redis.xrevrange(STREAM_DLQ, count=n)
        if not rows:
            await msg.answer("🧯 Очередь ошибок пуста")
            return
        lines = []
        for _id, fields in rows or []:
            f = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in (fields or {}).items()}
            lines.append(f"{f.get('stream')} {f.get('id')}: {f.get('reason', '')[:200]}")
        await msg.answer("\n".join(lines))
    finally:
        await redis.aclose()


@admin_router.message(Command("dlq_clear"))
async def cmd_dlq_clear(msg: Message) -> None:
    if not _is_admin(msg):
        return
    redis = create_redis()
    try:
        await redis.delete(STREAM_DLQ)
    finally:
        await redis.aclose()
    await msg.answer("🧯 Очередь ошибок очищена")


BROADCAST_KEY = "admin:broadcast:{admin}"


@admin_router.message(Command("broadcast"))
async def cmd_broadcast(msg: Message, cmd: CommandObject, session) -> None:
    """Рассылка в два шага: /broadcast <текст> -> проверка -> /broadcast_confirm."""
    if not _is_admin(msg):
        return
    text = (cmd.args or "").strip()
    if not text:
        await msg.answer("Формат: /broadcast <текст> (потом /broadcast_confirm)")
        return
    count = (await session.execute(select(func.count()).select_from(User).where(User.is_banned.is_(False)))).scalar_one()
    redis = create_redis()
    try:
        await redis.set(BROADCAST_KEY.format(admin=_uid(msg)), text.encode(), ex=600)
    finally:
        await redis.aclose()
    await msg.answer(f"Отправить {count} пользователям?\n\n{text}\n\nПодтвердите: /broadcast_confirm (10 минут)")


@admin_router.message(Command("broadcast_confirm"))
async def cmd_broadcast_confirm(msg: Message, session) -> None:
    if not _is_admin(msg):
        return
    redis = create_redis()
    try:
        raw = await redis.get(BROADCAST_KEY.format(admin=_uid(msg)))
        await redis.delete(BROADCAST_KEY.format(admin=_uid(msg)))
    finally:
        await redis.aclose()
    if not raw:
        await msg.answer("Нет текста для рассылки (или прошло 10 минут): сначала /broadcast <текст>")
        return
    text = raw.decode() if isinstance(raw, bytes) else str(raw)
    ids = [r[0] for r in (await session.execute(select(User.tg_id).where(User.is_banned.is_(False)))).all()]
    sent = failed = 0
    for tg_id in ids:
        try:
            await msg.bot.send_message(tg_id, text)  # type: ignore[union-attr]
            sent += 1
        except Exception:  # noqa: BLE001 — заблокировали бота и т. п.
            failed += 1
        await asyncio.sleep(0.05)  # ~20 сообщений/с, ниже лимита Telegram
    await msg.answer(f"📣 Рассылка: отправлено {sent}, не доставлено {failed}")
