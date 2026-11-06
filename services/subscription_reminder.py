# services/subscription_reminder.py — напоминание о конце подписки за 3 дня (ТЗ 13)
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

from loguru import logger
from sqlalchemy import select

from db.models import User

REMIND_DAYS = 3


async def due_users(session: Any, now: dt.datetime | None = None) -> list[tuple[int, dt.datetime]]:
    """Пользователи с платной подпиской, которая закончится в ближайшие REMIND_DAYS дней."""
    now = now or dt.datetime.now(dt.timezone.utc)
    rows = (await session.execute(
        select(User.tg_id, User.sub_expires_at).where(
            User.sub_plan != "free", User.sub_expires_at.is_not(None), User.is_banned.is_(False)
        )
    )).all()
    out = []
    for tg_id, exp in rows:
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=dt.timezone.utc)
        if now < exp <= now + dt.timedelta(days=REMIND_DAYS):
            out.append((tg_id, exp))
    return out


async def send_reminders(bot: Any, session_scope: Any, redis: Any, now: dt.datetime | None = None) -> int:
    """Одно напоминание на пользователя и срок подписки (после продления срок новый — напомним снова)."""
    now = now or dt.datetime.now(dt.timezone.utc)
    sent = 0
    async with session_scope() as session:
        due = await due_users(session, now)
    for tg_id, exp in due:
        if not await redis.set(f"rem3:{tg_id}:{int(exp.timestamp())}", b"1", nx=True, ex=5 * 86400):
            continue
        days = max((exp - now).days, 0)
        try:
            await bot.send_message(
                tg_id, f"⏰ Подписка заканчивается {exp:%d.%m.%Y} (через {days} дн.). "
                       "Продлите в приложении, вкладка «Подписка» — иначе уведомления прекратятся (фильтры сохранятся).")
            sent += 1
        except Exception as exc:  # noqa: BLE001 — заблокировал бота и т. п.
            logger.debug(f"[reminder] {tg_id}: {exc!r}")
    return sent


async def run(bot: Any, session_scope: Any, redis: Any, stop_event: asyncio.Event, interval: float = 3600) -> None:
    logger.info("[reminder] напоминания о подписке запущены")
    while not stop_event.is_set():
        try:
            n = await send_reminders(bot, session_scope, redis)
            if n:
                logger.info(f"[reminder] отправлено напоминаний: {n}")
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[reminder] {exc!r}")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
