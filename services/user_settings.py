# services/user_settings.py — пауза уведомлений, тихие часы, статус подписки (чистая логика, без aiogram)
from __future__ import annotations

import datetime as dt
from typing import Any


def in_quiet_hours(user: Any, now: dt.datetime | None = None) -> bool:
    """Сейчас у пользователя тихие часы? quiet_start..quiet_end по его часовому поясу (tz_offset часов от UTC);
    интервал может переходить через полночь (23 -> 8)."""
    start, end = getattr(user, "quiet_start", None), getattr(user, "quiet_end", None)
    if start is None or end is None or start == end:
        return False
    now = now or dt.datetime.now(dt.timezone.utc)
    hour = (now + dt.timedelta(hours=int(getattr(user, "tz_offset", 0) or 0))).hour
    return start <= hour < end if start < end else (hour >= start or hour < end)


def can_receive(user: Any, now: dt.datetime | None = None) -> bool:
    """Алерты можно слать: не забанен, не на паузе, не в тихих часах."""
    return not (user.is_banned or user.is_paused or in_quiet_hours(user, now))


def describe_status(user: Any, filters_count: int, now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.now(dt.timezone.utc)
    exp = user.sub_expires_at
    if exp is not None and exp.tzinfo is None:
        exp = exp.replace(tzinfo=dt.timezone.utc)
    if user.sub_plan != "free" and exp and exp > now:
        sub = f"✅ Подписка активна до {exp:%d.%m.%Y} ({(exp - now).days} дн.)"
    else:
        sub = "❌ Подписки нет — оформите в приложении (вкладка «Подписка»)"
    state = "⏸ на паузе (/resume)" if user.is_paused else "🔔 включены"
    if in_quiet_hours(user, now):
        state += ", сейчас тихие часы"
    quiet = f"{user.quiet_start:02d}:00–{user.quiet_end:02d}:00" if user.quiet_start is not None and user.quiet_end is not None else "не заданы"
    return (f"{sub}\n🎯 Активных фильтров: {filters_count}\n🔔 Уведомления: {state}\n"
            f"🌙 Тихие часы: {quiet} (UTC{user.tz_offset:+d}; поменять: /quiet, /tz)")
