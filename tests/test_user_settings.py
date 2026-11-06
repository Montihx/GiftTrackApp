"""Пауза, тихие часы, /status, напоминание о подписке, порядок роутеров (админский /stats не перехватывает пользовательский)."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from core.config import Settings
from db.models import Filter, User
from services.dispatcher import NotificationDispatcher
from services.subscription_reminder import due_users, send_reminders
from services.user_settings import can_receive, describe_status, in_quiet_hours


def u(**kw: Any) -> Any:
    base = dict(is_banned=False, is_paused=False, quiet_start=None, quiet_end=None, tz_offset=0, sub_plan="free",
                sub_expires_at=None)
    return SimpleNamespace(**{**base, **kw})


def at(hour: int) -> dt.datetime:
    return dt.datetime(2026, 11, 6, hour, 30, tzinfo=dt.timezone.utc)


def test_quiet_hours_across_midnight_and_timezone() -> None:
    night = u(quiet_start=23, quiet_end=8)
    assert in_quiet_hours(night, at(23)) and in_quiet_hours(night, at(3)) and not in_quiet_hours(night, at(12))
    assert in_quiet_hours(u(quiet_start=9, quiet_end=17), at(10)) and not in_quiet_hours(u(quiet_start=9, quiet_end=17), at(20))
    # UTC+5: 20:30 UTC = 01:30 у пользователя -> тихие часы 23–8
    assert in_quiet_hours(u(quiet_start=23, quiet_end=8, tz_offset=5), at(20))
    assert not in_quiet_hours(u(), at(3)) and not in_quiet_hours(u(quiet_start=5, quiet_end=5), at(5))


def test_can_receive_and_status_text() -> None:
    assert can_receive(u(), at(12)) and not can_receive(u(is_paused=True)) and not can_receive(u(is_banned=True))
    now = at(12)
    paid = u(sub_plan="premium", sub_expires_at=now + dt.timedelta(days=5, hours=1), quiet_start=23, quiet_end=8, tz_offset=5)
    text = describe_status(paid, 3, now)
    assert "Подписка активна до 11.11.2026" in text and "Активных фильтров: 3" in text and "23:00–08:00" in text and "UTC+5" in text
    assert "Подписки нет" in describe_status(u(), 0, now)


async def test_dispatcher_skips_paused_and_quiet_users(session: Any) -> None:
    session.add_all([User(tg_id=31), User(tg_id=32, is_paused=True), User(tg_id=33, quiet_start=0, quiet_end=23, tz_offset=0)])
    await session.flush()
    session.add_all([Filter(user_id=x, collection_id=None, is_active=True, require_owner=False) for x in (31, 32, 33)])
    await session.commit()
    d = NotificationDispatcher(bot=AsyncMock(), session=session, redis=AsyncMock(), resolver=AsyncMock(), settings=Settings())
    users, _ = await d._match_users(None, {}, kind="sale")
    now_hour = dt.datetime.now(dt.timezone.utc).hour
    assert 31 in users and 32 not in users  # пауза
    assert (33 not in users) == (now_hour < 23)  # тихие часы 00–23: молчит почти весь день


async def test_reminder_once_per_expiry(session: Any) -> None:
    now = dt.datetime(2026, 11, 6, 12, 0, tzinfo=dt.timezone.utc)
    session.add_all([User(tg_id=41, sub_plan="premium", sub_expires_at=now + dt.timedelta(days=2)),
                     User(tg_id=42, sub_plan="premium", sub_expires_at=now + dt.timedelta(days=10)),
                     User(tg_id=43, sub_plan="free")])
    await session.commit()

    class R:
        def __init__(self) -> None:
            self.kv: dict[str, Any] = {}

        async def set(self, k: str, v: Any, nx: bool = False, ex: int | None = None) -> bool | None:
            if nx and k in self.kv:
                return None
            self.kv[k] = v
            return True

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def scope() -> Any:
        yield session

    assert [t for t, _ in await due_users(session, now)] == [41]
    bot, redis = AsyncMock(), R()
    assert await send_reminders(bot, scope, redis, now) == 1 and await send_reminders(bot, scope, redis, now) == 0
    assert bot.send_message.call_args[0][0] == 41 and "Подписка заканчивается 08.11.2026" in bot.send_message.call_args[0][1]


async def test_is_admin_filter_lets_other_users_fall_through(monkeypatch) -> None:
    from bot.admin import IsAdmin
    from core import config

    monkeypatch.setattr(config.get_settings(), "admin_tg_ids", "100,200")
    f = IsAdmin()
    assert await f(SimpleNamespace(from_user=SimpleNamespace(id=100))) is True
    # не админ -> False: сообщение идёт дальше, к пользовательскому /stats из handlers.py
    assert await f(SimpleNamespace(from_user=SimpleNamespace(id=5))) is False
    assert await f(SimpleNamespace(from_user=None)) is False
