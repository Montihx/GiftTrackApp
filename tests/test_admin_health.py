"""Админка (/ban, /stats), мониторинг (алерты администратору), /delete_me, бан в диспетчере."""
from __future__ import annotations

import datetime as dt
from typing import Any
from unittest.mock import AsyncMock

from bot.admin import build_stats
from bot.privacy import delete_user_data
from core.config import Settings
from db.models import Favorite, Filter, User
from services.dispatcher import NotificationDispatcher
from services.health_monitor import HealthMonitor


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, Any] = {}
        self.lens: dict[str, int] = {}
        self.groups: dict[str, list[Any]] = {}

    async def get(self, key: str) -> Any:
        return self.kv.get(key)

    async def xlen(self, stream: str) -> int:
        return self.lens.get(stream, 0)

    async def xinfo_groups(self, stream: str) -> list[Any]:
        if stream not in self.groups:
            raise RuntimeError("no such key")
        return self.groups[stream]


def monitor(redis: FakeRedis, bot: Any, **kw: Any) -> HealthMonitor:
    return HealthMonitor(bot, redis, Settings(admin_tg_ids="1,2", health_max_lag=100, health_max_dlq=5, **kw))


async def test_health_alerts_once_then_recovery() -> None:
    r, bot = FakeRedis(), AsyncMock()
    r.kv["tg_owner:alive"] = b"1"
    r.groups["nft_listings"] = [{"name": "g:dispatcher", "lag": 900}]
    m = monitor(r, bot)
    await m.check_once()
    texts = [c[0][1] for c in bot.send_message.call_args_list]
    assert len(texts) == 2 and all("отстаёт" in t and "900" in t for t in texts)  # оба админа, один раз

    bot.send_message.reset_mock()
    await m.check_once()
    bot.send_message.assert_not_awaited()  # тот же сбой — без спама

    r.groups["nft_listings"] = [{"name": "g:dispatcher", "lag": 3}]
    await m.check_once()
    assert all("Восстановилось" in c[0][1] for c in bot.send_message.call_args_list) and bot.send_message.await_count == 2


async def test_health_detects_dlq_and_dead_telethon_worker() -> None:
    r, bot = FakeRedis(), AsyncMock()
    r.lens["nft_dlq"] = 12  # воркер владельцев не «жив» (ключа нет)
    p = await monitor(r, bot).problems()
    assert "dlq" in p and "telethon" in p and "dispatcher_lag" not in p


async def test_health_without_admins_does_nothing() -> None:
    import asyncio

    stop = asyncio.Event()
    stop.set()
    bot = AsyncMock()
    await HealthMonitor(bot, FakeRedis(), Settings(admin_tg_ids="")).run(stop)
    bot.send_message.assert_not_awaited()


async def test_stats_text(session: Any) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    session.add_all([User(tg_id=1), User(tg_id=2, sub_plan="premium", sub_expires_at=now + dt.timedelta(days=3)),
                     User(tg_id=3, is_banned=True)])
    await session.flush()
    session.add(Filter(user_id=1, collection_id=None, is_active=True))
    await session.commit()
    r = FakeRedis()
    r.kv[f"stats:sent:{now:%Y%m%d}"] = b"7"
    r.lens["nft_dlq"] = 2
    text = await build_stats(session, r)
    assert "Пользователей: 3 (с подпиской 1, забанено 1)" in text and "Активных фильтров: 1" in text
    assert "7 алертов отправлено" in text and "nft_dlq: 2" in text


async def test_delete_me_removes_user_data(session: Any) -> None:
    session.add_all([User(tg_id=10), User(tg_id=11)])
    await session.flush()
    session.add_all([Filter(user_id=10, collection_id=None, is_active=True), Favorite(user_id=10, item_address="EQ_fav_item"),
                     Filter(user_id=11, collection_id=None, is_active=True)])
    await session.commit()
    await delete_user_data(session, 10)
    await session.commit()
    from sqlalchemy import func, select

    assert (await session.execute(select(func.count()).select_from(User).where(User.tg_id == 10))).scalar_one() == 0
    assert (await session.execute(select(func.count()).select_from(Filter).where(Filter.user_id == 10))).scalar_one() == 0
    assert (await session.execute(select(func.count()).select_from(User).where(User.tg_id == 11))).scalar_one() == 1  # чужое цело


async def test_banned_user_gets_no_alerts(session: Any) -> None:
    session.add_all([User(tg_id=21), User(tg_id=22, is_banned=True)])
    await session.flush()
    session.add_all([Filter(user_id=u, collection_id=None, is_active=True, require_owner=False) for u in (21, 22)])
    await session.commit()
    d = NotificationDispatcher(bot=AsyncMock(), session=session, redis=AsyncMock(), resolver=AsyncMock(), settings=Settings())
    users, _ = await d._match_users(None, {}, kind="sale")
    assert users == {21}
