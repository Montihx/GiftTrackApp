"""Лимиты отправки (ТЗ 8.7), сводка «ещё N», 403/429, доступ по подписке."""
from __future__ import annotations

import datetime as dt
from typing import Any
from unittest.mock import AsyncMock

from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.methods import SendMessage

from core.config import Settings
from db.models import Filter, User
from services.dispatcher import NotificationDispatcher
from services.rate_limit import SendRateLimiter


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, Any] = {}

    async def incr(self, key: str) -> int:
        self.kv[key] = int(self.kv.get(key, 0)) + 1
        return self.kv[key]

    async def expire(self, key: str, ttl: int) -> None:
        pass

    async def get(self, key: str) -> Any:
        return self.kv.get(key)

    async def set(self, key: str, value: Any, ex: int | None = None, nx: bool = False) -> bool:
        self.kv[key] = value
        return True

    async def getdel(self, key: str) -> Any:
        return self.kv.pop(key, None)


class Clock:
    def __init__(self) -> None:
        self.t = 1_000_000.0
        self.slept: list[float] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, s: float) -> None:
        self.slept.append(s)
        self.t += s


async def test_minute_limit_drops_and_counts_then_summary() -> None:
    r, c = FakeRedis(), Clock()
    lim = SendRateLimiter(r, global_per_sec=25, user_per_sec=100, user_per_min=3, clock=c.now, sleep=c.sleep)
    assert [await lim.acquire(7) for _ in range(5)] == [True, True, True, False, False]
    assert await lim.pop_over(7) == 2 and await lim.pop_over(7) == 0  # сводка один раз, потом сброс


async def test_per_second_limit_waits_for_next_second() -> None:
    r, c = FakeRedis(), Clock()
    lim = SendRateLimiter(r, user_per_sec=1, user_per_min=100, clock=c.now, sleep=c.sleep)
    assert await lim.acquire(1) and await lim.acquire(1)  # второе сообщение той же секунды ждёт
    assert len(c.slept) == 1 and 0 < c.slept[0] <= 1.01


async def test_global_limit_is_shared_between_users() -> None:
    r, c = FakeRedis(), Clock()
    lim = SendRateLimiter(r, global_per_sec=2, user_per_sec=5, user_per_min=100, clock=c.now, sleep=c.sleep)
    for uid in (1, 2, 3):
        assert await lim.acquire(uid)
    assert len(c.slept) == 1  # третий (чужой) пользователь упёрся в глобальные 2/с


async def test_limiter_fails_open_on_redis_error() -> None:
    redis = AsyncMock()
    redis.incr = AsyncMock(side_effect=RuntimeError("redis down"))
    assert await SendRateLimiter(redis).acquire(1) is True


def _dispatcher(bot: Any, redis: Any, session: Any = None, **kw: Any) -> NotificationDispatcher:
    return NotificationDispatcher(bot=bot, session=session or AsyncMock(), redis=redis, resolver=AsyncMock(),
                                  settings=Settings(**kw))


async def test_send_limited_summary_and_blocked_user() -> None:
    r = FakeRedis()
    r.kv["rl:over:5"] = 4  # накопленные сверх лимита
    bot = AsyncMock()
    d = _dispatcher(bot, r)
    await d._send_limited(5, "алерт", None, None)
    texts = [c[0][1] for c in bot.send_message.call_args_list]
    assert "Ещё 4 сигналов" in texts[0] and texts[1] == "алерт"

    bot2 = AsyncMock()
    bot2.send_message = AsyncMock(side_effect=TelegramForbiddenError(method=SendMessage(chat_id=1, text="x"), message="blocked"))
    d2 = _dispatcher(bot2, r)
    await d2._send_limited(9, "t", None, None)
    assert r.kv["rl:blocked:9"] == b"1"
    bot2.send_message.reset_mock()
    await d2._send_limited(9, "t", None, None)
    bot2.send_message.assert_not_awaited()  # заблокировавшему боту больше не пишем


async def test_send_limited_retries_once_on_retry_after(monkeypatch) -> None:
    import services.dispatcher as disp

    slept: list[float] = []

    async def fake_sleep(s: float) -> None:
        slept.append(s)

    monkeypatch.setattr(disp.asyncio, "sleep", fake_sleep)
    bot = AsyncMock()
    bot.send_message = AsyncMock(side_effect=[TelegramRetryAfter(method=SendMessage(chat_id=1, text="x"), message="flood", retry_after=3), "ok"])
    d = _dispatcher(bot, FakeRedis())
    assert await d._send_limited(2, "t", None, None) == "ok"
    assert 3 in slept and bot.send_message.await_count == 2


async def test_subscription_required_gates_alerts(session: Any) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    session.add_all([
        User(tg_id=81, sub_plan="free"),
        User(tg_id=82, sub_plan="premium", sub_expires_at=now + dt.timedelta(days=5)),
        User(tg_id=83, sub_plan="premium", sub_expires_at=now - dt.timedelta(days=1)),  # истекла
    ])
    await session.flush()
    session.add_all([Filter(user_id=u, collection_id=None, is_active=True, require_owner=False) for u in (81, 82, 83)])
    await session.commit()

    off = _dispatcher(AsyncMock(), FakeRedis(), session)
    users, _ = await off._match_users(None, {}, kind="sale")
    assert users == {81, 82, 83}  # по умолчанию (SUBSCRIPTION_REQUIRED=false) — как раньше
    on = _dispatcher(AsyncMock(), FakeRedis(), session, subscription_required=True)
    users, _ = await on._match_users(None, {}, kind="sale")
    assert users == {82}
