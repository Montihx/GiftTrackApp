# services/rate_limit.py — лимиты отправки Telegram (ТЗ 8.7): глобально, на пользователя в секунду и в минуту
"""
Счётчики в Redis с окнами по секундам/минутам (INCR + EXPIRE), работают и при нескольких процессах диспетчера.

  глобально        rl:gs:{сек}        не более `global_per_sec` (25: запас под лимит Telegram ~30/с)
  пользователь     rl:us:{uid}:{сек}  не более `user_per_sec` (1)
                   rl:um:{uid}:{мин}  не более `user_per_min` (20)

Секундные лимиты не отбрасывают сообщение, а ждут следующей секунды. Минутный — отбрасывает: лишние
сигналы считаются (`rl:over:{uid}`), и перед следующим разрешённым сообщением пользователю уходит одна
сводка «ещё N сигналов» (антифлуд, ТЗ 8.7). Любая ошибка Redis — fail-open: алерт важнее лимита.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger


class SendRateLimiter:
    def __init__(
        self,
        redis: Any,
        global_per_sec: int = 25,
        user_per_sec: int = 1,
        user_per_min: int = 20,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.redis = redis
        self.global_per_sec = global_per_sec
        self.user_per_sec = user_per_sec
        self.user_per_min = user_per_min
        self._clock = clock
        self._sleep = sleep

    async def _hit(self, key: str, ttl: int) -> int:
        n = await self.redis.incr(key)
        if n == 1:
            await self.redis.expire(key, ttl)
        return int(n)

    async def _wait_slot(self, prefix: str, limit: int) -> None:
        """Ждёт, пока в текущей секунде найдётся свободный слот (не дольше нескольких секунд)."""
        for _ in range(5):
            now = self._clock()
            if await self._hit(f"{prefix}:{int(now)}", 2) <= limit:
                return
            await self._sleep(int(now) + 1 - now + 0.01)

    async def acquire(self, uid: int) -> bool:
        """True — отправлять; False — минутный лимит пользователя исчерпан (сигнал учтён для сводки)."""
        try:
            minute = int(self._clock() // 60)
            if await self._hit(f"rl:um:{uid}:{minute}", 120) > self.user_per_min:
                await self._hit(f"rl:over:{uid}", 3600)
                return False
            await self._wait_slot(f"rl:us:{uid}", self.user_per_sec)
            await self._wait_slot("rl:gs", self.global_per_sec)
        except Exception as exc:  # noqa: BLE001 — fail-open
            logger.debug(f"[rate_limit] {exc!r} — пропускаю проверку")
        return True

    async def pop_over(self, uid: int) -> int:
        """Сколько сигналов пользователю не показано из-за минутного лимита (и сброс счётчика)."""
        try:
            raw = await self.redis.getdel(f"rl:over:{uid}")
            return int(raw) if isinstance(raw, (bytes, str, int)) and int(raw) > 0 else 0
        except Exception:  # noqa: BLE001
            return 0
