# services/health_monitor.py — алерты администратору о сбоях (ТЗ, NFR-5): отставание, очередь ошибок, воркеры
"""
Раз в `HEALTH_INTERVAL` секунд проверяет состояние и пишет админам (ADMIN_TG_IDS) только при ИЗМЕНЕНИИ состояния
(сломалось / починилось), с паузой `HEALTH_REPEAT` между повторами одного и того же сбоя:

  dispatcher_lag   у группы g:dispatcher по потокам событий lag > HEALTH_MAX_LAG (диспетчер не успевает)
  dlq              в nft_dlq больше HEALTH_MAX_DLQ сообщений (события не обрабатываются)
  telethon         воркер владельцев не отвечает (tg_owner:alive пропал: сервис resolver упал или вход Telethon слетел)
  no_events        в потоках нет новых событий дольше HEALTH_STALL минут (листенеры/источники встали)
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from loguru import logger

from core.config import Settings
from core.redis_client import DISPATCHER_GROUP, STREAM_DLQ, STREAM_NFT_LISTINGS, STREAM_NFT_SALES, STREAM_NFT_UPGRADES

STREAMS = (STREAM_NFT_UPGRADES, STREAM_NFT_LISTINGS, STREAM_NFT_SALES)
ALIVE_KEY = "tg_owner:alive"


def _info(group: Any) -> dict[str, Any]:
    """XINFO GROUPS отдаёт dict (redis-py 5+) или плоский список."""
    if isinstance(group, dict):
        return {(k.decode() if isinstance(k, bytes) else k): v for k, v in group.items()}
    items = list(group)
    return {(items[i].decode() if isinstance(items[i], bytes) else items[i]): items[i + 1] for i in range(0, len(items) - 1, 2)}


class HealthMonitor:
    def __init__(self, bot: Any, redis: Any, settings: Settings) -> None:
        self.bot = bot
        self.redis = redis
        self.settings = settings
        self.state: dict[str, bool] = {}  # проблема -> сейчас активна
        self._last_sent: dict[str, float] = {}
        self._last_len: dict[str, int] = {}
        self._last_growth = time.monotonic()

    async def problems(self) -> dict[str, str]:
        """Текущие проблемы: код -> описание (пусто — всё в порядке)."""
        out: dict[str, str] = {}
        s = self.settings
        worst_lag = 0
        for stream in STREAMS:
            try:
                for g in await self.redis.xinfo_groups(stream):
                    info = _info(g)
                    name = info.get("name")
                    name = name.decode() if isinstance(name, bytes) else name
                    if name == DISPATCHER_GROUP and isinstance(info.get("lag"), int):
                        worst_lag = max(worst_lag, info["lag"])
            except Exception:  # noqa: BLE001 — потока ещё нет
                continue
        if worst_lag > s.health_max_lag:
            out["dispatcher_lag"] = f"диспетчер отстаёт: в очереди {worst_lag} событий (порог {s.health_max_lag})"
        try:
            dlq = int(await self.redis.xlen(STREAM_DLQ))
        except Exception:  # noqa: BLE001
            dlq = 0
        if dlq > s.health_max_dlq:
            out["dlq"] = f"в очереди ошибок nft_dlq {dlq} сообщений (порог {s.health_max_dlq}), см. /dlq"
        if s.owner_lookup_enabled and not isinstance(await self.redis.get(ALIVE_KEY), (bytes, str)):
            out["telethon"] = "воркер владельцев (Telethon) не отвечает: сервис resolver упал или вход в Telegram слетел"
        # поток событий: растёт ли хоть один
        total = 0
        for stream in STREAMS:
            try:
                total += int(await self.redis.xlen(stream))
            except Exception:  # noqa: BLE001
                pass
        if total != self._last_len.get("total", -1):
            self._last_growth = time.monotonic()
        self._last_len["total"] = total
        if time.monotonic() - self._last_growth > s.health_stall_minutes * 60:
            out["no_events"] = f"в потоках событий нет новых данных {s.health_stall_minutes}+ мин: источники встали"
        return out

    async def notify(self, text: str) -> None:
        for raw in self.settings.admin_tg_ids.split(","):
            raw = raw.strip()
            if raw.isdigit():
                try:
                    await self.bot.send_message(int(raw), text)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[health] не удалось написать админу {raw}: {exc!r}")

    async def check_once(self) -> None:
        now = time.monotonic()
        current = await self.problems()
        for code, text in current.items():
            fresh = not self.state.get(code)
            if fresh or now - self._last_sent.get(code, 0) > self.settings.health_repeat:
                self._last_sent[code] = now
                await self.notify(f"⚠️ {text}")
            self.state[code] = True
        for code, active in list(self.state.items()):
            if active and code not in current:
                self.state[code] = False
                await self.notify(f"✅ Восстановилось: {code}")

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self.settings.admin_tg_ids.strip():
            logger.info("[health] ADMIN_TG_IDS пуст — алерты администратору выключены")
            return
        logger.info("[health] мониторинг запущен")
        while not stop_event.is_set():
            try:
                await self.check_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[health] проверка упала: {exc!r}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.settings.health_interval)
            except asyncio.TimeoutError:
                pass
