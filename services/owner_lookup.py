# services/owner_lookup.py — владелец подарка (Telegram-аккаунт) по slug через Telethon-аккаунт сервиса resolver
"""
Для листингов из блокчейна и апгрейдов адрес кошелька почти никогда не приводит к Telegram-аккаунту
(Tonviewer/GetGems-источники недоступны). Зато Telegram по slug подарка (t.me/nft/PlushPepe-727) отдаёт
владельца, если он не скрыт: payments.getUniqueStarGift(slug).

Работа через Redis (dispatcher и resolver — разные контейнеры):
  dispatcher: request_owner(redis, slug) — кэш tg_owner:{slug}; промах -> LPUSH tg_owner:queue и ожидание ответа
              до таймаута; если воркер не отвечает (нет входа Telethon) — сразу None, без ожидания;
  resolver:   OwnerLookupWorker — BRPOP очереди, getUniqueStarGift, запись в кэш (1 ч; «владелец скрыт» — 5 мин).
"""
from __future__ import annotations

import asyncio
import re
from typing import Any

import orjson
from loguru import logger

QUEUE_KEY = "tg_owner:queue"
CACHE_KEY = "tg_owner:{slug}"
ALIVE_KEY = "tg_owner:alive"
HIT_TTL = 3600
MISS_TTL = 300


def gift_slug(nft_name: str | None) -> str | None:
    """'Plush Pepe #727' -> 'PlushPepe-727'; 'Durov’s Cap #1' -> 'DurovsCap-1'."""
    m = re.match(r"^(.*?)\s*#\s*(\d+)\s*$", nft_name or "")
    if not m:
        return None
    title = re.sub(r"[^A-Za-z0-9]", "", m.group(1))
    return f"{title}-{m.group(2)}" if title else None


def _loads(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, (bytes, str)):
        return None
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


async def request_owner(
    redis: Any, slug: str, timeout: float = 4.0, poll: float = 0.4, max_queue: int = 0
) -> dict[str, Any] | None:
    """Владелец {"tg_user_id", "username", "name"} или None (не найден / скрыт / воркер недоступен).

    max_queue > 0 — защита диспетчера: воркер отвечает ~1 запрос/с, и если очередь уже длиннее max_queue, событие
    не ждёт ответа (иначе при потоке в сотни листингов в минуту диспетчер отстаёт и копит lag).
    """
    key = CACHE_KEY.format(slug=slug)
    cached = _loads(await redis.get(key))
    if cached is not None:
        return cached or None  # {} — известный промах
    if not isinstance(await redis.get(ALIVE_KEY), (bytes, str)):
        return None  # воркер не запущен (нет входа Telethon): не заставляем уведомление ждать
    if max_queue > 0:
        try:
            qlen = await redis.llen(QUEUE_KEY)
            if isinstance(qlen, int) and qlen >= max_queue:
                return None
        except Exception:  # noqa: BLE001
            pass
    await redis.lpush(QUEUE_KEY, slug)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        await asyncio.sleep(poll)
        cached = _loads(await redis.get(key))
        if cached is not None:
            return cached or None
    return None


def _amount(raw: int | None, currency: str) -> float | None:
    """Сумма из минимальных единиц валюты: TON — нано, Stars (XTR) — как есть, фиат — центы."""
    if raw is None:
        return None
    if currency == "TON":
        return raw / 1e9
    if currency == "XTR":
        return float(raw)
    return raw / 100


def value_from_info(info: Any) -> dict[str, Any] | None:
    currency = str(getattr(info, "currency", "") or "").upper()
    value = _amount(getattr(info, "value", None), currency)
    if value is None:
        return None
    return {
        "currency": currency, "value": value,
        "floor": _amount(getattr(info, "floor_price", None), currency),
        "average": _amount(getattr(info, "average_price", None), currency),
        "last_sale": _amount(getattr(info, "last_sale_price", None), currency),
        "listed": getattr(info, "listed_count", None),
    }


class OwnerLookupWorker:
    def __init__(self, client: Any, redis: Any, interval: float = 1.0) -> None:
        self.client = client
        self.redis = redis
        self.interval = interval
        self.stats = {"lookups": 0, "found": 0, "hidden": 0, "flood_waits": 0}

    async def lookup(self, slug: str) -> dict[str, Any]:
        from telethon import errors
        from telethon.tl import functions

        from services.telegram_market import parse_resale_gift

        try:
            res = await self.client(functions.payments.GetUniqueStarGiftRequest(slug=slug))
        except errors.FloodWaitError as exc:
            self.stats["flood_waits"] += 1
            logger.warning(f"[owner_lookup] FloodWait {exc.seconds}s")
            await asyncio.sleep(exc.seconds + 1)
            raise
        users = {u.id: u for u in getattr(res, "users", None) or []}
        chats = {c.id: c for c in getattr(res, "chats", None) or []}
        item = parse_resale_gift(res.gift, users, chats)
        out: dict[str, Any] = {}
        if item is not None and (item["seller_tg_id"] or item["seller_username"]):
            out = {"tg_user_id": item["seller_tg_id"], "username": item["seller_username"], "name": item["seller_name"]}
        value = await self._value(slug)
        if value:
            out["value"] = value
        return out

    async def _value(self, slug: str) -> dict[str, Any] | None:
        """Оценка подарка (то, что see.tg показывает как «Оценка»): payments.getUniqueStarGiftValueInfo."""
        from telethon import errors
        from telethon.tl import functions

        try:
            info = await self.client(functions.payments.GetUniqueStarGiftValueInfoRequest(slug=slug))
        except errors.FloodWaitError:
            raise
        except Exception as exc:  # noqa: BLE001 — оценки может не быть: уведомление уйдёт без неё
            logger.debug(f"[owner_lookup] оценка {slug}: {exc!r}")
            return None
        return value_from_info(info)

    async def handle(self, slug: str) -> dict[str, Any]:
        key = CACHE_KEY.format(slug=slug)
        cached = _loads(await self.redis.get(key))
        if cached is not None:
            return cached
        try:
            owner = await self.lookup(slug)
        except Exception as exc:  # noqa: BLE001 — slug не найден/сеть: считаем «скрыт», чтобы не долбить повторно
            logger.debug(f"[owner_lookup] {slug}: {exc!r}")
            owner = {}
        self.stats["lookups"] += 1
        self.stats["found" if (owner.get("username") or owner.get("tg_user_id")) else "hidden"] += 1
        await self.redis.set(key, orjson.dumps(owner), ex=HIT_TTL if owner else MISS_TTL)
        return owner

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("[owner_lookup] старт: владельцы подарков по slug")
        while not stop_event.is_set():
            await self.redis.set(ALIVE_KEY, b"1", ex=30)
            try:
                # без блокирующего BRPOP: он на общем пуле Redis давал network:TimeoutError — опрашиваем очередь сами
                raw = await self.redis.rpop(QUEUE_KEY)
                item = (QUEUE_KEY, raw) if raw is not None else None
                if item is None:
                    await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[owner_lookup] redis: {exc!r}")
                await asyncio.sleep(2)
                continue
            if item is None:
                continue
            slug = item[1].decode() if isinstance(item[1], bytes) else item[1]
            await self.handle(slug)
            await asyncio.sleep(self.interval)
        logger.info(f"[owner_lookup] остановлен: {self.stats}")
