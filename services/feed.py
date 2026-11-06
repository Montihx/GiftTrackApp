# services/feed.py — лента сигналов пользователя для мини-приложения (последние алерты, ТЗ 12.2 /api/feed)
"""Карточка пишется при успешной отправке алерта: Redis list `feed:{uid}` (последние FEED_MAX, хранится 7 дней)."""
from __future__ import annotations

from typing import Any

import orjson

FEED_KEY = "feed:{uid}"
FEED_MAX = 200
FEED_TTL = 7 * 86400
KINDS = ("upgrade", "listing", "sale")


async def push_card(redis: Any, uid: int, card: dict[str, Any]) -> None:
    key = FEED_KEY.format(uid=uid)
    await redis.lpush(key, orjson.dumps(card))
    await redis.ltrim(key, 0, FEED_MAX - 1)
    await redis.expire(key, FEED_TTL)


async def read_cards(redis: Any, uid: int, kind: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """Свежие карточки (новые первыми); kind фильтрует по типу сигнала."""
    raw = await redis.lrange(FEED_KEY.format(uid=uid), 0, FEED_MAX - 1)
    out: list[dict[str, Any]] = []
    for r in raw or []:
        try:
            card = orjson.loads(r)
        except orjson.JSONDecodeError:
            continue
        if kind in KINDS and card.get("kind") != kind:
            continue
        out.append(card)
        if len(out) >= limit:
            break
    return out
