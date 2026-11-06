# /workspace/services/resolver/resolver.py — резолвинг TON-кошелька в Telegram-аккаунт (из уже известного)
"""
OwnerResolver: находит Telegram-аккаунт по адресу кошелька среди УЖЕ известных связок.

  1. Redis-кэш wallet:{ton} (TTL 1 ч);
  2. БД wallet_tg_map — связки, найденные сканером bio (services/resolver/telethon_worker.py: в bio пользователя указан кошелёк);
  3. иначе адрес ставится в очередь telethon_resolve_queue (фоновый скан), ответ source="pending".

Внешних API нет: Tonviewer недоступен, а GetGems платный. Контакт владельца подарка даёт Telegram по slug
(services/owner_lookup.py, services/telegram_upgrades.py), кошелёк нужен только как запасной путь.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import cache_get_json, cache_set_json, wallet_key
from db.models import WalletTgMap


class OwnerResolver:
    """DI: session (AsyncSession), redis (Redis client), settings (Settings)."""

    def __init__(self, session: AsyncSession, redis: Any, settings: Settings | None = None) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings or get_settings()
        self.stats = {"cache": 0, "db": 0, "pending": 0}

    async def resolve(self, ton_address: str) -> dict[str, Any]:
        """{tg_user_id, username, source}: source = cache | db | pending."""
        cached = await cache_get_json(self.redis, wallet_key(ton_address))
        if cached:
            self.stats["cache"] += 1
            return cached

        row = (await self.session.execute(
            select(WalletTgMap).where(WalletTgMap.ton_address == ton_address)
        )).scalar_one_or_none()
        if row is not None and (row.username or row.tg_user_id):
            result = {"tg_user_id": row.tg_user_id, "username": row.username, "source": "db"}
            await cache_set_json(self.redis, wallet_key(ton_address), result, ttl=3600)
            self.stats["db"] += 1
            return result

        await self.redis.lpush("telethon_resolve_queue", ton_address)
        self.stats["pending"] += 1
        return {"tg_user_id": None, "username": None, "source": "pending"}
