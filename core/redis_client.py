# /workspace/core/redis_client.py — Redis: кэш + Streams (единый доступ)
from __future__ import annotations

from typing import Any

import orjson
import redis.asyncio as aioredis
from redis.asyncio import Redis

from core.config import get_settings

# ---------- Константы ключей (см. ТЗ «REDIS КЭШ») ----------
STREAM_ALERTS_HOT = "alerts:hot"       # мгновенная доставка алертов
STREAM_NFT_UPGRADES = "nft_upgrades"   # события mint/upgrade из TON-листенера
STREAM_NFT_LISTINGS = "nft_listings"   # события листингов из маркетплейс-листенера
STREAM_DLQ = "nft_dlq"                 # события, которые не удалось обработать за DISPATCH_MAX_ATTEMPTS попыток
DISPATCHER_GROUP = "g:dispatcher"       # consumer group диспетчера (XREADGROUP + XACK)
STREAM_NFT_SALES = "nft_sales"         # события продаж (TonAPI-sync) -> уведомление «GIFT SOLD!»


def floor_key(collection_id: str, model: str | None = None) -> str:
    """floor:{collection_id} или floor:{collection_id}:{model}"""
    if model is None:
        return f"floor:{collection_id}"
    return f"floor:{collection_id}:{model}"


def recent_sales_key(collection_id: str, model: str) -> str:
    """recent_sales:{collection_id}:{model}"""
    return f"recent_sales:{collection_id}:{model}"


def owner_history_key(nft_address: str) -> str:
    """owner_history:{nft_address}"""
    return f"owner_history:{nft_address}"


def wallet_key(ton_address: str) -> str:
    """wallet:{ton_address}"""
    return f"wallet:{ton_address}"


def collection_attrs_key(collection_id: str) -> str:
    """collection_attrs:{collection_id}"""
    return f"collection_attrs:{collection_id}"


def create_redis() -> Redis:
    """Фабрика async-клиента Redis (decode_responses=False — храним orjson-байты)."""
    settings = get_settings()
    return aioredis.from_url(
        settings.redis_url,
        decode_responses=False,
        max_connections=50,
        health_check_interval=30,
    )


async def cache_set_json(redis: Redis, key: str, value: Any, ttl: int) -> None:
    """Запись JSON-значения с TTL (orjson быстрее stdlib json)."""
    await redis.set(key, orjson.dumps(value), ex=ttl)


async def cache_get_json(redis: Redis, key: str) -> Any | None:
    """Чтение JSON-значения; None при промахе."""
    raw = await redis.get(key)
    if raw is None:
        return None
    try:
        return orjson.loads(raw)
    except orjson.JSONDecodeError:
        return None


async def stream_publish(redis: Redis, stream: str, payload: dict[str, Any]) -> str:
    """Публикация события в Redis Stream. Словарь сериализуется в поле 'data' целиком."""
    entry: Any = {"data": orjson.dumps(payload).decode()}
    entry_id = await redis.xadd(stream, entry, maxlen=100_000, approximate=True)
    return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
