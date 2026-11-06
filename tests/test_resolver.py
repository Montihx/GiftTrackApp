# tests/test_resolver.py — OwnerResolver: кэш -> БД (связки из bio) -> очередь Telethon
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from db.models import WalletTgMap
from services.resolver.resolver import OwnerResolver


def _redis(cached: bytes | None = None) -> AsyncMock:
    r = AsyncMock()
    r.get = AsyncMock(return_value=cached)
    return r


@pytest.mark.asyncio
async def test_resolve_cache_hit(session):
    redis = _redis(b'{"tg_user_id": 123, "username": "wimp", "source": "cache"}')
    resolver = OwnerResolver(session=session, redis=redis)
    result = await resolver.resolve("EQwallet123")
    assert result["username"] == "wimp" and result["tg_user_id"] == 123
    assert resolver.stats["cache"] == 1 and not redis.lpush.called


@pytest.mark.asyncio
async def test_resolve_from_db_map_then_cached(session):
    """Связка из wallet_tg_map (её находит сканер bio) возвращается и кладётся в кэш."""
    session.add(WalletTgMap(ton_address="EQwallet_db", tg_user_id=55, username="ndryl", source="telethon", confidence=80))
    await session.commit()
    redis = _redis()
    resolver = OwnerResolver(session=session, redis=redis)
    result = await resolver.resolve("EQwallet_db")
    assert result == {"tg_user_id": 55, "username": "ndryl", "source": "db"}
    assert resolver.stats["db"] == 1 and redis.set.called and not redis.lpush.called


@pytest.mark.asyncio
async def test_resolve_pending_goes_to_telethon_queue(session):
    redis = _redis()
    resolver = OwnerResolver(session=session, redis=redis)
    result = await resolver.resolve("EQunknown")
    assert result == {"tg_user_id": None, "username": None, "source": "pending"}
    redis.lpush.assert_awaited_with("telethon_resolve_queue", "EQunknown")
    assert resolver.stats["pending"] == 1


@pytest.mark.asyncio
async def test_with_retry_does_not_retry_permanent_errors():
    from core.retry import PermanentError, with_retry

    calls = 0

    async def boom():
        nonlocal calls
        calls += 1
        raise PermanentError("HTTP 404")

    with pytest.raises(PermanentError):
        await with_retry(boom, context="t")
    assert calls == 1
