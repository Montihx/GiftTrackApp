# /workspace/tests/test_resolver.py — тесты Owner Resolver
"""Unit-тесты для services/resolver/resolver.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from core.config import Settings
from core.retry import PermanentError
from services.resolver.resolver import OwnerResolver


@pytest.mark.asyncio
async def test_resolve_cache_hit(session):
    """Резолвинг из Redis кэша (шаг 1)."""
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(
        return_value=b'{"tg_user_id": 123, "username": "wimp", "source": "cache"}'
    )

    resolver = OwnerResolver(session=session, redis=redis_mock, settings=Settings(tonviewer_enabled=True))
    result = await resolver.resolve("EQwallet123")

    assert result["username"] == "wimp"
    assert result["source"] == "cache"
    assert resolver.stats["cache"] == 1


@pytest.mark.asyncio
async def test_resolve_ton_dns(session):
    """Резолвинг через TON DNS (шаг 2)."""
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.lpush = AsyncMock()

    resolver = OwnerResolver(session=session, redis=redis_mock, settings=Settings(tonviewer_enabled=True))

    with patch("services.resolver.resolver._http_request") as mock_http:
        mock_http.return_value = b'{"domains": ["wimp.ton"]}'
        result = await resolver.resolve("EQwallet123")

    assert result["username"] == "wimp"
    assert result["source"] == "ton_dns"
    assert resolver.stats["ton_dns"] == 1


@pytest.mark.asyncio
async def test_resolve_getgems(session):
    """Резолвинг через GetGems профиль (шаг 3)."""
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.lpush = AsyncMock()

    resolver = OwnerResolver(session=session, redis=redis_mock, settings=Settings(tonviewer_enabled=True))

    with patch("services.resolver.resolver._http_request") as mock_http:
        # Первый вызов (TON DNS) — пусто, второй (GetGems) — username
        mock_http.side_effect = [
            b'{"domains": []}',
            b'{"data": {"account": {"socialLinks": [{"type": "telegram", "url": "https://t.me/sadness"}]}}}',
        ]
        result = await resolver.resolve("EQwallet123")

    assert result["username"] == "sadness"
    assert result["source"] == "getgems"
    assert resolver.stats["getgems"] == 1


@pytest.mark.asyncio
async def test_resolve_pending(session):
    """Fallback: адрес ставится в очередь Telethon (шаг 5)."""
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.lpush = AsyncMock()

    resolver = OwnerResolver(session=session, redis=redis_mock, settings=Settings(tonviewer_enabled=True))

    with patch("services.resolver.resolver._http_request") as mock_http:
        mock_http.return_value = b'{"domains": []}'
        result = await resolver.resolve("EQwallet123")

    assert result["source"] == "pending"
    assert redis_mock.lpush.called
    assert resolver.stats["pending"] == 1


@pytest.mark.asyncio
async def test_resolve_saves_to_db(session):
    """Результат резолвинга сохраняется в БД wallet_tg_map."""
    from sqlalchemy import select

    from db.models import WalletTgMap

    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.lpush = AsyncMock()

    resolver = OwnerResolver(session=session, redis=redis_mock, settings=Settings(tonviewer_enabled=True))

    with patch("services.resolver.resolver._http_request") as mock_http:
        mock_http.return_value = b'{"domains": ["ndryl.ton"]}'
        await resolver.resolve("EQwallet_db")

    await session.commit()

    stmt = select(WalletTgMap).where(WalletTgMap.ton_address == "EQwallet_db")
    row = (await session.execute(stmt)).scalar_one()
    assert row.username == "ndryl"
    assert row.source == "ton_dns"


@pytest.mark.asyncio
async def test_dead_tonviewer_steps_are_skipped_without_requests(session):
    """Tonviewer выключен (API мёртв): шаги DNS/tx не делают запросов и не ретраятся — алерт не задерживается."""
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.lpush = AsyncMock()
    resolver = OwnerResolver(session=session, redis=redis_mock, settings=Settings(tonviewer_enabled=False))

    with patch("services.resolver.resolver._http_request") as mock_http:
        mock_http.side_effect = PermanentError("HTTP 401 getgems")  # платный GetGems без ключа
        first = await resolver.resolve("EQwalletA")
        second = await resolver.resolve("EQwalletB")  # шаг GetGems уже на паузе — второго запроса нет

    assert first["source"] == "pending" and second["source"] == "pending"
    assert mock_http.call_count == 1  # только GetGems и только один раз


@pytest.mark.asyncio
async def test_with_retry_does_not_retry_permanent_errors():
    from core.retry import with_retry

    calls = 0

    async def boom():
        nonlocal calls
        calls += 1
        raise PermanentError("HTTP 404")

    with pytest.raises(PermanentError):
        await with_retry(boom, context="t")
    assert calls == 1
