# /workspace/tests/conftest.py — фикстуры pytest (in-memory SQLite вместо PG)
from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Чтобы pydantic-settings не падал без .env
os.environ.setdefault("DB_URL", "postgresql+asyncpg://gt:pass@localhost/gifttrack")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("CRYPTOBOT_WEBHOOK_SECRET", "testsecret")
os.environ.setdefault("SHOWCASE_CACHE_TTL", "0")  # тесты видят свежие данные без кэша API

from db.models import Base  # noqa: E402


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """
    Тестовая сессия на aiosqlite. JSONB/UUID/pgcrypto — pg-специфичны,
    поэтому компилируем схему через generic-эквиваленты.
    BigInteger-PK (autoincrement) в SQLite не работает как rowid —
    временно приводим их к Integer только для DDL в тестах.
    """
    from sqlalchemy import BigInteger, Integer

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    # SQLite-совместимость: BIGINT PK без autoincrement -> INTEGER (rowid)
    patched: list[tuple] = []
    for table in Base.metadata.sorted_tables:
        for col in table.primary_key.columns:
            if isinstance(col.type, BigInteger):
                patched.append((col, col.type))
                col.type = Integer()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    for col, old_type in patched:  # возвращаем боевые типы
        col.type = old_type

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        yield s

    await engine.dispose()
