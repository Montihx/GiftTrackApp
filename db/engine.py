# /workspace/db/engine.py — асинхронный движок БД + session factory
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import get_settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Ленивый синглтон движка (пул соединений на весь процесс сервиса)."""
    global _engine, _session_factory
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.db_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,   # защита от «мёртвых» соединений после рестарта postgres
            pool_recycle=1800,
            echo=False,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    get_engine()  # гарантируем инициализацию
    assert _session_factory is not None
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Контекст-менеджер/зависимость FastAPI: commit при успехе, rollback при ошибке."""
    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise



async def dispose_engine() -> None:
    """Закрытие пула соединений при graceful shutdown."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
