# /workspace/api/twa.py — роутеры TWA API
"""
Endpoints для Telegram Web App:
  (GET /api/collections и остальная витрина — в api/showcase.py)
  GET    /api/filters                — список фильтров пользователя
  POST   /api/filters                — создать фильтр
  DELETE /api/filters/{filter_id}    — удалить фильтр
  GET    /api/collections/{id}/stream — SSE-стрим real-time данных (push каждые 5 сек)
"""
from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any


import orjson
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import get_current_user_init_data
from core.redis_client import cache_get_json, floor_key
from db.engine import session_scope
from db.models import Attribute, Collection, Filter

router = APIRouter(prefix="/api", tags=["twa"])
async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI-зависимость: сессия БД (commit/rollback через session_scope)."""
    async with session_scope() as session:
        yield session


class FilterCreate(BaseModel):
    """Схема создания фильтра из TWA."""

    collection_id: str | None = None
    model_id: str | None = None
    background_id: str | None = None
    symbol_id: str | None = None
    pattern_id: str | None = None
    floor_min: float | None = Field(default=None, ge=0)
    floor_max: float | None = Field(default=None, ge=0)


def _parse_user_id(init_data: dict[str, Any]) -> int:
    """Извлекает user.id из распарсенного initData."""
    user_raw = init_data.get("user", "")
    if not user_raw:
        raise HTTPException(status_code=401, detail="No user in initData")
    try:
        user = orjson.loads(user_raw)
        return int(user["id"])
    except (ValueError, KeyError, TypeError):
        raise HTTPException(status_code=401, detail="Bad user payload") from None


def _username(init_data: dict[str, Any]) -> str | None:
    try:
        return orjson.loads(init_data.get("user", "{}")).get("username")
    except Exception:  # noqa: BLE001
        return None


@router.get("/filters")
async def get_filters(
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    """Список фильтров текущего пользователя."""
    user_id = _parse_user_id(init_data)
    stmt = select(Filter).where(Filter.user_id == user_id)
    filters = (await session.execute(stmt)).scalars().all()
    # Названия атрибутов — для карточек «Мои фильтры» (тяжёлый список коллекций с атрибутами больше не отдаётся)
    attr_ids = {a for f in filters for a in (f.model_id, f.background_id, f.symbol_id, f.pattern_id) if a}
    names: dict[Any, str] = {}
    if attr_ids:
        rows = await session.execute(select(Attribute.id, Attribute.value).where(Attribute.id.in_(attr_ids)))
        names = {aid: val for aid, val in rows.all()}
    return {
        "filters": [
            {
                "id": str(f.id),
                "model_name": names.get(f.model_id),
                "background_name": names.get(f.background_id),
                "symbol_name": names.get(f.symbol_id),
                "pattern_name": names.get(f.pattern_id),
                "collection_id": str(f.collection_id) if f.collection_id else None,
                "model_id": str(f.model_id) if f.model_id else None,
                "background_id": str(f.background_id) if f.background_id else None,
                "symbol_id": str(f.symbol_id) if f.symbol_id else None,
                "pattern_id": str(f.pattern_id) if f.pattern_id else None,
                "floor_min": float(f.floor_min) if f.floor_min is not None else None,
                "floor_max": float(f.floor_max) if f.floor_max is not None else None,
                "is_active": f.is_active,
            }
            for f in filters
        ]
    }



@router.post("/filters")
async def create_filter(
    payload: FilterCreate,
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    """Создать фильтр для текущего пользователя."""
    user_id = _parse_user_id(init_data)
    # Пользователь мог открыть мини-приложение, не нажимая /start в боте: без строки в users фильтр
    # падал бы на внешнем ключе (500). Создаём её при необходимости.
    from services.showcase import ensure_user

    await ensure_user(session, user_id, _username(init_data))
    new_filter = Filter(
        user_id=user_id,
        collection_id=uuid.UUID(payload.collection_id) if payload.collection_id else None,
        model_id=uuid.UUID(payload.model_id) if payload.model_id else None,
        background_id=uuid.UUID(payload.background_id) if payload.background_id else None,
        symbol_id=uuid.UUID(payload.symbol_id) if payload.symbol_id else None,
        pattern_id=uuid.UUID(payload.pattern_id) if payload.pattern_id else None,
        floor_min=payload.floor_min,
        floor_max=payload.floor_max,
        is_active=True,
    )
    session.add(new_filter)
    await session.flush()
    filter_id = str(new_filter.id)
    logger.info(f"Фильтр {filter_id} создан для user {user_id}")
    return {"ok": True, "filter_id": filter_id}

@router.delete("/filters/{filter_id}")
async def delete_filter(
    filter_id: str,
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    """Удалить фильтр (только если он принадлежит пользователю)."""
    user_id = _parse_user_id(init_data)
    stmt = select(Filter).where(Filter.id == uuid.UUID(filter_id), Filter.user_id == user_id)
    filt = (await session.execute(stmt)).scalar_one_or_none()
    if filt is None:
        raise HTTPException(status_code=404, detail="Filter not found")
    await session.delete(filt)
    return {"ok": True}



@router.get("/collections/{collection_id}/stream")
async def collection_stream(collection_id: str) -> StreamingResponse:
    """
    SSE-стрим real-time данных коллекции (floor, recent sales).

    Push каждые 5 сек. Frontend подключается через EventSource.
    """

    async def event_generator():
        from core.redis_client import create_redis

        from services import showcase as sc

        redis = create_redis()
        cid = sc.parse_uuid(collection_id)
        try:
            while True:
                floor = await cache_get_json(redis, floor_key(collection_id))
                change = None
                if cid is not None:
                    try:
                        async with session_scope() as session:
                            if floor is None:
                                coll = await session.get(Collection, cid)
                                floor = float(coll.floor_price) if coll and coll.floor_price is not None else None
                            change = (await sc.floor_changes_24h(session, {cid: floor}))[cid]
                    except Exception as exc:  # noqa: BLE001 — стрим живёт и без БД
                        logger.debug(f"stream {collection_id}: change не посчитан: {exc!r}")
                payload = {"floor_price": floor, "floor_24h_change": change}
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(5)
        finally:
            await redis.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
