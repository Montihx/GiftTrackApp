# /workspace/api/twa.py — роутеры TWA API
"""
Endpoints для Telegram Web App:
  GET    /api/collections            — все коллекции с атрибутами и floor
  GET    /api/filters                — список фильтров пользователя
  POST   /api/filters                — создать фильтр
  DELETE /api/filters/{filter_id}    — удалить фильтр
  GET    /api/collections/{id}/stream — SSE-стрим real-time данных (push каждые 5 сек)
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import orjson
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import get_current_user_init_data
from core.config import get_settings
from core.redis_client import cache_get_json, collection_attrs_key, floor_key
from db.engine import session_scope
from db.models import Attribute, Collection, Filter

router = APIRouter(prefix="/api", tags=["twa"])


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
    except (ValueError, KeyError):
        raise HTTPException(status_code=401, detail="Bad user payload")


@router.get("/collections")
async def get_collections() -> dict[str, Any]:
    """Все коллекции с атрибутами (model/background/symbol/pattern) и floor-ценами."""
    from core.redis_client import create_redis

    redis = create_redis()
    try:
        async with session_scope() as session:
            stmt = select(Collection).where(~Collection.contract_address.startswith("tg_gift:"))
            collections = (await session.execute(stmt)).scalars().all()

            result = []
            for coll in collections:
                attrs_stmt = select(Attribute).where(Attribute.collection_id == coll.id)
                attrs = (await session.execute(attrs_stmt)).scalars().all()

                # Floor из Redis (быстрый путь) или из БД
                floor = await cache_get_json(redis, floor_key(str(coll.id)))
                if floor is None:
                    floor = float(coll.floor_price)

                result.append(
                    {
                        "id": str(coll.id),
                        "name": coll.name,
                        "icon_url": coll.icon_url,
                        "floor_price": floor,
                        "items_count": coll.items_count,
                        "attributes": {
                            "models": [
                                {"id": str(a.id), "value": a.value, "icon_url": a.icon_url, "count": a.items_count}
                                for a in attrs if a.type == "model"
                            ],
                            "backgrounds": [
                                {"id": str(a.id), "value": a.value, "icon_url": a.icon_url, "count": a.items_count}
                                for a in attrs if a.type == "background"
                            ],
                            "symbols": [
                                {"id": str(a.id), "value": a.value, "icon_url": a.icon_url, "count": a.items_count}
                                for a in attrs if a.type == "symbol"
                            ],
                            "patterns": [
                                {"id": str(a.id), "value": a.value, "icon_url": a.icon_url, "count": a.items_count}
                                for a in attrs if a.type == "pattern"
                            ],
                        },
                    }
                )
            return {"collections": result}
    finally:
        await redis.aclose()


@router.get("/filters")
async def get_filters(init_data: dict = Depends(get_current_user_init_data)) -> dict[str, Any]:
    """Список фильтров текущего пользователя."""
    user_id = _parse_user_id(init_data)
    async with session_scope() as session:
        stmt = select(Filter).where(Filter.user_id == user_id)
        filters = (await session.execute(stmt)).scalars().all()
        return {
            "filters": [
                {
                    "id": str(f.id),
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
    payload: FilterCreate, init_data: dict = Depends(get_current_user_init_data)
) -> dict[str, Any]:
    """Создать фильтр для текущего пользователя."""
    user_id = _parse_user_id(init_data)

    async with session_scope() as session:
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
async def delete_filter(filter_id: str, init_data: dict = Depends(get_current_user_init_data)) -> dict[str, Any]:
    """Удалить фильтр (только если он принадлежит пользователю)."""
    user_id = _parse_user_id(init_data)

    async with session_scope() as session:
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

        redis = create_redis()
        try:
            while True:
                floor = await cache_get_json(redis, floor_key(collection_id))
                payload = {"floor_price": floor}
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(5)
        finally:
            await redis.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
