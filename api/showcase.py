# api/showcase.py — API витрины коллекций (карточки, график флора, продажи, предметы, избранное, подписка)
"""
  GET    /api/collections                       — карточки (floor, 24h change, items_count, rarity_summary)
  GET    /api/collections/{id}                  — метаданные + топ-атрибуты с rarity
  GET    /api/collections/{id}/floor-history    — ?range=7d|30d|all  -> [{ts, price}]
  GET    /api/collections/{id}/sales            — ?limit&offset
  GET    /api/items/{address}                   — атрибуты, редкость, владелец, цена листинга (initData)
  GET/POST/DELETE /api/favorites                — избранное (initData)
  GET    /api/subscription                      — {plan, expires_at, is_active, plans}
  POST   /api/subscription/invoice              — инвойс CryptoBot на выбранный тариф
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import get_current_user_init_data
from api.twa import _parse_user_id, get_session
from core.config import get_settings
from core.config import get_settings
from core.redis_client import floor_key
from db.models import Collection, User
from services import showcase as sc
from services.tonapi import TonApiClient

router = APIRouter(prefix="/api", tags=["showcase"])


def get_tonapi() -> TonApiClient:
    """DI: клиент TonAPI (в тестах подменяется)."""
    return TonApiClient(get_settings())


async def _redis_floors(ids: list[str]) -> dict[str, float]:
    """Свежие floors из Redis одним MGET; при недоступности Redis витрина работает на данных БД."""
    if not ids:
        return {}
    try:
        import orjson

        from core.redis_client import create_redis

        redis = create_redis()
        try:
            values = await redis.mget([floor_key(cid) for cid in ids])
            out: dict[str, float] = {}
            for cid, raw in zip(ids, values, strict=True):
                if raw is not None:
                    try:
                        out[cid] = float(orjson.loads(raw))
                    except (orjson.JSONDecodeError, TypeError, ValueError):
                        continue
            return out
        finally:
            await redis.aclose()
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"showcase: Redis floors недоступны: {exc!r}")
        return {}


async def _require_collection(session: AsyncSession, raw_id: str) -> uuid.UUID:
    cid = sc.parse_uuid(raw_id)
    if cid is None or await session.get(Collection, cid) is None:
        raise HTTPException(status_code=404, detail="Collection not found")
    return cid


def _username(init_data: dict[str, Any]) -> str | None:
    import orjson

    try:
        return orjson.loads(init_data.get("user", "{}")).get("username")
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Коллекции
# ---------------------------------------------------------------------------
async def _tg_stats(ids: list[str]) -> dict[str, dict[str, Any]]:
    """Данные Telegram по коллекциям (число улучшенных, флор и листинги маркета Telegram) одним MGET.

    Пишет их TelegramMarketWatcher (сервис resolver). Нет Redis/данных — пусто, витрина работает на БД.
    """
    if not ids:
        return {}
    try:
        import orjson

        from core.redis_client import create_redis

        redis = create_redis()
        try:
            keys = [f"tg_market:floor:{c}" for c in ids] + [f"tg_market:stats:{c}" for c in ids] + ["tg_market:rate"]
            raw = await redis.mget(keys)
        finally:
            await redis.aclose()

        def load(v: Any) -> Any:
            try:
                return orjson.loads(v) if v is not None else None
            except orjson.JSONDecodeError:
                return None

        n = len(ids)
        rate_global = load(raw[-1])
        out: dict[str, dict[str, Any]] = {}
        for i, cid in enumerate(ids):
            floor, stats = load(raw[i]) or {}, load(raw[n + i]) or {}
            rate = stats.get("ton_per_star") or rate_global
            min_stars = floor.get("min_stars")
            out[cid] = {
                "issued_count": stats.get("issued"),
                "tg_listed": floor.get("listed") if floor.get("listed") is not None else stats.get("listed"),
                "tg_floor_stars": min_stars,
                "tg_floor_price": round(min_stars * rate, 2) if min_stars and rate else None,
            }
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"showcase: данные Telegram недоступны: {exc!r}")
        return {}


def _apply_tg(item: dict[str, Any], tg: dict[str, Any] | None) -> None:
    """Добавляет в карточку данные Telegram и «лучший флор» (минимум из блокчейна и маркета Telegram)."""
    tg = tg or {}
    item["issued_count"] = tg.get("issued_count")
    item["tg_listed"] = tg.get("tg_listed")
    show = get_settings().tg_floor_show
    item["tg_floor_stars"] = tg.get("tg_floor_stars") if show else None
    item["tg_floor_price"] = tg.get("tg_floor_price") if show else None
    # основной флор — всегда ончейн; телеграмный только справочно (TG_FLOOR_SHOW=true)
    item["best_floor"] = item.get("floor_price") or None
    item["best_floor_source"] = "onchain" if item["best_floor"] else None


_LIST_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}


@router.get("/collections")
async def get_collections(session: AsyncSession = Depends(get_session)) -> dict[str, Any]:
    """Лёгкий список коллекций; ответ кэшируется на SHOWCASE_CACHE_TTL секунд (общий для всех пользователей)."""
    import time

    ttl = get_settings().showcase_cache_ttl
    now = time.monotonic()
    if ttl > 0 and _LIST_CACHE["data"] is not None and now - _LIST_CACHE["ts"] < ttl:
        return _LIST_CACHE["data"]
    ids = [str(i) for i in (await session.execute(select(Collection.id))).scalars().all()]
    overrides = await _redis_floors(ids)
    data = {"collections": await sc.list_collections(session, overrides)}
    tg = await _tg_stats([c["id"] for c in data["collections"]])
    for item in data["collections"]:
        _apply_tg(item, tg.get(item["id"]))
    if ttl > 0:
        _LIST_CACHE.update(ts=now, data=data)
    return data


@router.get("/collections/{collection_id}/attributes")
async def get_collection_attributes(collection_id: str, session: AsyncSession = Depends(get_session)) -> dict[str, Any]:
    """Модели, фоны, символы и узоры одной коллекции (форма фильтров)."""
    cid = await _require_collection(session, collection_id)
    data = await sc.list_collection_attributes(session, cid)
    assert data is not None
    return data


@router.get("/collections/{collection_id}")
async def get_collection(collection_id: str, session: AsyncSession = Depends(get_session)) -> dict[str, Any]:
    cid = await _require_collection(session, collection_id)
    override = (await _redis_floors([str(cid)])).get(str(cid))
    detail = await sc.get_collection_detail(session, cid, override)
    assert detail is not None
    _apply_tg(detail, (await _tg_stats([str(cid)])).get(str(cid)))
    return detail


@router.get("/collections/{collection_id}/floor-history")
async def get_floor_history(
    collection_id: str,
    range_: str = Query("7d", alias="range", pattern="^(7d|30d|all)$"),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    cid = await _require_collection(session, collection_id)
    return {"range": range_, "points": await sc.floor_history(session, cid, range_)}


@router.get("/collections/{collection_id}/sales")
async def get_sales(
    collection_id: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    cid = await _require_collection(session, collection_id)
    return await sc.sales_page(session, cid, limit, offset)


# ---------------------------------------------------------------------------
# Предмет
# ---------------------------------------------------------------------------
@router.get("/items/{address}")
async def get_item(
    address: str,
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
    tonapi: TonApiClient = Depends(get_tonapi),
) -> dict[str, Any]:
    user_id = _parse_user_id(init_data)
    user = (await session.execute(select(User).where(User.tg_id == user_id))).scalar_one_or_none()
    premium = sc.subscription_info(user)["is_active"]
    item = await sc.item_detail(session, address, premium)
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    price = await tonapi.fetch_item_sale_price(address)  # None без ключа / без листинга
    item["listing_price"] = float(price) if price is not None else None
    return item


# ---------------------------------------------------------------------------
# Избранное
# ---------------------------------------------------------------------------
class FavoriteIn(BaseModel):
    collection_id: str | None = None
    item_address: str | None = None


async def _favorite_target(
    session: AsyncSession, collection_id: str | None, item_address: str | None
) -> tuple[uuid.UUID | None, str | None]:
    if bool(collection_id) == bool(item_address):
        raise HTTPException(status_code=422, detail="Specify exactly one of collection_id / item_address")
    if collection_id:
        return await _require_collection(session, collection_id), None
    return None, item_address


@router.get("/favorites")
async def get_favorites(
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    return await sc.list_favorites(session, _parse_user_id(init_data))


@router.post("/favorites")
async def add_favorite(
    payload: FavoriteIn,
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    user_id = _parse_user_id(init_data)
    cid, addr = await _favorite_target(session, payload.collection_id, payload.item_address)
    await sc.ensure_user(session, user_id, _username(init_data))
    created = await sc.add_favorite(session, user_id, cid, addr)
    return {"ok": True, "created": created}


@router.delete("/favorites")
async def delete_favorite(
    collection_id: str | None = Query(None),
    item_address: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    user_id = _parse_user_id(init_data)
    cid, addr = await _favorite_target(session, collection_id, item_address)
    return {"ok": True, "removed": await sc.remove_favorite(session, user_id, cid, addr)}


# ---------------------------------------------------------------------------
# Подписка
# ---------------------------------------------------------------------------
@router.get("/subscription")
async def get_subscription(
    session: AsyncSession = Depends(get_session),
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    user_id = _parse_user_id(init_data)
    user = (await session.execute(select(User).where(User.tg_id == user_id))).scalar_one_or_none()
    return {**sc.subscription_info(user), "plans": get_settings().subscription_plans}


class InvoiceIn(BaseModel):
    plan_id: str


@router.post("/subscription/invoice")
async def create_subscription_invoice(
    payload: InvoiceIn,
    init_data: dict = Depends(get_current_user_init_data),
) -> dict[str, Any]:
    settings = get_settings()
    plan = next((p for p in settings.subscription_plans if p["id"] == payload.plan_id), None)
    if plan is None:
        raise HTTPException(status_code=422, detail="Unknown plan")
    if not settings.cryptobot_token:
        raise HTTPException(status_code=503, detail="Payments are not configured")
    user_id = _parse_user_id(init_data)

    from services.payments import CryptoBotClient

    invoice = await CryptoBotClient(settings.cryptobot_token).create_invoice(
        amount=plan["price_usd"],
        payload=f"{user_id}:{plan['days']}",
        description=f"GiftTrack Premium — {plan['title']}",
        fiat="USD",
    )
    return {"pay_url": invoice["pay_url"]}
