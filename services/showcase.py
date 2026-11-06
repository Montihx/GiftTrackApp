# services/showcase.py — выборки для API витрины (чистая работа с БД, без FastAPI/Redis)
from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    Attribute,
    Collection,
    Favorite,
    FloorSnapshot,
    NftOwnershipHistory,
    ProcessedNft,
    Sale,
    User,
)

ATTR_TYPES = ("model", "background", "symbol", "pattern")
ATTR_PLURAL = {"model": "models", "background": "backgrounds", "symbol": "symbols", "pattern": "patterns"}
PLACEHOLDER_PREFIX = "tg_gift:"
RANGES = {"7d": dt.timedelta(days=7), "30d": dt.timedelta(days=30), "all": None}


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_uuid(raw: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError, TypeError):
        return None


def _f(value: Any) -> float | None:
    return float(value) if value is not None else None


def rarity_pct(attr_count: int | None, total: int | None) -> float | None:
    """Доля предметов с трейтом от всей коллекции, % (None — данных нет)."""
    if not attr_count or not total:
        return None
    return round(min(attr_count / total * 100, 100.0), 2)


def is_trackable(contract_address: str) -> bool:
    """Есть реальный адрес контракта (а не seed-плейсхолдер tg_gift:...)."""
    return not contract_address.startswith(PLACEHOLDER_PREFIX)


# ---------------------------------------------------------------------------
# 24h change по снапшотам флора
# ---------------------------------------------------------------------------
async def floor_changes_24h(
    session: AsyncSession, floors: dict[uuid.UUID, Any], now: dt.datetime | None = None
) -> dict[uuid.UUID, float | None]:
    """
    % изменения флора за 24ч: (текущий − последний снапшот старше 24ч) / он × 100.
    Нет снапшота старше 24ч или текущего флора -> None (UI покажет «—»).
    """
    out: dict[uuid.UUID, float | None] = {cid: None for cid in floors}
    ids = [cid for cid, fl in floors.items() if fl is not None]
    if not ids:
        return out
    cutoff = (now or utcnow()) - dt.timedelta(hours=24)
    # Самый поздний снапшот старше 24ч — одна строка на коллекцию (раньше выгружались ВСЕ снимки за 30 дней,
    # а их пишется много: по одному на каждое изменение флора)
    window = (
        select(FloorSnapshot.collection_id.label("cid"), func.max(FloorSnapshot.ts).label("ts"))
        .where(
            FloorSnapshot.collection_id.in_(ids),
            FloorSnapshot.ts <= cutoff,
            FloorSnapshot.ts >= cutoff - dt.timedelta(days=30),
        )
        .group_by(FloorSnapshot.collection_id)
        .subquery()
    )
    rows = (
        await session.execute(
            select(FloorSnapshot.collection_id, FloorSnapshot.price).join(
                window, (FloorSnapshot.collection_id == window.c.cid) & (FloorSnapshot.ts == window.c.ts)
            )
        )
    ).all()
    baseline: dict[uuid.UUID, Decimal] = {}
    for cid, price in rows:
        baseline.setdefault(cid, price)
    for cid in ids:
        base = baseline.get(cid)
        if base is None or Decimal(str(base)) == 0:
            continue
        out[cid] = round((float(floors[cid]) - float(base)) / float(base) * 100, 2)
    return out


# ---------------------------------------------------------------------------
# Коллекции
# ---------------------------------------------------------------------------
def _attr_dict(a: Attribute, total: int | None) -> dict[str, Any]:
    return {
        "id": str(a.id),
        "value": a.value,
        "icon_url": a.icon_url,
        "count": a.items_count,
        "rarity": rarity_pct(a.items_count, total),
        "floor": _f(a.floor_price) if a.floor_price else None,  # 0 в схеме = «нет floor»
    }


def _rarity_summary(attrs: list[Attribute], total: int | None) -> dict[str, Any]:
    counts = {ATTR_PLURAL[t]: sum(1 for a in attrs if a.type == t) for t in ATTR_TYPES}
    rarest = None
    scored = [(rarity_pct(a.items_count, total), a) for a in attrs]
    scored = [(r, a) for r, a in scored if r is not None]
    if scored:
        r, a = min(scored, key=lambda x: (x[0], x[1].value))
        rarest = {"type": a.type, "value": a.value, "rarity": r}
    return {**counts, "rarest": rarest}


def _collection_meta(
    coll: Collection, floor: Any, change: float | None, attrs: list[Attribute]
) -> dict[str, Any]:
    return {
        "id": str(coll.id),
        "name": coll.name,
        "icon_url": coll.icon_url,
        "floor_price": _f(floor),
        "floor_24h_change": change,
        "items_count": coll.items_count,
        "trackable": is_trackable(coll.contract_address),
        "source": coll.source,
        "rarity_summary": _rarity_summary(attrs, coll.items_count),
    }


async def _rarity_summaries(session: AsyncSession) -> dict[uuid.UUID, dict[str, Any]]:
    """rarity_summary по всем коллекциям двумя агрегатными запросами (без выгрузки всех атрибутов)."""
    counts: dict[uuid.UUID, dict[str, int]] = {}
    rows = await session.execute(
        select(Attribute.collection_id, Attribute.type, func.count()).group_by(Attribute.collection_id, Attribute.type)
    )
    for cid, typ, n in rows.all():
        counts.setdefault(cid, {})[typ] = n
    ranked = (
        select(
            Attribute.collection_id.label("cid"), Attribute.type.label("typ"), Attribute.value.label("val"),
            Attribute.items_count.label("cnt"),
            func.row_number().over(
                partition_by=Attribute.collection_id, order_by=(Attribute.items_count.asc(), Attribute.value.asc())
            ).label("rn"),
        ).where(Attribute.items_count > 0).subquery()
    )
    rarest: dict[uuid.UUID, tuple[str, str, int]] = {
        cid: (typ, val, cnt)
        for cid, typ, val, cnt in (await session.execute(
            select(ranked.c.cid, ranked.c.typ, ranked.c.val, ranked.c.cnt).where(ranked.c.rn == 1)
        )).all()
    }
    return {"counts": counts, "rarest": rarest}  # type: ignore[dict-item]


def _summary_for(coll: Collection, agg: Any) -> dict[str, Any]:
    c = agg["counts"].get(coll.id, {})
    out: dict[str, Any] = {ATTR_PLURAL[t]: c.get(t, 0) for t in ATTR_TYPES}
    rarest = agg["rarest"].get(coll.id)
    r = rarity_pct(rarest[2], coll.items_count) if rarest else None
    out["rarest"] = {"type": rarest[0], "value": rarest[1], "rarity": r} if rarest and r is not None else None
    return out


async def list_collections(
    session: AsyncSession, floor_overrides: dict[str, float] | None = None
) -> list[dict[str, Any]]:
    """Лёгкий список коллекций (в т.ч. seed без адреса) + 24h change + сводка редкости.

    Атрибуты (модели/фоны/символы) здесь НЕ отдаются: у коллекции их сотни, а коллекций больше ста —
    раньше ответ был в мегабайты и считался секундами. Для формы фильтров они подгружаются по одной
    выбранной коллекции: list_collection_attributes. Ключ "attributes" оставлен пустым для старых клиентов.
    """
    floor_overrides = floor_overrides or {}
    colls = (await session.execute(select(Collection).order_by(Collection.name))).scalars().all()
    agg = await _rarity_summaries(session)
    floors = {c.id: floor_overrides.get(str(c.id), c.floor_price) for c in colls}
    changes = await floor_changes_24h(session, floors)

    result = []
    for c in colls:
        item: dict[str, Any] = {
            "id": str(c.id), "name": c.name, "icon_url": c.icon_url,
            "floor_price": _f(floors[c.id]), "floor_24h_change": changes[c.id],
            "items_count": c.items_count, "trackable": is_trackable(c.contract_address), "source": c.source,
            "rarity_summary": _summary_for(c, agg),
            "attributes": {ATTR_PLURAL[t]: [] for t in ATTR_TYPES},
        }
        result.append(item)
    return result


async def list_collection_attributes(session: AsyncSession, collection_id: uuid.UUID) -> dict[str, Any] | None:
    """Все модели/фоны/символы/узоры одной коллекции — для формы фильтров."""
    coll = await session.get(Collection, collection_id)
    if coll is None:
        return None
    attrs = (await session.execute(
        select(Attribute).where(Attribute.collection_id == collection_id).order_by(Attribute.value)
    )).scalars().all()
    return {
        "collection_id": str(collection_id),
        "attributes": {
            ATTR_PLURAL[t]: [_attr_dict(a, coll.items_count) for a in attrs if a.type == t] for t in ATTR_TYPES
        },
    }


async def get_collection_detail(
    session: AsyncSession, collection_id: uuid.UUID, floor_override: float | None = None, top_n: int = 10
) -> dict[str, Any] | None:
    coll = await session.get(Collection, collection_id)
    if coll is None:
        return None
    attrs = (
        await session.execute(select(Attribute).where(Attribute.collection_id == collection_id))
    ).scalars().all()
    floor = floor_override if floor_override is not None else coll.floor_price
    change = (await floor_changes_24h(session, {coll.id: floor}))[coll.id]
    detail = _collection_meta(coll, floor, change, list(attrs))

    top: dict[str, list[dict[str, Any]]] = {}
    for t in ATTR_TYPES:
        items = [_attr_dict(a, coll.items_count) for a in attrs if a.type == t]
        # самые редкие первыми; без данных о редкости — в конце, по алфавиту
        items.sort(key=lambda x: (x["rarity"] is None, x["rarity"] or 0.0, x["value"]))
        top[ATTR_PLURAL[t]] = items[:top_n]
    detail["top_attributes"] = top
    return detail


async def floor_history(
    session: AsyncSession, collection_id: uuid.UUID, range_: str, now: dt.datetime | None = None
) -> list[dict[str, Any]]:
    stmt = select(FloorSnapshot.ts, FloorSnapshot.price).where(FloorSnapshot.collection_id == collection_id)
    delta = RANGES[range_]
    if delta is not None:
        stmt = stmt.where(FloorSnapshot.ts >= (now or utcnow()) - delta)
    rows = (await session.execute(stmt.order_by(FloorSnapshot.ts.asc()))).all()
    return [{"ts": ts.isoformat(), "price": float(price)} for ts, price in rows]


async def sales_page(
    session: AsyncSession, collection_id: uuid.UUID, limit: int, offset: int
) -> dict[str, Any]:
    total = (
        await session.execute(select(func.count()).select_from(Sale).where(Sale.collection_id == collection_id))
    ).scalar_one()
    rows = (
        await session.execute(
            select(Sale)
            .where(Sale.collection_id == collection_id)
            .order_by(Sale.ts.desc(), Sale.id.desc())
            .limit(limit)
            .offset(offset)
        )
    ).scalars().all()
    # buyer_tg_id намеренно не отдаём — персональные данные не нужны ленте продаж
    return {
        "total": total,
        "items": [{"item_address": s.item_address, "price": float(s.price), "ts": s.ts.isoformat()} for s in rows],
    }


# ---------------------------------------------------------------------------
# Предмет
# ---------------------------------------------------------------------------
async def item_detail(session: AsyncSession, address: str, is_premium: bool) -> dict[str, Any] | None:
    nft = await session.get(ProcessedNft, address)
    if nft is None:
        return None
    coll = await session.get(Collection, nft.collection_id) if nft.collection_id else None
    total = coll.items_count if coll else None

    attributes = []
    for t, value in (("model", nft.model), ("background", nft.background),
                     ("symbol", nft.symbol), ("pattern", nft.pattern)):
        if not value:
            continue
        rarity = None
        if coll is not None:
            attr = (
                await session.execute(
                    select(Attribute).where(
                        Attribute.collection_id == coll.id, Attribute.type == t, Attribute.value == value
                    )
                )
            ).scalar_one_or_none()
            if attr is not None:
                rarity = rarity_pct(attr.items_count, total)
        attributes.append({"type": t, "value": value, "rarity": rarity})

    username = (
        await session.execute(
            select(NftOwnershipHistory.owner_username)
            .where(NftOwnershipHistory.nft_address == address, NftOwnershipHistory.owner_username.is_not(None))
            .order_by(NftOwnershipHistory.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    has_contact = bool(username)
    owner = {
        "ton": nft.current_owner_ton,
        # ссылка на владельца — премиум-функция («Точный поиск владельца»)
        "username": username if is_premium else None,
        "profile_url": f"https://t.me/{username}" if (is_premium and username) else None,
        "locked": has_contact and not is_premium,
    }
    return {
        "address": nft.nft_address,
        "index": nft.nft_index,
        "collection": {"id": str(coll.id), "name": coll.name, "icon_url": coll.icon_url} if coll else None,
        "attributes": attributes,
        "owner": owner,
        "listing_price": None,  # заполняется в API из TonAPI (если есть ключ)
    }


# ---------------------------------------------------------------------------
# Избранное / пользователь / подписка
# ---------------------------------------------------------------------------
async def ensure_user(session: AsyncSession, tg_id: int, username: str | None = None) -> User:
    user = (await session.execute(select(User).where(User.tg_id == tg_id))).scalar_one_or_none()
    if user is None:
        user = User(tg_id=tg_id, username=username)
        session.add(user)
        await session.flush()
    return user


async def list_favorites(session: AsyncSession, user_id: int) -> dict[str, Any]:
    favs = (
        await session.execute(select(Favorite).where(Favorite.user_id == user_id).order_by(Favorite.id.desc()))
    ).scalars().all()
    coll_ids = [f.collection_id for f in favs if f.collection_id]
    colls = {}
    if coll_ids:
        colls = {
            c.id: c
            for c in (await session.execute(select(Collection).where(Collection.id.in_(coll_ids)))).scalars().all()
        }
    floors = {c.id: c.floor_price for c in colls.values()}
    changes = await floor_changes_24h(session, floors)
    collections, items = [], []
    for f in favs:
        if f.collection_id and f.collection_id in colls:
            c = colls[f.collection_id]
            collections.append(
                {"id": str(c.id), "name": c.name, "icon_url": c.icon_url,
                 "floor_price": _f(c.floor_price), "floor_24h_change": changes[c.id]}
            )
        elif f.item_address:
            items.append({"item_address": f.item_address})
    await _enrich_favorite_items(session, items)
    return {"collections": collections, "items": items}


async def _enrich_favorite_items(session: AsyncSession, items: list[dict[str, Any]]) -> None:
    """Имя «<коллекция> #<номер>», модель и иконка — вместо голого адреса в «Избранном»."""
    if not items:
        return
    rows = (
        await session.execute(
            select(ProcessedNft, Collection)
            .join(Collection, Collection.id == ProcessedNft.collection_id, isouter=True)
            .where(ProcessedNft.nft_address.in_([i["item_address"] for i in items]))
        )
    ).all()
    by_addr = {nft.nft_address: (nft, coll) for nft, coll in rows}
    for i in items:
        nft, coll = by_addr.get(i["item_address"], (None, None))
        i["name"] = f"{coll.name} #{nft.nft_index}" if nft is not None and coll is not None else None
        i["model"] = nft.model if nft is not None else None
        i["icon_url"] = coll.icon_url if coll is not None else None
        i["collection_name"] = coll.name if coll is not None else None


async def add_favorite(
    session: AsyncSession, user_id: int, collection_id: uuid.UUID | None, item_address: str | None
) -> bool:
    """Идемпотентно. True — создано, False — уже было."""
    stmt = select(Favorite.id).where(Favorite.user_id == user_id)
    stmt = stmt.where(Favorite.collection_id == collection_id) if collection_id else stmt.where(
        Favorite.item_address == item_address
    )
    if (await session.execute(stmt)).first() is not None:
        return False
    session.add(Favorite(user_id=user_id, collection_id=collection_id, item_address=item_address))
    await session.flush()
    return True


async def remove_favorite(
    session: AsyncSession, user_id: int, collection_id: uuid.UUID | None, item_address: str | None
) -> bool:
    stmt = select(Favorite).where(Favorite.user_id == user_id)
    stmt = stmt.where(Favorite.collection_id == collection_id) if collection_id else stmt.where(
        Favorite.item_address == item_address
    )
    fav = (await session.execute(stmt)).scalar_one_or_none()
    if fav is None:
        return False
    await session.delete(fav)
    await session.flush()
    return True


def subscription_info(user: User | None, now: dt.datetime | None = None) -> dict[str, Any]:
    """{plan, expires_at, is_active}: активна только платная подписка с будущим сроком."""
    now = now or utcnow()
    if user is None or user.sub_plan in (None, "free"):
        return {"plan": "free", "expires_at": None, "is_active": False}
    exp = user.sub_expires_at
    if exp is not None and exp.tzinfo is None:
        exp = exp.replace(tzinfo=dt.timezone.utc)
    return {
        "plan": user.sub_plan,
        "expires_at": exp.isoformat() if exp else None,
        "is_active": bool(exp and exp > now),
    }
