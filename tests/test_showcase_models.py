# tests/test_showcase_models.py — FloorSnapshot / Sale / Favorite + миграция 0002
from __future__ import annotations

import datetime as dt
import importlib.util
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from db.models import Collection, Favorite, FloorSnapshot, Sale, User

NOW = dt.datetime(2026, 11, 5, 12, 0, tzinfo=dt.timezone.utc)


async def _coll(session: Any) -> Collection:
    c = Collection(name="Teddy", contract_address="EQ" + "A" * 46, source="seed")
    session.add(c)
    await session.commit()
    return c


async def test_floor_snapshot_roundtrip(session: Any) -> None:
    c = await _coll(session)
    session.add(FloorSnapshot(collection_id=c.id, price=Decimal("5.5"), ts=NOW))
    await session.commit()
    snap = (await session.execute(select(FloorSnapshot))).scalar_one()
    assert snap.price == Decimal("5.5") and snap.collection_id == c.id


async def test_sale_dedupe_by_item_and_ts(session: Any) -> None:
    c = await _coll(session)
    session.add(Sale(collection_id=c.id, item_address="EQitem", price=Decimal("3"), ts=NOW))
    await session.commit()
    session.add(Sale(collection_id=c.id, item_address="EQitem", price=Decimal("3"), ts=NOW))
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()


async def test_sale_buyer_is_optional(session: Any) -> None:
    c = await _coll(session)
    session.add(Sale(collection_id=c.id, item_address="EQ1", price=Decimal("1"), ts=NOW))
    await session.commit()
    assert (await session.execute(select(Sale))).scalar_one().buyer_tg_id is None


async def test_favorite_collection_and_item(session: Any) -> None:
    c = await _coll(session)
    session.add(User(tg_id=1, username="u"))
    await session.commit()
    session.add_all([Favorite(user_id=1, collection_id=c.id), Favorite(user_id=1, item_address="EQitem")])
    await session.commit()
    assert len((await session.execute(select(Favorite))).scalars().all()) == 2


async def test_favorite_duplicate_rejected(session: Any) -> None:
    c = await _coll(session)
    session.add(User(tg_id=1))
    await session.commit()
    session.add(Favorite(user_id=1, collection_id=c.id))
    await session.commit()
    session.add(Favorite(user_id=1, collection_id=c.id))
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()


async def test_favorite_needs_exactly_one_target(session: Any) -> None:
    c = await _coll(session)
    session.add(User(tg_id=1))
    await session.commit()
    session.add(Favorite(user_id=1))  # ни коллекции, ни item
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()
    session.add(Favorite(user_id=1, collection_id=c.id, item_address="EQx"))  # оба сразу
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()


async def test_collection_floor_nullable_and_seed_source(session: Any) -> None:
    c = await _coll(session)
    assert c.floor_price is None and c.source == "seed"


def test_migration_0002_chain() -> None:
    path = Path(__file__).resolve().parent.parent / "db" / "migrations" / "versions" / "0002_showcase_tables.py"
    spec = importlib.util.spec_from_file_location("m0002", path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    assert mod.revision == "0002" and mod.down_revision == "0001"
    assert callable(mod.upgrade) and callable(mod.downgrade)
