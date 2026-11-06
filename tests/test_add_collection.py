# tests/test_add_collection.py — /add_collection: валидация адреса и идемпотентность
from __future__ import annotations

from typing import Any

from sqlalchemy import func, select

from bot.admin import add_manual_collection, normalize_ton_address
from db.models import Collection

ADDR = "EQDTwwzDZK39EYHdBcnd_A6CnpQQGwRqLXjxcJkeN2vPXLkY"


def test_normalize_rejects_garbage() -> None:
    assert normalize_ton_address("hello") is None
    assert normalize_ton_address("EQshort") is None
    assert normalize_ton_address("") is None


def test_normalize_accepts_friendly() -> None:
    assert normalize_ton_address(f"  {ADDR} ") is not None


async def test_add_is_idempotent_and_manual(session: Any) -> None:
    status, coll = await add_manual_collection(session, ADDR, "Teddy")
    assert status == "created" and coll.source == "manual" and coll.name == "Teddy"
    status2, coll2 = await add_manual_collection(session, ADDR, "Other name")
    assert status2 == "exists" and coll2.id == coll.id and coll2.name == "Teddy"
    cnt = (await session.execute(select(func.count()).select_from(Collection))).scalar_one()
    assert cnt == 1


async def test_add_without_name_uses_placeholder(session: Any) -> None:
    _, coll = await add_manual_collection(session, ADDR, None)
    assert coll.name.startswith("Collection ")


async def test_existing_source_not_overwritten(session: Any) -> None:
    session.add(Collection(name="Teddy", contract_address=ADDR, source="getgems"))
    await session.commit()
    status, coll = await add_manual_collection(session, ADDR, "X")
    assert status == "exists" and coll.source == "getgems"
