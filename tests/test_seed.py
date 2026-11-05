# tests/test_seed.py — seed-каталог: структура JSON, идемпотентность, backfill адреса
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select

from core.config import Settings
from db.models import Collection
from services.seed import load_seed_entries, upsert_seed

SEED = Path(__file__).resolve().parent.parent / "db" / "seed" / "gift_collections.json"
ADDR = "EQDTwwzDZK39EYHdBcnd_A6CnpQQGwRqLXjxcJkeN2vPXLkY"


def test_shipped_seed_has_20_plus_unique_collections() -> None:
    entries = load_seed_entries(SEED)
    assert len(entries) >= 20
    assert len({e["name"] for e in entries}) == len(entries)
    assert len({e["contract_address"] for e in entries}) == len(entries)


def test_settings_default_points_to_shipped_seed() -> None:
    assert Path(Settings().seed_catalog_path).name == "gift_collections.json"


def _write(tmp_path: Path, items: list[dict[str, Any]]) -> Path:
    p = tmp_path / "seed.json"
    p.write_text(json.dumps({"collections": items}), encoding="utf-8")
    return p


async def test_upsert_creates_and_is_idempotent(session: Any) -> None:
    first = await upsert_seed(session, SEED)
    await session.commit()
    n = (await session.execute(select(func.count()).select_from(Collection))).scalar_one()
    assert first["created"] == n >= 20
    second = await upsert_seed(session, SEED)
    assert second["created"] == 0 and second["skipped"] == n
    rows = (await session.execute(select(Collection))).scalars().all()
    assert all(r.source == "seed" and r.floor_price is None for r in rows)


async def test_existing_collection_not_overwritten(session: Any, tmp_path: Path) -> None:
    session.add(Collection(name="Mine", contract_address=ADDR, source="manual"))
    await session.commit()
    stats = await upsert_seed(session, _write(tmp_path, [{"name": "Teddy", "contract_address": ADDR}]))
    assert stats["created"] == 0 and stats["skipped"] == 1
    c = (await session.execute(select(Collection).where(Collection.contract_address == ADDR))).scalar_one()
    assert c.name == "Mine" and c.source == "manual"


async def test_placeholder_backfilled_with_real_address(session: Any, tmp_path: Path) -> None:
    await upsert_seed(session, _write(tmp_path, [{"name": "Plush Pepe"}]))
    await session.commit()
    stats = await upsert_seed(session, _write(tmp_path, [{"name": "Plush Pepe", "contract_address": ADDR}]))
    assert stats["backfilled"] == 1 and stats["created"] == 0
    session.expire_all()
    rows = (await session.execute(select(Collection))).scalars().all()
    assert [r.contract_address for r in rows] == [ADDR]


async def test_invalid_address_falls_back_to_placeholder(session: Any, tmp_path: Path) -> None:
    await upsert_seed(session, _write(tmp_path, [{"name": "Bad", "contract_address": "nope"}]))
    row = (await session.execute(select(Collection))).scalar_one()
    assert row.contract_address == "tg_gift:bad"
