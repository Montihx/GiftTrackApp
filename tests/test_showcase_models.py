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
    cid = c.id  # после rollback() объекты сессии expire'ятся — берём id заранее (иначе MissingGreenlet)
    session.add(User(tg_id=1))
    await session.commit()
    session.add(Favorite(user_id=1))  # ни коллекции, ни item
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()
    session.add(Favorite(user_id=1, collection_id=cid, item_address="EQx"))  # оба сразу
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()


async def test_collection_floor_nullable_and_seed_source(session: Any) -> None:
    c = await _coll(session)
    await session.refresh(c)
    assert c.floor_price is None and c.source == "seed"


def test_migration_0002_chain() -> None:
    path = Path(__file__).resolve().parent.parent / "db" / "migrations" / "versions" / "0002_showcase_tables.py"
    spec = importlib.util.spec_from_file_location("m0002", path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    assert mod.revision == "0002" and mod.down_revision == "0001"
    assert callable(mod.upgrade) and callable(mod.downgrade)

async def test_favorite_items_enriched_with_name_and_model(session) -> None:
    from db.models import Collection, Favorite, ProcessedNft, User
    from services.showcase import list_favorites

    session.add(User(tg_id=9001))
    coll = Collection(name="Plush Pepe", contract_address="EQfav_enrich", source="seed", icon_url="https://x/i.webp")
    session.add(coll)
    await session.flush()
    session.add(ProcessedNft(nft_address="0:fav", collection_id=coll.id, nft_index=2, model="Hothead"))
    session.add_all([Favorite(user_id=9001, item_address="0:fav"), Favorite(user_id=9001, item_address="0:unknown")])
    await session.commit()
    items = {i["item_address"]: i for i in (await list_favorites(session, 9001))["items"]}
    assert items["0:fav"]["name"] == "Plush Pepe #2" and items["0:fav"]["model"] == "Hothead"
    assert items["0:fav"]["icon_url"] == "https://x/i.webp"
    assert items["0:unknown"]["name"] is None


async def test_collections_list_is_light_and_attributes_load_per_collection(session) -> None:
    from decimal import Decimal

    from db.models import Attribute, Collection
    from services.showcase import list_collection_attributes, list_collections

    a = Collection(name="Alpha", contract_address="EQlight_a", source="seed", items_count=100)
    b = Collection(name="Beta", contract_address="tg_gift:beta", source="seed")
    session.add_all([a, b])
    await session.flush()
    session.add_all([
        Attribute(collection_id=a.id, type="model", value="Zed", items_count=5, floor_price=Decimal("9")),
        Attribute(collection_id=a.id, type="model", value="Amy", items_count=50),
        Attribute(collection_id=a.id, type="background", value="Red", items_count=0),
        Attribute(collection_id=a.id, type="symbol", value="Moon", items_count=2),
    ])
    await session.commit()

    rows = {c["name"]: c for c in await list_collections(session)}
    alpha = rows["Alpha"]
    assert alpha["attributes"] == {"models": [], "backgrounds": [], "symbols": [], "patterns": []}  # лёгкий ответ
    assert alpha["rarity_summary"]["models"] == 2 and alpha["rarity_summary"]["backgrounds"] == 1
    # самый редкий = минимальный положительный items_count (Moon: 2 из 100 = 2%), нулевые не считаются
    assert alpha["rarity_summary"]["rarest"] == {"type": "symbol", "value": "Moon", "rarity": 2.0}
    assert rows["Beta"]["trackable"] is False and rows["Beta"]["rarity_summary"]["rarest"] is None

    full = await list_collection_attributes(session, a.id)
    assert full is not None and [m["value"] for m in full["attributes"]["models"]] == ["Amy", "Zed"]
    assert full["attributes"]["models"][1]["floor"] == 9.0 and full["attributes"]["symbols"][0]["count"] == 2
    assert await list_collection_attributes(session, __import__("uuid").uuid4()) is None


async def test_floor_change_uses_latest_snapshot_before_cutoff(session) -> None:
    import datetime as dt
    from decimal import Decimal

    from db.models import Collection, FloorSnapshot
    from services.showcase import floor_changes_24h

    c = Collection(name="Gamma", contract_address="EQchange_g", source="seed")
    session.add(c)
    await session.flush()
    now = dt.datetime.now(dt.timezone.utc)
    for hours, price in ((72, 50), (30, 100), (26, 110), (5, 130), (1, 140)):
        session.add(FloorSnapshot(collection_id=c.id, price=Decimal(price), ts=now - dt.timedelta(hours=hours)))
    await session.commit()
    out = await floor_changes_24h(session, {c.id: Decimal(121)}, now)
    assert out[c.id] == 10.0  # база — самый поздний снимок старше 24ч (26ч назад = 110)
