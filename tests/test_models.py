# /workspace/tests/test_models.py — смок-тесты моделей Этапа 1
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    Attribute,
    Collection,
    Filter,
    ListenerState,
    ModelSale,
    User,
    WalletTgMap,
)


async def _mk_collection(session: AsyncSession, addr: str = "EQTest...abc") -> Collection:
    c = Collection(
        name="Plush Pepe",
        contract_address=addr,
        source="getgems",
        floor_price=Decimal("12.5"),
        items_count=100,
    )
    session.add(c)
    await session.flush()
    return c


@pytest.mark.asyncio
async def test_collection_crud(session: AsyncSession) -> None:
    c = await _mk_collection(session)
    assert c.id is not None
    got = (await session.execute(select(Collection).where(Collection.id == c.id))).scalar_one()
    assert got.name == "Plush Pepe"
    assert got.floor_price == Decimal("12.5")


@pytest.mark.asyncio
async def test_collection_unique_contract(session: AsyncSession) -> None:
    await _mk_collection(session, "EQdup")
    dup = Collection(name="Dup", contract_address="EQdup", source="manual")
    session.add(dup)
    # Ошибку может поднять и flush, и commit (SQLite откладывает unique до записи)
    with pytest.raises(IntegrityError):
        await session.flush()
        await session.commit()
    await session.rollback()


@pytest.mark.asyncio
async def test_attribute_check_constraint(session: AsyncSession) -> None:
    c = await _mk_collection(session, "EQattr")
    a = Attribute(collection_id=c.id, type="model", value="Disco Bear")
    session.add(a)
    await session.flush()
    bad = Attribute(collection_id=c.id, type="sound", value="meow")  # вне enum
    session.add(bad)
    with pytest.raises(IntegrityError):
        await session.flush()
    await session.rollback()


@pytest.mark.asyncio
async def test_user_and_filter(session: AsyncSession) -> None:
    c = await _mk_collection(session, "EQflt")
    u = User(tg_id=123456789, username="wimp", first_name="Vadim")
    session.add(u)
    f = Filter(user_id=u.tg_id, collection_id=c.id, floor_min=Decimal("1"), floor_max=Decimal("50"))
    session.add(f)
    await session.flush()

    fetched = (await session.execute(select(Filter).where(Filter.user_id == u.tg_id))).scalar_one()
    assert fetched.is_active is True
    assert u.is_subscribed() is True  # free-план всегда активен

    paid = User(tg_id=999, sub_plan="pro", sub_expires_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1))
    session.add(paid)
    await session.flush()
    assert paid.is_subscribed() is False  # просрочено


@pytest.mark.asyncio
async def test_wallet_tg_map_and_listener_state(session: AsyncSession) -> None:
    w = WalletTgMap(ton_address="EQowner", tg_user_id=42, username="trubar", source="ton_dns", confidence=90)
    s = ListenerState(key="mint:EQcoll", last_index=1337)
    session.add_all([w, s])
    await session.flush()
    assert (await session.get(WalletTgMap, "EQowner")).confidence == 90
    assert (await session.get(ListenerState, "mint:EQcoll")).last_index == 1337


@pytest.mark.asyncio
async def test_model_sale(session: AsyncSession) -> None:
    c = await _mk_collection(session, "EQsale")
    sale = ModelSale(
        collection_id=c.id, model="Hamster Ball", price=Decimal("3503.5"),
        marketplace="getgems", nft_index=8681, nft_address="EQnft8681",
    )
    session.add(sale)
    await session.flush()
    row = (await session.execute(select(ModelSale).where(ModelSale.collection_id == c.id))).scalar_one()
    # SQLite не отдаёт tzinfo из CURRENT_TIMESTAMP — проверяем только факт автозаполнения
    assert row.sold_at is not None
