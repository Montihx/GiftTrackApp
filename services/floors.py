# services/floors.py — запись floor коллекции + снапшот при изменении (общее для updater и TonAPI-sync)
from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Collection, FloorSnapshot


async def apply_collection_floor(
    session: AsyncSession, coll_id: uuid.UUID, floor: Decimal, now: dt.datetime | None = None
) -> bool:
    """UPDATE floor по PK; при изменении пишет FloorSnapshot. True — floor изменился."""
    tbl = Collection.__table__
    prev = (await session.execute(select(tbl.c.floor_price).where(tbl.c.id == coll_id))).scalar_one_or_none()
    changed = prev is None or Decimal(str(prev)) != floor
    if changed:
        session.add(FloorSnapshot(collection_id=coll_id, price=floor, ts=now or dt.datetime.now(dt.timezone.utc)))
    await session.execute(update(tbl).where(tbl.c.id == coll_id).values(floor_price=floor, last_updated=func.now()))
    return changed
