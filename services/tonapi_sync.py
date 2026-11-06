# services/tonapi_sync.py — синхронизация коллекций через TonAPI (primary при наличии TONAPI_KEY)
"""
У TonAPI нет потока листингов/продаж, поэтому работаем по снимкам: каждые tonapi_poll_interval
секунд читаем ВСЕ items коллекции и сравниваем с прошлым снимком (Redis):
  * новый предмет   -> processed_nfts (+ история владения);
  * листинг         -> событие в STREAM_NFT_LISTINGS (для dispatcher);
  * продажа         -> Sale: предмет был в листинге и «эффективный владелец» сменился, цена = цена листинга;
  * по всем items   -> floor коллекции (+FloorSnapshot), items_count, attributes (count/floor).
Эффективный владелец: пока предмет в листинге, owner в TonAPI — контракт sale, продавец в sale.owner.
Ограничения: коллекции больше tonapi_floor_scan_limit пропускаются (неполный скан = неверный floor);
время листинга/продажи — момент обнаружения (точность = интервал опроса).
"""
from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.redis_client import STREAM_NFT_LISTINGS, cache_get_json, cache_set_json, floor_key, stream_publish
from db.models import Attribute, Collection, NftOwnershipHistory, ProcessedNft, Sale
from services.floors import apply_collection_floor
from services.market.events import ListingEvent, listing_to_stream_payload
from services.tonapi import TonApiClient, parse_ton_price

STATE_KEY = "tonapi_sync:{collection}"
STATE_TTL = 7 * 86400
TRAIT_MAP = {"model": "model", "backdrop": "background", "background": "background",
             "symbol": "symbol", "pattern": "pattern"}


@dataclass(frozen=True, slots=True)
class ItemState:
    address: str
    index: int
    owner: str | None
    price: Decimal | None
    attrs: dict[str, str]
    market: str = "tonapi"


@dataclass
class SyncDiff:
    new_items: list[ItemState] = field(default_factory=list)
    listings: list[ItemState] = field(default_factory=list)
    sales: list[tuple[ItemState, Decimal]] = field(default_factory=list)
    transfers: list[tuple[ItemState, str | None]] = field(default_factory=list)


def parse_item(raw: dict[str, Any]) -> ItemState | None:
    address = raw.get("address")
    if not address:
        return None
    sale = raw.get("sale") or None
    owner = ((sale or {}).get("owner") or {}).get("address") if sale else None
    owner = owner or (raw.get("owner") or {}).get("address")
    attrs: dict[str, str] = {}
    for a in (raw.get("metadata") or {}).get("attributes") or []:
        trait = TRAIT_MAP.get(str(a.get("trait_type") or a.get("name") or "").lower())
        value = a.get("value")
        if trait and value not in (None, ""):
            attrs[trait] = str(value)
    try:
        index = int(raw.get("index") or 0)
    except (TypeError, ValueError):
        index = 0
    market = (((sale or {}).get("market") or {}).get("name") or "tonapi").lower() if sale else "tonapi"
    return ItemState(address, index, owner, parse_ton_price(sale) if sale else None, attrs, market)


def to_state(items: list[ItemState]) -> dict[str, list[Any]]:
    return {i.address: [i.owner, str(i.price) if i.price is not None else None] for i in items}


def diff_states(prev: dict[str, list[Any]] | None, cur: list[ItemState]) -> SyncDiff:
    """prev=None — первый запуск: только new_items, без событий."""
    diff = SyncDiff()
    if prev is None:
        diff.new_items = list(cur)
        return diff
    for c in cur:
        p = prev.get(c.address)
        if p is None:
            diff.new_items.append(c)
            if c.price is not None:
                diff.listings.append(c)
            continue
        prev_price = Decimal(p[1]) if p[1] is not None else None
        owner_changed = c.owner is not None and c.owner != p[0]
        if owner_changed:
            diff.transfers.append((c, p[0]))
            if prev_price is not None:
                diff.sales.append((c, prev_price))
        if c.price is not None and (prev_price is None or owner_changed):
            diff.listings.append(c)
    return diff


class TonApiSync:
    def __init__(self, session: AsyncSession, redis: Any, settings: Settings,
                 client: TonApiClient | None = None) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings
        self.client = client or TonApiClient(settings)
        self.stats = {"listings": 0, "sales": 0, "transfers": 0, "skipped": 0, "errors": 0}

    async def process_collections(self) -> None:
        rows = (await self.session.execute(
            select(Collection.id, Collection.name, Collection.contract_address)
            .where(~Collection.contract_address.startswith("tg_gift:"))
        )).all()
        for coll_id, name, address in rows:
            try:
                await self.sync_collection(coll_id, name, address)
            except Exception as exc:  # noqa: BLE001 — одна коллекция не роняет цикл
                self.stats["errors"] += 1
                logger.warning(f"[tonapi_sync] {name}: {exc!r}")
                await self.session.rollback()

    async def sync_collection(self, coll_id: uuid.UUID, name: str, address: str,
                              now: dt.datetime | None = None) -> SyncDiff | None:
        now = now or dt.datetime.now(dt.timezone.utc)
        raw_items = await self.client.fetch_collection_items(address)
        if raw_items is None:
            self.stats["skipped"] += 1
            return None
        items = [i for i in (parse_item(r) for r in raw_items) if i is not None]
        cid = str(coll_id)
        state_key = STATE_KEY.format(collection=cid)
        prev = await cache_get_json(self.redis, state_key)
        diff = diff_states(prev if isinstance(prev, dict) else None, items)

        await self._persist(coll_id, items, diff, now)
        await self.session.commit()  # сначала БД, потом снимок в Redis — иначе при сбое события потеряются
        await cache_set_json(self.redis, state_key, to_state(items), ttl=STATE_TTL)

        for item in diff.listings:
            event = ListingEvent(
                nft_address=item.address, nft_index=item.index, collection_address=address,
                marketplace=item.market, price_ton=float(item.price or 0), seller_ton=item.owner or "",
                attributes=dict(item.attrs), listed_at=now,
            )
            payload = listing_to_stream_payload(event)
            payload["collection_id"] = cid
            await stream_publish(self.redis, STREAM_NFT_LISTINGS, payload)
        self.stats["listings"] += len(diff.listings)
        self.stats["sales"] += len(diff.sales)
        self.stats["transfers"] += len(diff.transfers)
        if diff.listings or diff.sales:
            logger.info(f"[tonapi_sync] {name}: листингов={len(diff.listings)}, продаж={len(diff.sales)}")
        return diff

    async def _persist(self, coll_id: uuid.UUID, items: list[ItemState], diff: SyncDiff,
                       now: dt.datetime) -> None:
        s = self.session
        ttl = self.settings.floor_cache_ttl

        touched = list(dict.fromkeys([i.address for i in diff.new_items] + [i.address for i, _ in diff.transfers]))
        existing: dict[str, ProcessedNft] = {}
        for k in range(0, len(touched), 1000):
            res = await s.execute(select(ProcessedNft).where(ProcessedNft.nft_address.in_(touched[k:k + 1000])))
            existing.update({n.nft_address: n for n in res.scalars().all()})
        for item in diff.new_items:
            nft = existing.get(item.address)
            if nft is None:
                s.add(ProcessedNft(
                    nft_address=item.address, collection_id=coll_id, nft_index=item.index,
                    model=item.attrs.get("model"), background=item.attrs.get("background"),
                    symbol=item.attrs.get("symbol"), pattern=item.attrs.get("pattern"),
                    current_owner_ton=item.owner))
                if item.owner:
                    s.add(NftOwnershipHistory(nft_address=item.address, owner_ton=item.owner, acquired_at=now))
            else:  # запись от старого listener-ton без коллекции/трейтов — дополняем
                nft.collection_id = coll_id
                nft.nft_index = item.index
                nft.model, nft.background = item.attrs.get("model"), item.attrs.get("background")
                nft.symbol, nft.pattern = item.attrs.get("symbol"), item.attrs.get("pattern")
                nft.current_owner_ton = item.owner
        await s.flush()
        new_addrs = {i.address for i in diff.new_items}
        for item, _prev in diff.transfers:
            nft = existing.get(item.address)
            if nft is not None:
                nft.current_owner_ton = item.owner
            if item.address not in new_addrs and item.owner:
                s.add(NftOwnershipHistory(nft_address=item.address, owner_ton=item.owner, acquired_at=now))

        for item, price in diff.sales:
            await s.execute(pg_insert(Sale.__table__)
                            .values(collection_id=coll_id, item_address=item.address, price=price, ts=now)
                            .on_conflict_do_nothing())

        counts: dict[tuple[str, str], int] = {}
        floors: dict[tuple[str, str], Decimal] = {}
        for i in items:
            for t, v in i.attrs.items():
                counts[(t, v)] = counts.get((t, v), 0) + 1
                if i.price is not None and ((t, v) not in floors or i.price < floors[(t, v)]):
                    floors[(t, v)] = i.price
        attrs = {(a.type, a.value): a for a in
                 (await s.execute(select(Attribute).where(Attribute.collection_id == coll_id))).scalars().all()}
        for key, cnt in counts.items():
            a = attrs.get(key)
            if a is None:
                a = Attribute(collection_id=coll_id, type=key[0], value=key[1], items_count=cnt)
                s.add(a)
                attrs[key] = a
            elif a.items_count != cnt:
                a.items_count = cnt
            fl = floors.get(key)
            if fl is not None and (a.floor_price is None or Decimal(str(a.floor_price)) != fl):
                a.floor_price = fl
            if key[0] == "model" and fl is not None:
                await cache_set_json(self.redis, floor_key(str(coll_id), key[1]), float(fl), ttl=ttl)

        coll = await s.get(Collection, coll_id)
        if coll is not None and coll.items_count != len(items):
            coll.items_count = len(items)
        listed = [i.price for i in items if i.price is not None]
        if listed:
            floor = min(listed)
            await apply_collection_floor(s, coll_id, floor, now)
            await cache_set_json(self.redis, floor_key(str(coll_id)), float(floor), ttl=ttl)
