# services/tonapi_sync.py — синхронизация коллекций через TonAPI (primary при наличии TONAPI_KEY)
"""
У TonAPI нет потока листингов/продаж, поэтому работаем по снимкам: читаем items коллекции
постранично и сравниваем с прошлым состоянием предмета (Redis-хэш на коллекцию):
  * новый предмет   -> processed_nfts (+ история владения);
  * листинг         -> событие в STREAM_NFT_LISTINGS (для dispatcher, уведомление «ЛИСТИНГ»);
  * продажа         -> Sale + событие в STREAM_NFT_SALES (уведомление «GIFT SOLD!»):
                       предмет был в листинге и «эффективный владелец» сменился, цена = цена листинга;
  * по завершении прохода -> floor коллекции (+FloorSnapshot), items_count, attributes (count/floor).

Постраничный скан (A4): за цикл читаем не больше tonapi_pages_per_cycle страниц, курсор хранится
в Redis. Коллекция до pages*1000 items проходится целиком каждый цикл; большая — за несколько
циклов (floor обновляется раз в проход, т.е. с задержкой ~ceil(items/1000/pages) циклов).
Первый проход — «bootstrap»: события не публикуются, только наполняется состояние.
Предметы, не встреченные за проход (сожжены/ушли из выдачи), удаляются из состояния.

Эффективный владелец: пока предмет в листинге, owner в TonAPI — контракт sale, продавец в sale.owner.
Предметы, ещё не выведенные в блокчейн, принадлежат «Fragment Gift Minter» — владелец неизвестен (None).
Номер подарка берём из metadata.name («Plush Pepe #2675»): поле index у Telegram-подарков — хэш.
Время листинга/продажи — момент обнаружения (точность = длительность прохода).
"""
from __future__ import annotations

import datetime as dt
import re
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import orjson
from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.redis_client import (
    STREAM_NFT_LISTINGS,
    STREAM_NFT_SALES,
    cache_set_json,
    floor_key,
    stream_publish,
)
from db.models import Attribute, Collection, NftOwnershipHistory, ProcessedNft, Sale
from services.floors import apply_collection_floor
from services.market.events import ListingEvent, listing_to_stream_payload
from services.tonapi import PAGE_LIMIT, TonApiClient, parse_ton_price

ITEMS_KEY = "tonapi_sync:{cid}:items"    # hash: nft_address -> orjson ItemState + номер прохода
CURSOR_KEY = "tonapi_sync:{cid}:cursor"  # offset следующей страницы
PASS_KEY = "tonapi_sync:{cid}:pass"      # номер текущего прохода
READY_KEY = "tonapi_sync:{cid}:ready"    # первый полный проход завершён -> публикуем события
STATE_TTL = 7 * 86400
TRAIT_MAP = {"model": "model", "backdrop": "background", "background": "background",
             "symbol": "symbol", "pattern": "pattern"}
MINTER_NAMES = {"fragment gift minter"}
INT64_MAX = 2**63 - 1
_NUM_RE = re.compile(r"#\s*(\d+)\s*$")
_MARKETS = ("getgems", "marketapp", "fragment", "portals", "tonnel", "mrkt", "xgift")


def normalize_market(name: str | None) -> str:
    """'Getgems Sales' -> 'getgems', 'Marketapp Marketplace' -> 'marketapp'."""
    low = (name or "").strip().lower()
    if not low:
        return "tonapi"
    for m in _MARKETS:
        if m in low:
            return m
    return low.split()[0]


@dataclass(frozen=True, slots=True)
class ItemState:
    address: str
    index: int
    owner: str | None
    price: Decimal | None
    attrs: dict[str, str]
    market: str = "tonapi"
    name: str | None = None


@dataclass
class SyncDiff:
    new_items: list[ItemState] = field(default_factory=list)
    listings: list[ItemState] = field(default_factory=list)
    # (текущее состояние, цена продажи, прежнее состояние — продавец и маркет листинга)
    sales: list[tuple[ItemState, Decimal, ItemState]] = field(default_factory=list)
    transfers: list[tuple[ItemState, str | None]] = field(default_factory=list)


def _owner(acc: dict[str, Any] | None) -> str | None:
    if not acc or not acc.get("address"):
        return None
    if str(acc.get("name") or "").strip().lower() in MINTER_NAMES:
        return None
    return acc["address"]


def _item_number(raw: dict[str, Any]) -> int:
    m = _NUM_RE.search(str((raw.get("metadata") or {}).get("name") or ""))
    if m:
        return int(m.group(1))
    try:
        index = int(raw.get("index") or 0)
    except (TypeError, ValueError):
        return 0
    return index if -INT64_MAX <= index <= INT64_MAX else 0


def parse_item(raw: dict[str, Any]) -> ItemState | None:
    address = raw.get("address")
    if not address:
        return None
    sale = raw.get("sale") or None
    owner = _owner((sale or {}).get("owner")) if sale else None
    owner = owner or _owner(raw.get("owner"))
    attrs: dict[str, str] = {}
    for a in (raw.get("metadata") or {}).get("attributes") or []:
        trait = TRAIT_MAP.get(str(a.get("trait_type") or a.get("name") or "").lower())
        value = a.get("value")
        if trait and value not in (None, ""):
            attrs[trait] = str(value)
    market = normalize_market(((sale or {}).get("market") or {}).get("name")) if sale else "tonapi"
    name = (raw.get("metadata") or {}).get("name") or None
    return ItemState(address, _item_number(raw), owner, parse_ton_price(sale) if sale else None,
                     attrs, market, name)


def encode_state(item: ItemState, pass_no: int) -> bytes:
    return orjson.dumps([item.owner, str(item.price) if item.price is not None else None,
                         item.index, item.attrs, item.market, item.name, pass_no])


def decode_state(address: str, raw: bytes | str | None) -> tuple[ItemState, int] | None:
    if raw is None:
        return None
    try:
        owner, price, index, attrs, market, name, pass_no = orjson.loads(raw)
        return ItemState(address, int(index), owner, Decimal(price) if price is not None else None,
                         dict(attrs), market, name), int(pass_no)
    except Exception:  # noqa: BLE001 — битая/старая запись = «не видели»
        return None


def to_state(items: list[ItemState]) -> dict[str, ItemState]:
    return {i.address: i for i in items}


def diff_states(prev: dict[str, ItemState] | None, cur: list[ItemState]) -> SyncDiff:
    """prev=None — bootstrap: только new_items, без событий."""
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
        owner_changed = c.owner is not None and c.owner != p.owner
        if owner_changed:
            diff.transfers.append((c, p.owner))
            if p.price is not None:
                diff.sales.append((c, p.price, p))
        if c.price is not None and (p.price != c.price or owner_changed):
            diff.listings.append(c)  # новый листинг или перевыставление по другой цене
    return diff


def _s(v: bytes | str | None) -> str | None:
    return v.decode() if isinstance(v, bytes) else v


class TonApiSync:
    def __init__(self, session: AsyncSession, redis: Any, settings: Settings,
                 client: TonApiClient | None = None, page_limit: int = PAGE_LIMIT) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings
        self.client = client or TonApiClient(settings)
        self.page_limit = page_limit
        self.stats = {"listings": 0, "sales": 0, "transfers": 0, "passes": 0, "errors": 0}

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

    async def _fetch_chunk(self, address: str, cursor: int) -> tuple[list[dict[str, Any]], int, bool]:
        """До tonapi_pages_per_cycle страниц с cursor. -> (items, следующий offset, проход завершён)."""
        raw: list[dict[str, Any]] = []
        offset = cursor
        for _ in range(self.settings.tonapi_pages_per_cycle):
            batch = await self.client.fetch_items_page(address, offset, self.page_limit)
            raw.extend(batch)
            offset += len(batch)
            if len(batch) < self.page_limit:
                return raw, offset, True
        return raw, offset, False

    async def sync_collection(self, coll_id: uuid.UUID, name: str, address: str,
                              now: dt.datetime | None = None) -> SyncDiff:
        now = now or dt.datetime.now(dt.timezone.utc)
        cid = str(coll_id)
        keys = {k: tpl.format(cid=cid) for k, tpl in
                (("items", ITEMS_KEY), ("cursor", CURSOR_KEY), ("pass", PASS_KEY), ("ready", READY_KEY))}
        cursor = int(_s(await self.redis.get(keys["cursor"])) or 0)
        pass_no = int(_s(await self.redis.get(keys["pass"])) or 0)
        ready = await self.redis.get(keys["ready"]) is not None

        raw_items, next_offset, complete = await self._fetch_chunk(address, cursor)
        items = list(to_state([i for i in (parse_item(r) for r in raw_items) if i is not None]).values())

        prev: dict[str, ItemState] = {}
        if items:
            values = await self.redis.hmget(keys["items"], [i.address for i in items])
            for item, raw in zip(items, values):
                decoded = decode_state(item.address, raw)
                if decoded is not None:
                    prev[item.address] = decoded[0]
        diff = diff_states(prev if ready else None, items)

        await self._persist_chunk(coll_id, diff, now)
        full: dict[str, ItemState] | None = None
        stale: list[str] = []
        if complete:
            full = {}
            for addr, raw in (await self.redis.hgetall(keys["items"])).items():
                a = _s(addr) or ""
                decoded = decode_state(a, raw)
                if decoded is not None and decoded[1] == pass_no:
                    full[a] = decoded[0]
                else:
                    stale.append(a)
            full.update(to_state(items))
            stale = [a for a in stale if a not in full]
            await self._persist_aggregates(coll_id, list(full.values()), now)
        await self.session.commit()  # сначала БД, потом состояние в Redis — иначе при сбое события потеряются

        if items:
            await self.redis.hset(keys["items"], mapping={i.address: encode_state(i, pass_no) for i in items})
        if stale:
            await self.redis.hdel(keys["items"], *stale)
        await self.redis.expire(keys["items"], STATE_TTL)
        if complete:
            await self.redis.set(keys["cursor"], b"0", ex=STATE_TTL)
            await self.redis.set(keys["pass"], str(pass_no + 1).encode(), ex=STATE_TTL)
            await self.redis.set(keys["ready"], b"1", ex=STATE_TTL)
            self.stats["passes"] += 1
        else:
            await self.redis.set(keys["cursor"], str(next_offset).encode(), ex=STATE_TTL)

        await self._publish(cid, name, address, diff, now)
        if diff.listings or diff.sales:
            logger.info(f"[tonapi_sync] {name}: листингов={len(diff.listings)}, продаж={len(diff.sales)}")
        if complete and not ready:
            logger.info(f"[tonapi_sync] {name}: первый проход завершён, items={len(full or {})}")
        return diff

    async def _publish(self, cid: str, name: str, address: str, diff: SyncDiff, now: dt.datetime) -> None:
        for item in diff.listings:
            event = ListingEvent(
                nft_address=item.address, nft_index=item.index, collection_address=address,
                marketplace=item.market, price_ton=float(item.price or 0), seller_ton=item.owner or "",
                attributes=dict(item.attrs), listed_at=now,
            )
            payload = listing_to_stream_payload(event)
            payload["collection_id"] = cid
            await stream_publish(self.redis, STREAM_NFT_LISTINGS, payload)
        for item, price, prev in diff.sales:
            await stream_publish(self.redis, STREAM_NFT_SALES, {
                "type": "sale", "collection_id": cid, "collection_name": name,
                "collection_address": address, "nft_address": item.address, "nft_index": item.index,
                "nft_name": item.name, "price_ton": float(price), "marketplace": prev.market,
                "seller_ton": prev.owner or "", "buyer_ton": item.owner or "",
                "attributes": dict(item.attrs or prev.attrs), "sold_at": now.isoformat(),
            })
        self.stats["listings"] += len(diff.listings)
        self.stats["sales"] += len(diff.sales)
        self.stats["transfers"] += len(diff.transfers)

    async def _persist_chunk(self, coll_id: uuid.UUID, diff: SyncDiff, now: dt.datetime) -> None:
        s = self.session
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

        for item, price, _p in diff.sales:
            await s.execute(pg_insert(Sale.__table__)
                            .values(collection_id=coll_id, item_address=item.address, price=price, ts=now)
                            .on_conflict_do_nothing())

    async def _persist_aggregates(self, coll_id: uuid.UUID, items: list[ItemState], now: dt.datetime) -> None:
        """Floor коллекции/моделей, items_count, attributes — по полному проходу."""
        s = self.session
        ttl = self.settings.floor_cache_ttl
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
            cur = Decimal(str(a.floor_price)) if a.floor_price is not None else None
            if fl is not None and cur != fl:
                a.floor_price = fl
            elif fl is None and cur:
                a.floor_price = Decimal(0)  # листингов больше нет; 0 = «нет floor» (server_default схемы)
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
