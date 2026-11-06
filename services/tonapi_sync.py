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

import asyncio
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
    STREAM_NFT_UPGRADES,
    cache_get_json,
    cache_set_json,
    floor_key,
    recent_sales_key,
    stream_publish,
)
from db.models import Attribute, Collection, Favorite, Filter, NftOwnershipHistory, ProcessedNft, Sale
from services.floors import apply_collection_floor
from services.gift_discovery import GiftCollectionDiscovery
from services.market.events import ListingEvent, listing_to_stream_payload
from services.tonapi import PAGE_LIMIT, TonApiClient, parse_ton_price

ITEMS_KEY = "tonapi_sync:{cid}:items"    # hash: nft_address -> orjson ItemState + номер прохода
CURSOR_KEY = "tonapi_sync:{cid}:cursor"  # offset следующей страницы
PASS_KEY = "tonapi_sync:{cid}:pass"      # номер текущего прохода
READY_KEY = "tonapi_sync:{cid}:ready"    # первый полный проход завершён -> публикуем события
STATE_TTL = 7 * 86400
RATE_KEY = "rate:ton_usd"
RATE_TTL = 300
RECENT_SALES_LIMIT = 10
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
    # Нет листинга, а владелец — контракт (не кошелёк): TonAPI не распознал контракт продажи или
    # предмет в процессе листинга. Такой снимок не порождает событий, прошлое состояние сохраняется.
    uncertain: bool = False


@dataclass
class SyncDiff:
    new_items: list[ItemState] = field(default_factory=list)
    listings: list[ItemState] = field(default_factory=list)
    # (текущее состояние, цена продажи, прежнее состояние — продавец и маркет листинга)
    sales: list[tuple[ItemState, Decimal, ItemState]] = field(default_factory=list)
    transfers: list[tuple[ItemState, str | None]] = field(default_factory=list)
    bootstrap: bool = False  # первый проход: new_items — это всё содержимое коллекции, не апгрейды


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
    raw_owner = raw.get("owner") or {}
    owner = _owner((sale or {}).get("owner")) if sale else None
    owner = owner or _owner(raw_owner)
    uncertain = not sale and owner is not None and raw_owner.get("is_wallet") is False
    attrs: dict[str, str] = {}
    for a in (raw.get("metadata") or {}).get("attributes") or []:
        trait = TRAIT_MAP.get(str(a.get("trait_type") or a.get("name") or "").lower())
        value = a.get("value")
        if trait and value not in (None, ""):
            attrs[trait] = str(value)
    market = normalize_market(((sale or {}).get("market") or {}).get("name")) if sale else "tonapi"
    name = (raw.get("metadata") or {}).get("name") or None
    return ItemState(address, _item_number(raw), owner, parse_ton_price(sale) if sale else None,
                     attrs, market, name, uncertain)


_ATTR_ORDER = ("model", "background", "symbol", "pattern")


def encode_state(item: ItemState, pass_no: int) -> bytes:
    """Компактно (~1,5 млн записей в Redis): имя не храним — оно = «<коллекция> #<номер>»."""
    return orjson.dumps([item.owner, str(item.price) if item.price is not None else None, item.index,
                         [item.attrs.get(k) for k in _ATTR_ORDER], item.market, pass_no])


def decode_state(address: str, raw: bytes | str | None) -> tuple[ItemState, int] | None:
    if raw is None:
        return None
    try:
        data = orjson.loads(raw)
        if len(data) == 7:  # старый формат: [..., attrs: dict, market, name, pass]
            owner, price, index, attrs, market, _name, pass_no = data
        else:
            owner, price, index, values, market, pass_no = data
            attrs = {k: v for k, v in zip(_ATTR_ORDER, values, strict=False) if v}
        return ItemState(address, int(index), owner, Decimal(price) if price is not None else None,
                         dict(attrs), market), int(pass_no)
    except Exception:  # noqa: BLE001 — битая/старая запись = «не видели»
        return None


def to_state(items: list[ItemState]) -> dict[str, ItemState]:
    return {i.address: i for i in items}


def diff_states(prev: dict[str, ItemState] | None, cur: list[ItemState]) -> SyncDiff:
    """prev=None — bootstrap: только new_items, без событий."""
    diff = SyncDiff()
    if prev is None:
        diff.new_items = list(cur)
        diff.bootstrap = True
        return diff
    for c in cur:
        p = prev.get(c.address)
        if c.uncertain:
            if p is None:
                diff.new_items.append(c)
            continue  # неопределённый снимок: без событий, прошлое состояние остаётся
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


def state_to_store(items: list[ItemState], prev: dict[str, ItemState]) -> list[ItemState]:
    """Для неопределённых снимков сохраняем прошлое состояние (если было)."""
    return [prev.get(i.address, i) if i.uncertain else i for i in items]


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
        self.stats = {"listings": 0, "sales": 0, "transfers": 0, "passes": 0, "errors": 0,
                      "stream_items": 0}
        self.discovery = GiftCollectionDiscovery(session, redis, settings, self.client)
        # Одна AsyncSession на скан и поток (services/tonapi_stream.py): работа с БД/состоянием — под замком
        self.lock = asyncio.Lock()
        self._cycle = 0

    async def _hot_collections(self) -> set[uuid.UUID]:
        """Коллекции, на которые у пользователей есть активные фильтры или избранное."""
        rows = await self.session.execute(
            select(Filter.collection_id).where(Filter.is_active.is_(True), Filter.collection_id.is_not(None))
            .union(select(Favorite.collection_id).where(Favorite.collection_id.is_not(None)))
        )
        return {r[0] for r in rows.all()}

    async def process_collections(self) -> None:
        self.discovery.client = self.client  # один клиент (и rate limiter) на sync и discovery
        async with self.lock:
            await self.discovery.maybe_run()  # новые коллекции Telegram Gifts -> БД (раз в interval)
            rows = (await self.session.execute(
                select(Collection.id, Collection.name, Collection.contract_address)
                .where(~Collection.contract_address.startswith("tg_gift:"))
            )).all()
            hot = await self._hot_collections()
        # Приоритет: «горячие» коллекции — каждый цикл и первыми; остальные — раз в
        # tonapi_cold_every циклов (события по ним всё равно приходят из потока в реальном времени).
        self._cycle += 1
        every = max(1, self.settings.tonapi_cold_every)
        rows = sorted(rows, key=lambda r: r[0] not in hot)
        for coll_id, name, address in rows:
            if coll_id not in hot and (self._cycle - 1) % every != 0:
                continue
            try:
                await self.sync_collection(coll_id, name, address)
            except Exception as exc:  # noqa: BLE001 — одна коллекция не роняет цикл
                self.stats["errors"] += 1
                logger.warning(f"[tonapi_sync] {name}: {exc!r}")
                async with self.lock:
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

        raw_items, next_offset, complete = await self._fetch_chunk(address, cursor)  # сеть — вне замка
        items = list(to_state([i for i in (parse_item(r) for r in raw_items) if i is not None]).values())
        async with self.lock:
            return await self._apply_chunk(coll_id, name, address, keys, items, pass_no, ready,
                                           next_offset, complete, now)

    async def _apply_chunk(self, coll_id: uuid.UUID, name: str, address: str, keys: dict[str, str],
                           items: list[ItemState], pass_no: int, ready: bool, next_offset: int,
                           complete: bool, now: dt.datetime) -> SyncDiff:
        cid = str(coll_id)
        prev: dict[str, ItemState] = {}
        if items:
            values = await self.redis.hmget(keys["items"], [i.address for i in items])
            for item, raw in zip(items, values, strict=True):
                decoded = decode_state(item.address, raw)
                if decoded is not None:
                    prev[item.address] = decoded[0]
        diff = diff_states(prev if ready else None, items)
        stored = state_to_store(items, prev)

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
            full.update(to_state(stored))
            stale = [a for a in stale if a not in full]
            await self._persist_aggregates(coll_id, list(full.values()), now)
        await self.session.commit()  # сначала БД, потом состояние в Redis — иначе при сбое события потеряются

        if stored:
            await self.redis.hset(keys["items"], mapping={i.address: encode_state(i, pass_no) for i in stored})
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

    async def apply_item(self, raw: dict[str, Any], now: dt.datetime | None = None) -> SyncDiff | None:
        """Событие из потока (services/tonapi_stream.py): одно NFT по GET /v2/nfts/{addr}.

        Только для коллекций, прошедших первый проход (иначе нечего сравнивать). Флор коллекции
        и модели снижается сразу, если новый листинг дешевле; рост флора — по завершении прохода.
        """
        now = now or dt.datetime.now(dt.timezone.utc)
        item = parse_item(raw)
        coll_addr = ((raw.get("collection") or {}).get("address")) or ""
        if item is None or not coll_addr:
            return None
        async with self.lock:
            coll = await self._collection_by_raw(coll_addr)
            if coll is None:
                return None
            cid = str(coll.id)
            keys = {k: tpl.format(cid=cid) for k, tpl in
                    (("items", ITEMS_KEY), ("pass", PASS_KEY), ("ready", READY_KEY))}
            if await self.redis.get(keys["ready"]) is None:
                return None
            pass_no = int(_s(await self.redis.get(keys["pass"])) or 0)
            decoded = decode_state(item.address, (await self.redis.hmget(keys["items"], [item.address]))[0])
            prev = {item.address: decoded[0]} if decoded is not None else {}
            diff = diff_states(prev, [item])
            stored = state_to_store([item], prev)
            await self._persist_chunk(coll.id, diff, now)
            if item.price is not None and not item.uncertain:
                await self._lower_floors(coll, item, now)
            await self.session.commit()
            await self.redis.hset(keys["items"], mapping={i.address: encode_state(i, pass_no) for i in stored})
            await self._publish(cid, coll.name, coll.contract_address, diff, now)
            self.stats["stream_items"] += 1
            return diff

    async def _collection_by_raw(self, raw_addr: str) -> Collection | None:
        from services.gift_discovery import raw_address

        target = raw_address(raw_addr)
        if not getattr(self, "_coll_cache", None) or target not in self._coll_cache:
            rows = (await self.session.execute(select(Collection))).scalars().all()
            self._coll_cache = {raw_address(c.contract_address): c.id for c in rows
                                if not c.contract_address.startswith("tg_gift:")}
        cid = self._coll_cache.get(target)
        return await self.session.get(Collection, cid) if cid is not None else None

    async def _lower_floors(self, coll: Collection, item: ItemState, now: dt.datetime) -> None:
        ttl = self.settings.floor_cache_ttl
        price = item.price
        assert price is not None
        if coll.floor_price is None or price < Decimal(str(coll.floor_price)):
            await apply_collection_floor(self.session, coll.id, price, now)
            await cache_set_json(self.redis, floor_key(str(coll.id)), float(price), ttl=ttl)
        model = item.attrs.get("model")
        if model:
            attr = (await self.session.execute(select(Attribute).where(
                Attribute.collection_id == coll.id, Attribute.type == "model", Attribute.value == model
            ))).scalar_one_or_none()
            if attr is not None and (not attr.floor_price or price < Decimal(str(attr.floor_price))):
                attr.floor_price = price
                await cache_set_json(self.redis, floor_key(str(coll.id), model), float(price), ttl=ttl)

    async def is_tracked_item(self, nft_address: str) -> bool:
        async with self.lock:
            return (await self.session.get(ProcessedNft, nft_address)) is not None

    async def _publish(self, cid: str, name: str, address: str, diff: SyncDiff, now: dt.datetime) -> None:
        for item in (diff.listings if self.settings.onchain_listings_enabled else []):
            event = ListingEvent(
                nft_address=item.address, nft_index=item.index, collection_address=address,
                marketplace=item.market, price_ton=float(item.price or 0), seller_ton=item.owner or "",
                attributes=dict(item.attrs), listed_at=now,
            )
            payload = listing_to_stream_payload(event)
            payload["collection_id"] = cid
            payload["nft_name"] = item.name or f"{name} #{item.index}"
            await stream_publish(self.redis, STREAM_NFT_LISTINGS, payload)
        # Апгрейды: предмет, которого не было в прошлых проходах (после первого прохода коллекции).
        # Пачка больше upgrade_burst_max за раз — не апгрейды, а сбой состояния: не рассылаем.
        if self.settings.onchain_upgrades_enabled and not diff.bootstrap and diff.new_items:
            if len(diff.new_items) > self.settings.upgrade_burst_max:
                logger.warning(f"[tonapi_sync] {name}: {len(diff.new_items)} новых предметов за раз — апгрейды не шлю")
            else:
                for item in diff.new_items:
                    await stream_publish(self.redis, STREAM_NFT_UPGRADES, {
                        "type": "mint", "collection_id": cid, "nft_address": item.address, "index": item.index,
                        "owner_ton": item.owner or "",
                        "metadata": {"name": item.name or f"{name} #{item.index}", **item.attrs},
                    })
                self.stats["upgrades"] = self.stats.get("upgrades", 0) + len(diff.new_items)
        sales = diff.sales if self.settings.onchain_sales_enabled else []
        ton_usd = await self._ton_usd() if sales else None
        for item, price, prev in sales:
            attrs = dict(item.attrs or prev.attrs)
            sale = {
                "type": "sale", "collection_id": cid, "collection_name": name,
                "collection_address": address, "nft_address": item.address, "nft_index": item.index,
                "nft_name": item.name or f"{name} #{item.index}", "price_ton": float(price),
                "price_usd": round(float(price) * ton_usd, 2) if ton_usd else None,
                "marketplace": prev.market, "seller_ton": prev.owner or "", "buyer_ton": item.owner or "",
                "attributes": attrs, "sold_at": now.isoformat(),
            }
            await stream_publish(self.redis, STREAM_NFT_SALES, sale)
            if attrs.get("model"):
                await self._push_recent_sale(cid, attrs["model"], sale)
        self.stats["listings"] += len(diff.listings)
        self.stats["sales"] += len(diff.sales)
        self.stats["transfers"] += len(diff.transfers)

    async def _ton_usd(self) -> float | None:
        cached = await cache_get_json(self.redis, RATE_KEY)
        if isinstance(cached, (int, float)) and cached > 0:
            return float(cached)
        fetch = getattr(self.client, "fetch_ton_usd", None)
        rate = await fetch() if fetch is not None else None
        if rate:
            await cache_set_json(self.redis, RATE_KEY, rate, ttl=RATE_TTL)
        return rate

    async def _push_recent_sale(self, cid: str, model: str, sale: dict[str, Any]) -> None:
        """recent_sales:{cid}:{model} — для блока «Последние продажи модели» в уведомлении ЛИСТИНГ."""
        key = recent_sales_key(cid, model)
        current = await cache_get_json(self.redis, key)
        entry = {"nft_index": sale["nft_index"], "price": sale["price_ton"],
                 "marketplace": sale["marketplace"], "sold_at": sale["sold_at"]}
        items = [entry] + (current if isinstance(current, list) else [])
        await cache_set_json(self.redis, key, items[:RECENT_SALES_LIMIT], ttl=STATE_TTL)

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
            else:  # запись без коллекции/трейтов (создана до скана) — дополняем
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
