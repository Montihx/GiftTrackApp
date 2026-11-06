# services/market_listings.py — новые листинги на MRKT и Portals (приоритетные источники алертов «ЛИСТИНГ»)
"""
Те же эндпоинты, что и для флоров (services/market_floors.py: вход через мини-приложение, обычный HTTP, без обхода защиты),
но сортировка «новые сверху»: любой новый id = новый листинг -> событие в `nft_listings` с marketplace=mrkt|portals.

  MRKT:    POST /gifts/saling {ordering: null (по времени выставления), lowToHigh: false, count: 20, cursor}
  Portals: GET  /nfts/search?sort_by=listed_at desc&status=listed&limit=100&offset=…

Контакта продавца у этих маркетов нет (подарок на маркете, не у пользователя) — такие алерты получают только фильтры с
выключенной галочкой «Листинги: только если можно написать владельцу». Ответы неофициальные: разбор терпимый, первый ответ
каждого маркета пишется в лог. Первый опрос только запоминает текущие листинги (без событий); пачка больше
`MARKET_LISTINGS_MAX_NEW` за опрос — сбой состояния, не рассылаем.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import time
from typing import Any

from loguru import logger
from sqlalchemy import select

from core.config import Settings
from core.redis_client import STREAM_NFT_LISTINGS, stream_publish
from db.models import Collection
from services.market_floors import MarketBlocked, PORTALS_API, MRKT_API, extract_price
from services.owner_lookup import gift_slug
from services.telegram_market import norm_title

SEEN_KEY = "mkt_seen:{source}:{id}"
BOOT_KEY = "mkt_boot:{source}"
SEEN_TTL = 24 * 3600


def _first(item: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        v = item.get(k)
        if v not in (None, ""):
            return v
    return None


def parse_mrkt(item: dict[str, Any]) -> dict[str, Any] | None:
    price = extract_price(item)
    title = _first(item, "collectionTitle", "collectionName", "title")
    number = _first(item, "number")
    ident = _first(item, "id", "giftIdString")
    if not (price and title and number and ident):
        return None
    attrs = {k: str(v) for k, v in (("model", item.get("modelName")), ("background", item.get("backdropName")),
                                    ("symbol", item.get("symbolName"))) if v}
    return {"id": str(ident), "title": str(title), "number": int(number), "price": price, "attributes": attrs}


def parse_portals(item: dict[str, Any]) -> dict[str, Any] | None:
    price = extract_price(item)
    title = _first(item, "name", "collection_name")
    number = _first(item, "external_collection_number", "number")
    ident = _first(item, "id")
    if not (price and title and number and ident):
        return None
    attrs: dict[str, str] = {}
    for a in item.get("attributes") or []:
        if not isinstance(a, dict):
            continue
        typ = str(a.get("type") or a.get("trait_type") or "").lower()
        val = a.get("value")
        key = {"model": "model", "backdrop": "background", "background": "background",
               "symbol": "symbol", "pattern": "symbol"}.get(typ)
        if key and val:
            attrs[key] = str(val)
    return {"id": str(ident), "title": str(title), "number": int(number), "price": price, "attributes": attrs}


class MarketListingWatcher:
    def __init__(self, floors: Any, redis: Any, session_scope: Any, settings: Settings) -> None:
        self.floors = floors  # MarketFloorWatcher: вход в маркеты, _authed, паузы при отказе
        self.redis = redis
        self.session_scope = session_scope
        self.settings = settings
        self._colls: dict[str, tuple[Any, str]] = {}
        self._colls_at = 0.0
        self.stats = {s: {"new": 0, "unknown_collection": 0, "suppressed": 0, "errors": 0} for s in ("mrkt", "portals")}

    async def _collections(self) -> dict[str, tuple[Any, str]]:
        if time.monotonic() - self._colls_at > 600 or not self._colls:
            async with self.session_scope() as session:
                rows = (await session.execute(select(Collection.id, Collection.name, Collection.contract_address))).all()
            self._colls = {norm_title(name): (cid, addr) for cid, name, addr in rows}
            self._colls_at = time.monotonic()
        return self._colls

    async def _fetch(self, source: str) -> list[dict[str, Any]]:
        if source == "mrkt":
            body: dict[str, Any] = {"collectionNames": [], "modelNames": [], "backdropNames": [], "symbolNames": [], "ordering": None,
                    "lowToHigh": False, "maxPrice": None, "minPrice": None, "mintable": None, "number": None,
                    "count": 20, "cursor": "", "query": None, "promotedFirst": False}
            out: list[dict[str, Any]] = []
            for _ in range(self.settings.market_listings_pages):
                data = await self.floors._authed("mrkt", "POST", f"{MRKT_API}/gifts/saling", json=body)
                out.extend((data or {}).get("gifts") or [])
                cursor = (data or {}).get("cursor")
                if not cursor:
                    break
                body = {**body, "cursor": cursor}
            return out
        params = {"offset": 0, "limit": 100, "sort_by": "listed_at desc", "status": "listed"}
        data = await self.floors._authed("portals", "GET", f"{PORTALS_API}/nfts/search", params=params)
        return (data or {}).get("results") or []

    async def poll_source(self, source: str) -> int:
        """Новые листинги маркета -> события. Возвращает число опубликованных."""
        if time.monotonic() < self.floors._pause_until[source]:
            return 0
        try:
            raw = await self._fetch(source)
        except MarketBlocked as exc:
            self.floors._pause_until[source] = time.monotonic() + 300
            logger.warning(f"[market_listings] {source}: {exc} — пауза 300s (защиту не обходим)")
            return 0
        except Exception as exc:  # noqa: BLE001
            self.stats[source]["errors"] += 1
            logger.warning(f"[market_listings] {source}: {exc!r}")
            return 0
        parse = parse_mrkt if source == "mrkt" else parse_portals
        items = [i for i in (parse(r) for r in raw if isinstance(r, dict)) if i]
        self.floors._log_sample(f"{source}-listing", raw)
        bootstrap = not await self.redis.exists(BOOT_KEY.format(source=source))
        fresh = []
        for i in items:
            if await self.redis.set(SEEN_KEY.format(source=source, id=i["id"]), b"1", nx=True, ex=SEEN_TTL):
                fresh.append(i)
        if bootstrap:
            await self.redis.set(BOOT_KEY.format(source=source), b"1", ex=SEEN_TTL * 30)
            return 0
        if len(fresh) > self.settings.market_listings_max_new:
            self.stats[source]["suppressed"] += len(fresh)
            logger.warning(f"[market_listings] {source}: {len(fresh)} новых за опрос — пропускаю (предохранитель)")
            return 0
        colls = await self._collections()
        now = dt.datetime.now(dt.timezone.utc)
        sent = 0
        for i in reversed(fresh):  # от старых к новым
            coll = colls.get(norm_title(i["title"]))
            if coll is None:
                self.stats[source]["unknown_collection"] += 1
                continue
            cid, addr = coll
            slug = gift_slug(f"{i['title']} #{i['number']}")
            await stream_publish(self.redis, STREAM_NFT_LISTINGS, {
                "type": "listing", "marketplace": source, "collection_id": str(cid), "collection_address": addr,
                "nft_address": slug or i["id"], "nft_index": i["number"], "nft_name": f"{i['title']} #{i['number']}",
                "price_ton": i["price"], "seller_ton": "", "attributes": i["attributes"],
                "link": f"https://t.me/nft/{slug}" if slug else "", "slug": slug, "listed_at": now.isoformat(),
                "tx_hash": None,
            })
            sent += 1
        self.stats[source]["new"] += sent
        if sent:
            logger.info(f"[market_listings] {source}: новых листингов {sent}")
        return sent

    async def run(self, stop_event: asyncio.Event) -> None:
        wanted = {s.strip().lower() for s in self.settings.market_floors_sources.split(",")}
        sources = [s for s in ("mrkt", "portals") if s in wanted]  # порядок = приоритет
        logger.info(f"[market_listings] запущен: {sources}")
        while not stop_event.is_set():
            for source in sources:
                await self.poll_source(source)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.settings.market_listings_interval)
            except asyncio.TimeoutError:
                pass
