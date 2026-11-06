# services/market_floors.py — флоры сторонних маркетов подарков (MRKT, Portals) для сравнения с ончейн-флором
"""
Неофициальные API маркетов (см. документацию boostNT/MRKT-API, boostNT/Portals-API):
  вход — через мини-приложение маркета вашим Telegram-аккаунтом (Telethon messages.requestAppWebView ->
  initData), дальше обычные HTTPS-запросы (aiohttp) с токеном. Никакой маскировки под браузер:
  если маркет отвечает отказом или проверкой (403/429/503, не-JSON), источник уходит в паузу
  и пишет в лог — обход защиты не делается.
  Tonnel не подключён: его документация прямо требует имитации браузера.

MRKT:    POST https://api.tgmrkt.io/api/v1/auth {"data": initData} -> {"token"}; токен в Authorization;
         POST /gifts/saling {collectionNames:[имя], ordering:"Price", lowToHigh:true, count:1} -> {"gifts":[...]}.
Portals: Authorization: "tma <initData>"; GET https://portal-market.com/api/collections?search=<имя>
         (id коллекции) и GET /api/nfts/search?collection_ids=<id>&sort_by=price asc&status=listed&limit=1.

Поля ответов в документации не описаны, поэтому разбор цены терпимый (extract_price) и первый ответ
каждого маркета пишется в лог: по нему можно поправить поля. Результат — Redis `market_floor:{source}:{cid}`
({"price": <в монете>, "ts": ...}); API витрины берёт минимум из ончейна и маркетов.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any
from urllib.parse import unquote

import aiohttp
from loguru import logger
from sqlalchemy import select

from core.config import Settings
from core.redis_client import cache_set_json
from db.models import Collection

FLOOR_KEY = "market_floor:{source}:{cid}"
SOURCES = ("mrkt", "portals")
SOURCE_TITLES = {"mrkt": "MRKT", "portals": "Portals"}

MRKT_API = "https://api.tgmrkt.io/api/v1"
PORTALS_API = "https://portal-market.com/api"
PRICE_KEYS = ("salePrice", "sale_price", "price", "priceNano", "listPrice", "amount")
NANO = 1_000_000_000


def _num(v: Any) -> float | None:
    if isinstance(v, dict):  # {"amount": ..} / {"value": ..}
        for k in ("amount", "value", "price"):
            if k in v:
                return _num(v[k])
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def extract_price(item: dict[str, Any]) -> float | None:
    """Цена листинга в монете (GRAM/TON). Значения >= 1e6 считаем нано-единицами (делим на 1e9)."""
    for key in PRICE_KEYS:
        n = _num(item.get(key))
        if n is not None:
            return round(n / NANO, 4) if n >= 1_000_000 else n
    return None


def min_price(items: list[Any]) -> float | None:
    prices = [p for p in (extract_price(i) for i in items if isinstance(i, dict)) if p]
    return min(prices) if prices else None


class MarketBlocked(Exception):
    """Маркет ответил отказом/проверкой — уходим в паузу, ничего не обходим."""


class MarketFloorWatcher:
    def __init__(self, client: Any, redis: Any, session_scope: Any, settings: Settings,
                 http: aiohttp.ClientSession | None = None) -> None:
        self.client = client  # Telethon
        self.redis = redis
        self.session_scope = session_scope
        self.settings = settings
        self._http = http
        self._tokens: dict[str, tuple[str, float]] = {}
        self._pause_until: dict[str, float] = {s: 0.0 for s in SOURCES}
        self._fail: dict[str, int] = {s: 0 for s in SOURCES}
        self._portals_ids: dict[str, str | None] = {}
        self._logged: set[str] = set()
        self.stats = {s: {"ok": 0, "empty": 0, "errors": 0} for s in SOURCES}

    # --- вход через мини-приложение ---
    async def _init_data(self, bot: str, short_name: str) -> str:
        from telethon.tl import functions, types

        peer = await self.client.get_input_entity(bot)
        user = await self.client.get_entity(bot)
        app = types.InputBotAppShortName(
            bot_id=types.InputUser(user_id=user.id, access_hash=user.access_hash), short_name=short_name)
        res = await self.client(functions.messages.RequestAppWebViewRequest(peer=peer, app=app, platform="android"))
        return unquote(res.url.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion", 1)[0])

    async def _request(self, method: str, url: str, **kw: Any) -> Any:
        http = self._http or aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        self._http = http
        async with http.request(method, url, **kw) as resp:
            if resp.status in (403, 429, 503) or resp.status >= 500:
                raise MarketBlocked(f"HTTP {resp.status}")
            if resp.status == 401:
                raise PermissionError("401")
            try:
                return await resp.json(content_type=None)
            except Exception as exc:  # не JSON — вероятно страница проверки
                raise MarketBlocked(f"не-JSON ответ: {exc!r}") from exc

    async def _token(self, source: str, force: bool = False) -> str:
        tok = self._tokens.get(source)
        if tok and not force and tok[1] > time.monotonic():
            return tok[0]
        if source == "mrkt":
            init = await self._init_data("mrkt", "app")
            data = await self._request("POST", f"{MRKT_API}/auth", json={"data": init})
            token = (data or {}).get("token")
            if not token:
                raise MarketBlocked("MRKT не выдал токен")
        else:
            token = f"tma {await self._init_data('portals', 'market')}"
        self._tokens[source] = (token, time.monotonic() + 6 * 3600)
        return token

    # --- запросы флора ---
    async def _mrkt_floor(self, name: str) -> float | None:
        body = {"collectionNames": [name], "modelNames": [], "backdropNames": [], "symbolNames": [],
                "ordering": "Price", "lowToHigh": True, "maxPrice": None, "minPrice": None, "mintable": None,
                "number": None, "count": 1, "cursor": "", "query": None, "promotedFirst": False}
        data = await self._authed("mrkt", "POST", f"{MRKT_API}/gifts/saling", json=body)
        gifts = (data or {}).get("gifts") or []
        self._log_sample("mrkt", gifts)
        return min_price(gifts)

    async def _portals_id(self, name: str) -> str | None:
        if name not in self._portals_ids:
            data = await self._authed("portals", "GET", f"{PORTALS_API}/collections", params={"search": name, "limit": 10})
            cols = (data or {}).get("collections") or []
            exact = [c for c in cols if str(c.get("name", "")).strip().lower() == name.strip().lower()]
            pick = (exact or cols[:1] or [None])[0]
            self._portals_ids[name] = str(pick["id"]) if pick and pick.get("id") is not None else None
        return self._portals_ids[name]

    async def _portals_floor(self, name: str) -> float | None:
        cid = await self._portals_id(name)
        if not cid:
            return None
        params = {"offset": 0, "limit": 1, "collection_ids": cid, "sort_by": "price asc", "status": "listed"}
        data = await self._authed("portals", "GET", f"{PORTALS_API}/nfts/search", params=params)
        results = (data or {}).get("results") or []
        self._log_sample("portals", results)
        return min_price(results)

    async def _authed(self, source: str, method: str, url: str, **kw: Any) -> Any:
        for attempt in (0, 1):
            headers = {"Authorization": await self._token(source, force=bool(attempt))}
            if source == "mrkt":
                headers["Referer"] = "https://cdn.tgmrkt.io/"
            try:
                return await self._request(method, url, headers=headers, **kw)
            except PermissionError:
                if attempt:
                    raise MarketBlocked("токен не принят (401)") from None
        return None

    def _log_sample(self, source: str, items: list[Any]) -> None:
        if source not in self._logged and items and isinstance(items[0], dict):
            self._logged.add(source)
            keys = sorted(items[0].keys())
            logger.info(f"[market_floors] {source}: поля листинга {keys}, цена -> {extract_price(items[0])}")

    # --- цикл ---
    async def poll_source(self, source: str, cid: str, name: str) -> None:
        if time.monotonic() < self._pause_until[source]:
            return
        try:
            price = await (self._mrkt_floor(name) if source == "mrkt" else self._portals_floor(name))
        except MarketBlocked as exc:
            self._fail[source] += 1
            pause = min(60 * 2 ** (self._fail[source] - 1), 3600)
            self._pause_until[source] = time.monotonic() + pause
            self.stats[source]["errors"] += 1
            logger.warning(f"[market_floors] {source}: {exc} — пауза {pause}s (защиту не обходим)")
            return
        except Exception as exc:  # noqa: BLE001
            self.stats[source]["errors"] += 1
            logger.warning(f"[market_floors] {source} {name}: {exc!r}")
            return
        self._fail[source] = 0
        if price is None:
            self.stats[source]["empty"] += 1
            return
        self.stats[source]["ok"] += 1
        await cache_set_json(self.redis, FLOOR_KEY.format(source=source, cid=cid),
                             {"price": price, "ts": int(time.time())}, ttl=self.settings.market_floors_ttl)

    async def run(self, stop_event: asyncio.Event) -> None:
        wanted = {s.strip().lower() for s in self.settings.market_floors_sources.split(",")}
        sources = [s for s in SOURCES if s in wanted]
        logger.info(f"[market_floors] запущен: {sources}")
        while not stop_event.is_set():
            async with self.session_scope() as session:
                rows = (await session.execute(select(Collection.id, Collection.name))).all()
            for cid, name in rows:
                for source in sources:
                    if stop_event.is_set():
                        return
                    await self.poll_source(source, cid, name)
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self.settings.market_floors_interval)
                    except asyncio.TimeoutError:
                        pass
            logger.info(f"[market_floors] круг завершён: {self.stats}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
