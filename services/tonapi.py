# services/tonapi.py — клиент TonAPI (https://tonapi.io), Bearer-ключ с tonconsole.com
"""
ВАЖНО: у TonAPI нет эндпоинта floor price. Floor считаем как min(sale.price) по items
коллекции (GET /v2/nfts/collections/{address}/items?limit&offset), учитывая только
листинги в нативной валюте. Проверено на живом API (2026-11):
  * sale.price = {"currency_type": "native", "value": "<nano>", "decimals": 9, "token_name": "Gram"}
    — нативная монета теперь называется "Gram", поэтому смотрим на currency_type, а не на имя;
  * sale.market.name — "Getgems Sales", "Marketapp Marketplace", ...;
  * limit страницы: 1000 с ключом, 100 без ключа ("anonymous tier").
fetch_collection_items/floor — разовый полный скан (до tonapi_floor_scan_limit); основной путь —
постраничный скан в services/tonapi_sync.py (fetch_items_page), который покрывает любые коллекции.
Все запросы проходят через общий для процесса rate limiter (TONAPI_RPS).
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import orjson
from loguru import logger

from core.config import Settings
from services.collection_discovery import _http_request

PAGE_LIMIT = 1000
NANO = Decimal(10) ** 9
NATIVE_TOKEN_NAMES = {"TON", "GRAM"}


def parse_ton_price(sale: dict[str, Any] | None) -> Decimal | None:
    """item.sale.price {"currency_type": "native", "value": "5500000000", ...} -> Decimal TON.

    Jetton-листинги (USDT и т.п.) -> None: floor считается только в TON.
    """
    if not sale:
        return None
    price = sale.get("price") or {}
    currency_type = price.get("currency_type")
    if currency_type is not None:
        if currency_type != "native":
            return None
    elif str(price.get("token_name", "TON")).upper() not in NATIVE_TOKEN_NAMES:
        return None
    try:
        value = Decimal(str(price.get("value")))
        decimals = int(price.get("decimals", 9))
    except Exception:  # noqa: BLE001
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value / (Decimal(10) ** decimals)


class RateLimiter:
    """Минимальный интервал между запросами (общий на процесс): rps<=0 — без ограничения."""

    def __init__(self, rps: float) -> None:
        self.interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def acquire(self) -> None:
        if self.interval <= 0:
            return
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now += wait
            self._next = now + self.interval


_LIMITERS: dict[float, RateLimiter] = {}


def shared_limiter(rps: float) -> RateLimiter:
    """Один limiter на процесс для заданного RPS: updater, sync и витрина делят квоту ключа."""
    if rps not in _LIMITERS:
        _LIMITERS[rps] = RateLimiter(rps)
    return _LIMITERS[rps]


def floor_from_items(items: list[dict[str, Any]]) -> Decimal | None:
    prices = [p for it in items if (p := parse_ton_price(it.get("sale"))) is not None]
    return min(prices) if prices else None


class TonApiClient:
    def __init__(self, settings: Settings, rate: asyncio.Semaphore | None = None,
                 limiter: RateLimiter | None = None) -> None:
        self.settings = settings
        self._rate = rate or asyncio.Semaphore(settings.api_max_concurrency)
        self._limiter = limiter or shared_limiter(settings.tonapi_rps)

    @property
    def enabled(self) -> bool:
        return self.settings.tonapi_enabled

    async def _get(self, path: str, params: dict[str, str] | None = None, context: str = "") -> Any:
        url = f"{self.settings.tonapi_url.rstrip('/')}{path}"
        headers = {"Authorization": f"Bearer {self.settings.tonapi_key}"}
        await self._limiter.acquire()
        raw = await _http_request(self._rate, "GET", url, params=params, headers=headers,
                                  context=f"tonapi:{context or path}")
        return orjson.loads(raw)

    async def fetch_items_page(self, address: str, offset: int, limit: int = PAGE_LIMIT) -> list[dict[str, Any]]:
        """Одна страница items коллекции (GET /v2/nfts/collections/{address}/items)."""
        page = await self._get(
            f"/v2/nfts/collections/{address}/items",
            params={"limit": str(limit), "offset": str(offset)},
            context=f"items[{address[:8]}…@{offset}]",
        )
        return (page or {}).get("nft_items") or []

    async def fetch_collection_items(self, address: str) -> list[dict[str, Any]] | None:
        """Все items коллекции или None, если их больше tonapi_floor_scan_limit (скан был бы неполным)."""
        limit = self.settings.tonapi_floor_scan_limit
        items: list[dict[str, Any]] = []
        offset = 0
        while True:
            batch = await self.fetch_items_page(address, offset)
            items.extend(batch)
            if len(batch) < PAGE_LIMIT:
                return items
            offset += PAGE_LIMIT
            if offset >= limit:
                logger.info(f"tonapi: {address[:8]}… больше {limit} items — пропускаю")
                return None

    async def fetch_collection_floor(self, address: str) -> Decimal | None:
        """Floor коллекции или None, если скан неполный/листингов нет."""
        items = await self.fetch_collection_items(address)
        return floor_from_items(items) if items is not None else None

    async def fetch_item_sale_price(self, item_address: str) -> Decimal | None:
        """Цена активного листинга NFT (GET /v2/nfts/{address}) или None."""
        if not self.enabled:
            return None
        try:
            item = await self._get(f"/v2/nfts/{item_address}", context=f"item[{item_address[:8]}…]")
        except Exception as exc:  # noqa: BLE001 — витрина не падает из-за внешнего API
            logger.debug(f"tonapi item {item_address[:8]}…: {exc!r}")
            return None
        return parse_ton_price((item or {}).get("sale"))
