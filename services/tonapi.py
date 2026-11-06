# services/tonapi.py — клиент TonAPI (https://tonapi.io), Bearer-ключ с tonconsole.com
"""
ВАЖНО: у TonAPI нет эндпоинта floor price. Floor считаем как min(sale.price) по items
коллекции (GET /v2/nfts/collections/{address}/items?limit&offset), учитывая только
листинги в TON. Если у коллекции больше items, чем tonapi_floor_scan_limit, скан был бы
частичным и дал бы неверную цену — тогда floor = None (витрина покажет «—»), а не ложное число.
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


def parse_ton_price(sale: dict[str, Any] | None) -> Decimal | None:
    """item.sale.price {"value": "5500000000", "token_name": "TON"} -> Decimal TON."""
    if not sale:
        return None
    price = sale.get("price") or {}
    if str(price.get("token_name", "TON")).upper() != "TON":
        return None
    try:
        value = Decimal(str(price.get("value")))
    except Exception:  # noqa: BLE001
        return None
    if value <= 0:
        return None
    return value / NANO


def floor_from_items(items: list[dict[str, Any]]) -> Decimal | None:
    prices = [p for it in items if (p := parse_ton_price(it.get("sale"))) is not None]
    return min(prices) if prices else None


class TonApiClient:
    def __init__(self, settings: Settings, rate: asyncio.Semaphore | None = None) -> None:
        self.settings = settings
        self._rate = rate or asyncio.Semaphore(settings.api_max_concurrency)

    @property
    def enabled(self) -> bool:
        return self.settings.tonapi_enabled

    async def _get(self, path: str, params: dict[str, str] | None = None, context: str = "") -> Any:
        url = f"{self.settings.tonapi_url.rstrip('/')}{path}"
        headers = {"Authorization": f"Bearer {self.settings.tonapi_key}"}

        raw = await _http_request(self._rate, "GET", url, params=params, headers=headers,
                                  context=f"tonapi:{context or path}")
        return orjson.loads(raw)

    async def fetch_collection_floor(self, address: str) -> Decimal | None:
        """Floor коллекции или None, если скан неполный/листингов нет."""
        limit = self.settings.tonapi_floor_scan_limit
        items: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = await self._get(
                f"/v2/nfts/collections/{address}/items",
                params={"limit": str(PAGE_LIMIT), "offset": str(offset)},
                context=f"items[{address[:8]}…]",
            )
            batch = (page or {}).get("nft_items") or []
            items.extend(batch)
            if len(batch) < PAGE_LIMIT:
                break  # последняя страница — скан полный
            offset += PAGE_LIMIT
            if offset >= limit:
                logger.info(f"tonapi: {address[:8]}… больше {limit} items — floor не считаю")
                return None
        return floor_from_items(items)

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
