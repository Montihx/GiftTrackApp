# services/tonapi.py — клиент TonAPI (https://tonapi.io), Bearer-ключ с tonconsole.com
"""
ВАЖНО: у TonAPI нет эндпоинта floor price. Floor считаем как min(sale.price) по items
коллекции (GET /v2/nfts/collections/{address}/items?limit&offset), учитывая только
листинги в нативной валюте. Проверено на живом API (2026-11):
  * sale.price = {"currency_type": "native", "value": "<nano>", "decimals": 9, "token_name": "Gram"}
    — нативная монета теперь называется "Gram", поэтому смотрим на currency_type, а не на имя;
  * sale.market.name — "Getgems Sales", "Marketapp Marketplace", ...;
  * limit страницы: 1000 с ключом, 100 без ключа ("anonymous tier").
Сканирование коллекций — постранично в services/tonapi_sync.py (fetch_items_page).
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
    """Минимальный интервал между запросами (общий на процесс): rps<=0 — без ограничения.

    high=True — приоритет (события реального времени из потока): пока такой запрос ждёт,
    обычные (полный скан) слот не получают, поэтому скан не задерживает уведомления.
    """

    def __init__(self, rps: float) -> None:
        self.interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next = 0.0
        self._high_waiting = 0

    async def acquire(self, high: bool = False) -> None:
        if self.interval <= 0:
            return
        loop = asyncio.get_running_loop()
        if high:
            self._high_waiting += 1
        try:
            while True:
                async with self._lock:
                    if high or self._high_waiting == 0:
                        now = loop.time()
                        wait = self._next - now
                        if wait <= 0:
                            self._next = now + self.interval
                            return
                    else:
                        wait = 0.05  # уступаем приоритетному запросу
                await asyncio.sleep(max(wait, 0.005))
        finally:
            if high:
                self._high_waiting -= 1


_LIMITERS: dict[float, RateLimiter] = {}


def shared_limiter(rps: float) -> RateLimiter:
    """Один limiter на процесс для заданного RPS: updater, sync и витрина делят квоту ключа."""
    if rps not in _LIMITERS:
        _LIMITERS[rps] = RateLimiter(rps)
    return _LIMITERS[rps]


class TonApiClient:
    def __init__(self, settings: Settings, rate: asyncio.Semaphore | None = None,
                 limiter: RateLimiter | None = None) -> None:
        self.settings = settings
        self._rate = rate or asyncio.Semaphore(settings.api_max_concurrency)
        self._limiter = limiter or shared_limiter(settings.tonapi_rps)

    @property
    def enabled(self) -> bool:
        return self.settings.tonapi_enabled

    async def _get(self, path: str, params: dict[str, str] | None = None, context: str = "",
                   high: bool = False) -> Any:
        url = f"{self.settings.tonapi_url.rstrip('/')}{path}"
        headers = {"Authorization": f"Bearer {self.settings.tonapi_key}"}
        await self._limiter.acquire(high=high)
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

    async def fetch_nft(self, address: str) -> dict[str, Any]:
        """Одно NFT (GET /v2/nfts/{address}): owner, sale, metadata, collection."""
        return await self._get(f"/v2/nfts/{address}", context=f"nft[{address[:8]}…]", high=True) or {}

    async def fetch_account_nfts(self, account: str, offset: int, limit: int = PAGE_LIMIT) -> list[dict[str, Any]]:
        """NFT, которыми владеет аккаунт (GET /v2/accounts/{account}/nfts)."""
        page = await self._get(
            f"/v2/accounts/{account}/nfts",
            params={"limit": str(limit), "offset": str(offset), "indirect_ownership": "false"},
            context=f"account_nfts[{account[:8]}…@{offset}]",
        )
        return (page or {}).get("nft_items") or []

    async def fetch_collection(self, address: str) -> dict[str, Any]:
        """Коллекция: metadata, metadata_status.url, trust, approved_by (GET /v2/nfts/collections/{a})."""
        return await self._get(f"/v2/nfts/collections/{address}", context=f"collection[{address[:8]}…]") or {}

    async def fetch_deployer(self, address: str) -> str | None:
        """Отправитель первой транзакции аккаунта (для контракта — тот, кто его развернул)."""
        data = await self._get(
            f"/v2/blockchain/accounts/{address}/transactions",
            params={"limit": "1", "sort_order": "asc"},
            context=f"deployer[{address[:8]}…]",
        )
        txs = (data or {}).get("transactions") or []
        if not txs:
            return None
        return (((txs[0].get("in_msg") or {}).get("source")) or {}).get("address")

    async def fetch_ton_usd(self) -> float | None:
        """Курс TON->USD (GET /v2/rates?tokens=ton&currencies=usd) или None."""
        try:
            data = await self._get("/v2/rates", params={"tokens": "ton", "currencies": "usd"}, context="rates")
            value = float(((data or {}).get("rates") or {})["TON"]["prices"]["USD"])
        except Exception as exc:  # noqa: BLE001 — курс необязателен (≈USD просто не покажем)
            logger.debug(f"tonapi rates: {exc!r}")
            return None
        return value if value > 0 else None

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
