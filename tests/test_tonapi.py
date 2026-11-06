# tests/test_tonapi.py — парсинг цен TonAPI, rate limiter, постраничный запрос items
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from core.config import Settings
from services import tonapi
from services.tonapi import TonApiClient, parse_ton_price


def _item(nano: str | None, token: str = "TON") -> dict[str, Any]:
    return {"sale": {"price": {"value": nano, "token_name": token}}} if nano else {}


def test_parse_ton_price() -> None:
    assert parse_ton_price({"price": {"value": "5500000000", "token_name": "TON"}}) == Decimal("5.5")
    assert parse_ton_price({"price": {"value": "1", "token_name": "USDT"}}) is None
    assert parse_ton_price({"price": {"value": "abc"}}) is None
    assert parse_ton_price(None) is None
    # живой формат TonAPI (2026-11): нативная монета называется "Gram"
    live = {"price": {"currency_type": "native", "value": "2500000000000", "decimals": 9, "token_name": "Gram"}}
    assert parse_ton_price(live) == Decimal("2500")
    jetton = {"price": {"currency_type": "jetton", "value": "1000000", "decimals": 6, "token_name": "USDT"}}
    assert parse_ton_price(jetton) is None


async def test_rate_limiter_spaces_requests() -> None:
    import asyncio

    lim = tonapi.RateLimiter(rps=50)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    for _ in range(4):
        await lim.acquire()
    assert loop.time() - t0 >= 3 / 50 * 0.9
    assert tonapi.shared_limiter(7.0) is tonapi.shared_limiter(7.0)


def test_enabled_depends_on_key() -> None:
    assert not Settings(tonapi_key="").tonapi_enabled
    assert Settings(tonapi_key=" k ").tonapi_enabled


async def test_fetch_items_page_params(monkeypatch: pytest.MonkeyPatch) -> None:
    cl = TonApiClient(Settings(tonapi_key="k"))
    calls: list[tuple[str, Any]] = []

    async def fake_get(path: str, params: Any = None, context: str = "") -> Any:
        calls.append((path, params))
        return {"nft_items": [_item("7000000000")]} if params["offset"] == "0" else {}

    monkeypatch.setattr(cl, "_get", fake_get)
    assert len(await cl.fetch_items_page("EQx", 0)) == 1
    assert await cl.fetch_items_page("EQx", 1000, limit=100) == []
    assert calls == [("/v2/nfts/collections/EQx/items", {"limit": "1000", "offset": "0"}),
                     ("/v2/nfts/collections/EQx/items", {"limit": "100", "offset": "1000"})]
