# tests/test_tonapi.py — парсинг цен TonAPI и логика полного/частичного скана
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from core.config import Settings
from services import tonapi
from services.tonapi import TonApiClient, floor_from_items, parse_ton_price


def _item(nano: str | None, token: str = "TON") -> dict[str, Any]:
    return {"sale": {"price": {"value": nano, "token_name": token}}} if nano else {}


def test_parse_ton_price() -> None:
    assert parse_ton_price({"price": {"value": "5500000000", "token_name": "TON"}}) == Decimal("5.5")
    assert parse_ton_price({"price": {"value": "1", "token_name": "USDT"}}) is None
    assert parse_ton_price({"price": {"value": "abc"}}) is None
    assert parse_ton_price(None) is None


def test_floor_from_items_takes_min_and_ignores_unlisted() -> None:
    items = [_item("7000000000"), _item(None), _item("5500000000"), _item("9", "USDT")]
    assert floor_from_items(items) == Decimal("5.5")
    assert floor_from_items([_item(None)]) is None


def test_enabled_depends_on_key() -> None:
    assert not Settings(tonapi_key="").tonapi_enabled
    assert Settings(tonapi_key=" k ").tonapi_enabled


async def test_floor_complete_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    cl = TonApiClient(Settings(tonapi_key="k"))

    async def fake_get(path: str, params: Any = None, context: str = "") -> Any:
        return {"nft_items": [_item("7000000000"), _item("5500000000")]}

    monkeypatch.setattr(cl, "_get", fake_get)
    assert await cl.fetch_collection_floor("EQx") == Decimal("5.5")


async def test_floor_none_when_scan_exceeds_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    cl = TonApiClient(Settings(tonapi_key="k", tonapi_floor_scan_limit=1000))

    async def fake_get(path: str, params: Any = None, context: str = "") -> Any:
        return {"nft_items": [_item("1000000000")] * tonapi.PAGE_LIMIT}  # полная страница -> есть ещё

    monkeypatch.setattr(cl, "_get", fake_get)
    assert await cl.fetch_collection_floor("EQx") is None
