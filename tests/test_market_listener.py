# /workspace/tests/test_market_listener.py — тесты Market Listener
"""Unit-тесты для services/market/listener.py."""
from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest

from services.market.events import ListingEvent, listing_to_stream_payload
from services.market.listener import MarketListener


def test_parse_listing():
    """Парсинг GraphQL-ноды листинга."""
    node = {
        "address": "EQnft123",
        "index": "8681",
        "collection": {"address": "EQcollection"},
        "metadata": {
            "attributes": [
                {"name": "Model", "value": "Obelisk"},
                {"name": "Background", "value": "Silver Blue"},
            ]
        },
        "minPrice": {"amount": {"value": "2400.0"}},
        "owner": {"address": "EQseller"},
        "createdAt": "2026-11-04T18:00:00Z",
    }

    listener = MarketListener.__new__(MarketListener)
    event = listener._parse_listing(node, "EQcollection")

    assert event is not None
    assert event.nft_address == "EQnft123"
    assert event.nft_index == 8681
    assert event.price_ton == 2400.0
    assert event.seller_ton == "EQseller"
    assert event.attributes == {"model": "Obelisk", "background": "Silver Blue"}
    assert event.marketplace == "getgems"


def test_parse_listing_invalid():
    """Листинг без адреса не парсится."""
    node = {"index": "1", "minPrice": None}
    listener = MarketListener.__new__(MarketListener)
    event = listener._parse_listing(node, "EQcollection")
    assert event is None


def test_listing_to_stream_payload():
    """Преобразование ListingEvent в payload."""
    event = ListingEvent(
        nft_address="EQnft123",
        nft_index=8681,
        collection_address="EQcollection",
        marketplace="getgems",
        price_ton=2400.0,
        seller_ton="EQseller",
        attributes={"model": "Obelisk"},
        listed_at=dt.datetime(2026, 11, 4, 18, 0, 0, tzinfo=dt.timezone.utc),
    )

    payload = listing_to_stream_payload(event)
    assert payload["type"] == "listing"
    assert payload["nft_address"] == "EQnft123"
    assert payload["price_ton"] == 2400.0
    assert payload["attributes"] == {"model": "Obelisk"}


@pytest.mark.asyncio
async def test_market_listener_process_collection(session):
    """Интеграционный тест: обработка коллекции с моками GraphQL."""
    from db.models import Collection

    coll = Collection(name="Market Coll", contract_address="EQmarket_coll", source="getgems")
    session.add(coll)
    await session.commit()

    mock_listings = [
        {
            "address": "EQnft_new",
            "index": "100",
            "collection": {"address": "EQmarket_coll"},
            "metadata": {"attributes": [{"name": "Model", "value": "Ruby"}]},
            "minPrice": {"amount": {"value": "189.0"}},
            "owner": {"address": "EQseller1"},
            "createdAt": "2026-11-04T18:00:00Z",
        }
    ]

    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value=b"1-0")

    listener = MarketListener(session=session, redis=redis_mock)
    listener._fetch_listings = AsyncMock(return_value=mock_listings)

    await listener._process_collection(str(coll.id), "Market Coll", "EQmarket_coll")

    assert listener.stats["listings"] == 1
    assert redis_mock.xadd.called


@pytest.mark.asyncio
async def test_market_listener_dedupe(session):
    """Повторный поллинг тех же листингов не дублирует события."""
    from db.models import Collection

    coll = Collection(name="Dedupe Market", contract_address="EQdedupe_mkt", source="getgems")
    session.add(coll)
    await session.commit()

    mock_listings = [
        {
            "address": "EQnft_dup",
            "index": "200",
            "collection": {"address": "EQdedupe_mkt"},
            "metadata": {"attributes": []},
            "minPrice": {"amount": {"value": "100.0"}},
            "owner": {"address": "EQseller2"},
            "createdAt": "2026-11-04T18:00:00Z",
        }
    ]

    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value=b"1-0")

    listener = MarketListener(session=session, redis=redis_mock)
    listener._fetch_listings = AsyncMock(return_value=mock_listings)

    # Первый цикл
    await listener._process_collection(str(coll.id), "Dedupe Market", "EQdedupe_mkt")
    assert listener.stats["listings"] == 1
    assert redis_mock.xadd.call_count == 1

    # Второй цикл с теми же листингами (Redis вернёт last_seen)
    redis_mock.get = AsyncMock(return_value=b'"2026-11-04T18:00:00+00:00"')
    await listener._process_collection(str(coll.id), "Dedupe Market", "EQdedupe_mkt")
    assert listener.stats["listings"] == 1
    assert redis_mock.xadd.call_count == 1  # нет дублей
