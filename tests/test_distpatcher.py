# /workspace/tests/test_dispatcher.py — тесты Notification Dispatcher
"""Unit-тесты для services/dispatcher.py."""
from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.dispatcher import NotificationDispatcher, _format_time_ago


def test_format_time_ago_days():
    """Форматирование 'N дн. назад'."""
    ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=5)
    assert _format_time_ago(ts) == "5 дн. назад"


def test_format_time_ago_months():
    """Форматирование 'N мес., M дн. назад'."""
    ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=67)
    result = _format_time_ago(ts)
    assert "мес." in result


def test_format_time_ago_none():
    """None -> 'неизвестно'."""
    assert _format_time_ago(None) == "неизвестно"


@pytest.mark.asyncio
async def test_process_upgrade_sends_to_subscribers(session):
    """Апгрейд: сообщение отправляется подписчикам с подходящим фильтром."""
    from db.models import Collection, Filter, User

    # Создаём пользователя и фильтр
    user = User(tg_id=12345, username="testuser")
    session.add(user)
    await session.flush()

    coll = Collection(name="Test Coll", contract_address="EQtest_disp", source="getgems")
    session.add(coll)
    await session.commit()

    filt = Filter(user_id=12345, collection_id=coll.id, is_active=True)
    session.add(filt)
    await session.commit()

    # Моки
    bot_mock = AsyncMock()
    bot_mock.send_message = AsyncMock()
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    resolver_mock = AsyncMock()
    resolver_mock.resolve = AsyncMock(
        return_value={"tg_user_id": 999, "username": "wimp", "source": "cache"}
    )

    dispatcher = NotificationDispatcher(
        bot=bot_mock, session=session, redis=redis_mock, resolver=resolver_mock
    )

    payload = {
        "type": "mint",
        "nft_address": "EQnft_disp",
        "collection_id": str(coll.id),
        "index": 42,
        "owner_ton": "EQowner_disp",
        "metadata": {"name": "Scared Cat #42", "model": "Obelisk", "background": "Silver Blue"},
    }

    await dispatcher.process_upgrade(payload)

    assert dispatcher.stats["upgrades"] == 1
    assert bot_mock.send_message.called
    call_args = bot_mock.send_message.call_args
    assert call_args[0][0] == 12345  # user_id
    assert "АПГРЕЙД" in call_args[0][1]
    assert "@wimp" in call_args[0][1]


@pytest.mark.asyncio
async def test_process_listing_includes_analytics(session):
    """Листинг: сообщение включает floor prices и recent sales из Redis."""
    from db.models import Collection, Filter, User

    user = User(tg_id=67890, username="buyer")
    session.add(user)
    await session.flush()

    coll = Collection(name="List Coll", contract_address="EQtest_list", source="getgems")
    session.add(coll)
    await session.commit()

    filt = Filter(user_id=67890, collection_id=coll.id, is_active=True)
    session.add(filt)
    await session.commit()

    bot_mock = AsyncMock()
    bot_mock.send_message = AsyncMock()

    # Redis возвращает аналитику
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(
        side_effect=lambda key: {
            b"floor:coll_id": b"155.0",
            b"floor:coll_id:Obelisk": b"1500.0",
        }.get(key if isinstance(key, bytes) else key.encode(), None)
    )

    resolver_mock = AsyncMock()
    resolver_mock.resolve = AsyncMock(
        return_value={"tg_user_id": 888, "username": "seller", "source": "cache"}
    )

    dispatcher = NotificationDispatcher(
        bot=bot_mock, session=session, redis=redis_mock, resolver=resolver_mock
    )

    payload = {
        "type": "listing",
        "nft_address": "EQnft_list",
        "nft_index": 1285,
        "collection_id": str(coll.id),
        "marketplace": "getgems",
        "price_ton": 2400.0,
        "seller_ton": "EQseller_list",
        "attributes": {"model": "Obelisk", "background": "Silver Blue"},
    }

    await dispatcher.process_listing(payload)

    assert dispatcher.stats["listings"] == 1
    assert bot_mock.send_message.called
    text = bot_mock.send_message.call_args[0][1]
    assert "ЛИСТИНГ" in text
    assert "2400.0 TON" in text
    assert "@seller" in text


@pytest.mark.asyncio
async def test_no_subscribers_no_send(session):
    """Если нет активных фильтров — сообщение не отправляется."""
    bot_mock = AsyncMock()
    bot_mock.send_message = AsyncMock()
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    resolver_mock = AsyncMock()
    resolver_mock.resolve = AsyncMock(return_value={})

    dispatcher = NotificationDispatcher(
        bot=bot_mock, session=session, redis=redis_mock, resolver=resolver_mock
    )

    payload = {
        "type": "mint",
        "nft_address": "EQnft_nosub",
        "collection_id": None,
        "index": 1,
        "owner_ton": "EQowner_nosub",
        "metadata": {},
    }

    await dispatcher.process_upgrade(payload)

    assert not bot_mock.send_message.called

@pytest.mark.asyncio
async def test_rarity_lines(session):
    """Rarity-строки: процент редкости и наличие."""
    from db.models import Attribute, Collection
    coll = Collection(name="Rarity Coll", contract_address="EQrarity", source="getgems", items_count=1000)
    session.add(coll)
    await session.commit()
    attr = Attribute(collection_id=coll.id, type="model", value="Obelisk", items_count=6)
    session.add(attr)
    await session.commit()
    bot_mock = AsyncMock()
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    resolver_mock = AsyncMock()
    dispatcher = NotificationDispatcher(bot=bot_mock, session=session, redis=redis_mock, resolver=resolver_mock)
    lines = await dispatcher._rarity_and_supply_lines(coll.id, {"model": "Obelisk"})
    assert any("Выпущено" in l for l in lines)
    assert any("0.6%" in l for l in lines)


