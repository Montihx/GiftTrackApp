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




def test_format_sale_message() -> None:
    from services.dispatcher import format_sale_message

    text = format_sale_message({
        "nft_name": "Plush Pepe #2675", "nft_address": "0:abc", "price_ton": 2500.0, "price_usd": 2727.5,
        "marketplace": "getgems",
        "attributes": {"model": "Amalgam", "background": "Dark Lilac", "symbol": "Wolf Rage"},
    })
    assert "GIFT SOLD!" in text and "Plush Pepe #2675" in text
    assert "Model: Amalgam" in text and "Backdrop: Dark Lilac" in text and "Symbol: Wolf Rage" in text
    assert "2 500 TON" in text and "≈ $2 727.5" in text and "Sold on GetGems" in text
    no_usd = format_sale_message({"collection_name": "Teddy", "nft_index": 3, "price_ton": 5,
                                  "marketplace": "marketapp"})
    assert "Teddy #3" in no_usd and "≈" not in no_usd and "Sold on Marketapp" in no_usd


@pytest.mark.asyncio
async def test_process_sale_respects_model_filter(session):
    from db.models import Attribute, Collection, Filter, User

    session.add_all([User(tg_id=1, username="a"), User(tg_id=2, username="b")])
    coll = Collection(name="Pepe", contract_address="EQsale_disp", source="seed")
    session.add(coll)
    await session.flush()
    other = Attribute(collection_id=coll.id, type="model", value="Other")
    session.add(other)
    await session.flush()
    session.add_all([Filter(user_id=1, collection_id=coll.id, is_active=True),
                     Filter(user_id=2, collection_id=coll.id, model_id=other.id, is_active=True)])
    await session.commit()

    bot_mock = AsyncMock()
    dispatcher = NotificationDispatcher(bot=bot_mock, session=session, redis=AsyncMock(), resolver=AsyncMock())
    await dispatcher.process_sale({"collection_id": str(coll.id), "nft_name": "Pepe #1", "price_ton": 9,
                                   "marketplace": "getgems", "attributes": {"model": "Amalgam"}})
    assert dispatcher.stats["sales"] == 1
    assert [c[0][0] for c in bot_mock.send_message.call_args_list] == [1]
    assert "GIFT SOLD!" in bot_mock.send_message.call_args[0][1]


@pytest.mark.asyncio
async def test_filters_match_symbol_and_model_floor_range(session):
    """Символ и «Флор модели от/до» учитываются при рассылке."""
    from decimal import Decimal

    from db.models import Attribute, Collection, Filter, User

    session.add_all([User(tg_id=t) for t in (11, 12, 13, 14)])
    coll = Collection(name="Pepe", contract_address="EQfilter_disp", source="seed")
    session.add(coll)
    await session.flush()
    moon = Attribute(collection_id=coll.id, type="symbol", value="Moon")
    sun = Attribute(collection_id=coll.id, type="symbol", value="Sun")
    model = Attribute(collection_id=coll.id, type="model", value="Amalgam", floor_price=Decimal("100"))
    session.add_all([moon, sun, model])
    await session.flush()
    session.add_all([
        Filter(user_id=11, collection_id=coll.id, symbol_id=moon.id, is_active=True),   # совпадает
        Filter(user_id=12, collection_id=coll.id, symbol_id=sun.id, is_active=True),    # другой символ
        Filter(user_id=13, collection_id=coll.id, floor_min=50, floor_max=150, is_active=True),  # 100 в диапазоне
        Filter(user_id=14, collection_id=coll.id, floor_min=200, is_active=True),       # floor ниже
    ])
    await session.commit()

    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)  # floor модели берётся из БД
    bot_mock = AsyncMock()
    dispatcher = NotificationDispatcher(bot=bot_mock, session=session, redis=redis, resolver=AsyncMock())
    await dispatcher.process_sale({"collection_id": str(coll.id), "price_ton": 1, "marketplace": "getgems",
                                   "attributes": {"model": "Amalgam", "symbol": "Moon"}})
    assert sorted(c[0][0] for c in bot_mock.send_message.call_args_list) == [11, 13]
