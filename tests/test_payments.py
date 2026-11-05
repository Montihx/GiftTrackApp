# /workspace/tests/test_payments.py — тесты платежей и админ-доступа
"""Unit-тесты для services/payments.py и api/payments.py."""
from __future__ import annotations

import datetime as dt

import pytest

from services.payments import activate_subscription


@pytest.mark.asyncio
async def test_activate_subscription_new_user(session):
    """Новый пользователь получает premium."""
    from sqlalchemy import select

    from db.models import User

    expiry = await activate_subscription(session, 111, 30)
    await session.commit()
    user = (await session.execute(select(User).where(User.tg_id == 111))).scalar_one()
    assert user.sub_plan == "premium"
    assert user.sub_expires_at is not None
    assert user.sub_expires_at > dt.datetime.now(dt.timezone.utc)


@pytest.mark.asyncio
async def test_activate_subscription_extends_active(session):
    """Активная подписка продлевается от конца текущей."""
    from sqlalchemy import select

    from db.models import User

    now = dt.datetime.now(dt.timezone.utc)
    session.add(User(tg_id=222, sub_plan="premium", sub_expires_at=now + dt.timedelta(days=10)))
    await session.commit()
    expiry = await activate_subscription(session, 222, 30)
    assert expiry > now + dt.timedelta(days=39)


@pytest.mark.asyncio
async def test_webhook_activates_subscription(session):
    """Вебхук paid активирует подписку."""
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy import select

    from api.main import app
    from api.twa import get_session
    from core.config import get_settings
    from db.models import User

    session.add(User(tg_id=333))
    await session.commit()

    async def fake_session():
        yield session

    app.dependency_overrides[get_session] = fake_session
    try:
        secret = get_settings().cryptobot_webhook_secret
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                f"/api/payments/webhook/{secret}",
                json={"payload": {"status": "paid", "payload": "333"}},
            )
        assert resp.status_code == 200
        user = (await session.execute(select(User).where(User.tg_id == 333))).scalar_one()
        assert user.sub_plan == "premium"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_webhook_bad_secret(session):
    """Неверный секрет отклоняется."""
    from httpx import ASGITransport, AsyncClient

    from api.main import app
    from api.twa import get_session

    async def fake_session():
        yield session

    app.dependency_overrides[get_session] = fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/payments/webhook/wrongsecret",
                json={"payload": {"status": "paid", "payload": "333"}},
            )
        assert resp.status_code == 403
    finally:
        app.dependency_overrides.clear()


def test_is_admin():
    """Парсинг admin_tg_ids из конфига."""
    from core.config import Settings

    s = Settings(admin_tg_ids="123, 456")
    assert s.is_admin(123)
    assert s.is_admin(456)
    assert not s.is_admin(789)
