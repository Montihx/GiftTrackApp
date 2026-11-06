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

    await activate_subscription(session, 111, 30)
    await session.commit()
    user = (await session.execute(select(User).where(User.tg_id == 111))).scalar_one()
    assert user.sub_plan == "premium"
    assert user.sub_expires_at is not None
    expected_expiry = dt.datetime.now(dt.timezone.utc)
    assert user.sub_expires_at.replace(tzinfo=None) > expected_expiry.replace(tzinfo=None)



@pytest.mark.asyncio
async def test_activate_subscription_extends_active(session):
    """Активная подписка продлевается от конца текущей."""

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


def _post_webhook(app, secret: str, body: dict, headers: dict | None = None):
    import orjson
    from httpx import ASGITransport, AsyncClient

    async def _go():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            return await client.post(f"/api/payments/webhook/{secret}", content=orjson.dumps(body),
                                     headers={"content-type": "application/json", **(headers or {})})
    return _go()


@pytest.mark.asyncio
async def test_activate_subscription_expired_starts_from_now(session):
    """Истёкшая подписка продлевается от текущего момента, а не от старой даты."""
    from db.models import User

    now = dt.datetime.now(dt.timezone.utc)
    session.add(User(tg_id=444, sub_plan="premium", sub_expires_at=now - dt.timedelta(days=5)))
    await session.commit()
    expiry = await activate_subscription(session, 444, 60)
    assert now + dt.timedelta(days=59) < expiry < now + dt.timedelta(days=61)


def test_verify_webhook_signature_matches_sdk_algorithm():
    import hashlib
    import hmac as hmac_mod

    from services.payments import verify_webhook_signature

    body = b'{"update_id":1}'
    sig = hmac_mod.new(hashlib.sha256(b"tok").digest(), body, hashlib.sha256).hexdigest()
    assert verify_webhook_signature("tok", body, sig)
    assert verify_webhook_signature("tok", body, sig.upper())
    assert not verify_webhook_signature("tok", body + b" ", sig)
    assert not verify_webhook_signature("other", body, sig)
    assert not verify_webhook_signature("tok", body, None)


@pytest.mark.asyncio
async def test_webhook_signature_required_when_token_set(session, monkeypatch):
    import hashlib
    import hmac as hmac_mod

    import orjson
    from sqlalchemy import select

    from api.main import app
    from api.twa import get_session
    from core.config import get_settings
    from db.models import PaymentInvoice, User

    async def fake_session():
        yield session

    settings = get_settings()
    monkeypatch.setattr(settings, "cryptobot_token", "tok")
    app.dependency_overrides[get_session] = fake_session
    try:
        body = {"update_id": 7, "update_type": "invoice_paid",
                "payload": {"invoice_id": 99, "status": "paid", "payload": "555:30",
                            "paid_asset": "TON", "paid_amount": "45.1"}}
        resp = await _post_webhook(app, settings.cryptobot_webhook_secret, body, {"crypto-pay-api-signature": "bad"})
        assert resp.status_code == 401

        sig = hmac_mod.new(hashlib.sha256(b"tok").digest(), orjson.dumps(body), hashlib.sha256).hexdigest()
        headers = {"crypto-pay-api-signature": sig}
        resp = await _post_webhook(app, settings.cryptobot_webhook_secret, body, headers)
        assert resp.status_code == 200 and resp.json() == {"ok": True}
        user = (await session.execute(select(User).where(User.tg_id == 555))).scalar_one()
        first_expiry = user.sub_expires_at

        # повторная доставка того же инвойса не продлевает подписку
        resp = await _post_webhook(app, settings.cryptobot_webhook_secret, body, headers)
        assert resp.json().get("duplicate") is True
        await session.refresh(user)
        assert user.sub_expires_at == first_expiry
        inv = await session.get(PaymentInvoice, 99)
        assert inv is not None and inv.days == 30 and inv.paid_asset == "TON"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_webhook_ignores_other_update_types(session):
    from api.main import app
    from api.twa import get_session
    from core.config import get_settings

    async def fake_session():
        yield session

    app.dependency_overrides[get_session] = fake_session
    try:
        body = {"update_type": "something_else", "payload": {"status": "paid", "payload": "1:30"}}
        resp = await _post_webhook(app, get_settings().cryptobot_webhook_secret, body)
        assert resp.json().get("ignored") is True
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_create_invoice_fiat_body_and_bot_invoice_url(monkeypatch):
    """Тело fiat-инвойса как в Crypto Pay API; pay_url берётся из bot_invoice_url."""
    from services import payments

    sent: dict = {}

    class FakeResp:
        async def read(self):
            import orjson
            return orjson.dumps({"ok": True, "result": {"invoice_id": 1, "bot_invoice_url": "https://t.me/CryptoBot?start=IV1"}})

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class FakeSession:
        def __init__(self, *a, **kw):
            pass

        def post(self, url, json=None, headers=None):
            sent.update(url=url, json=json, headers=headers)
            return FakeResp()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(payments.aiohttp, "ClientSession", FakeSession)
    res = await payments.CryptoBotClient("tok").create_invoice(49.0, "1:30", "Premium", fiat="USD")
    assert res["pay_url"] == "https://t.me/CryptoBot?start=IV1"
    assert sent["url"].endswith("/createInvoice") and sent["headers"] == {"Crypto-Pay-API-Token": "tok"}
    assert sent["json"] == {"amount": "49.0", "payload": "1:30", "description": "Premium",
                            "currency_type": "fiat", "fiat": "USD", "accepted_assets": "TON,USDT"}


def test_cryptobot_testnet_url():
    from core.config import Settings
    from services.payments import CryptoBotClient

    assert Settings().cryptobot_api_url == "https://pay.crypt.bot/api"
    assert CryptoBotClient("t", "https://testnet-pay.crypt.bot/api/").api_url == "https://testnet-pay.crypt.bot/api"
