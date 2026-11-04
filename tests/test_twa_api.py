# /workspace/tests/test_twa_api.py — тесты TWA API
"""Unit-тесты для api/auth.py и api/twa.py."""
from __future__ import annotations

import hashlib
import hmac
import time
from urllib.parse import urlencode

import pytest

from api.auth import validate_init_data


def _make_init_data(secret: bytes, user_id: int = 12345) -> str:
    """Генерирует валидный initData для тестов."""
    payload = {
        "user": f'{{"id": {user_id}, "username": "test"}}',
        "auth_date": str(int(time.time())),
        "query_id": "AAX",
    }
    check_string = "\n".join(sorted(f"{k}={v}" for k, v in payload.items()))
    hash_hex = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    payload["hash"] = hash_hex
    return urlencode(payload)


def test_validate_init_data_ok():
    """Валидный initData проходит проверку."""
    secret = b"test_secret_key"
    init_data = _make_init_data(secret)
    parsed = validate_init_data(init_data, secret)
    assert parsed["user"] == '{"id": 12345, "username": "test"}'


def test_validate_init_data_bad_signature():
    """Невалидная подпись отклоняется."""
    from fastapi import HTTPException

    secret = b"test_secret_key"
    init_data = _make_init_data(secret)
    with pytest.raises(HTTPException) as exc:
        validate_init_data(init_data, b"wrong_secret")
    assert exc.value.status_code == 401


def test_validate_init_data_outdated():
    """Устаревшие данные отклоняются."""
    from fastapi import HTTPException

    secret = b"test_secret_key"
    payload = {
        "user": '{"id": 1}',
        "auth_date": str(int(time.time()) - 100000),
    }
    check_string = "\n".join(sorted(f"{k}={v}" for k, v in payload.items()))
    payload["hash"] = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    init_data = urlencode(payload)

    with pytest.raises(HTTPException) as exc:
        validate_init_data(init_data, secret, max_age=86400)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_create_filter_endpoint(session):
    """POST /api/filters создаёт фильтр."""
    from httpx import ASGITransport, AsyncClient

    from api.main import app
    from db.models import Collection

    coll = Collection(name="API Coll", contract_address="EQapi_coll", source="getgems")
    session.add(coll)
    await session.commit()
    coll_id = str(coll.id)

    # Мокаем валидацию initData
    import api.twa as twa_module

    original_dep = twa_module.get_current_user_init_data

    async def fake_dep():
        return {"user": '{"id": 999, "username": "api_test"}'}

    app.dependency_overrides[original_dep] = fake_dep

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/filters",
                json={"collection_id": coll_id, "floor_min": 100.0},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["ok"] is True
            assert "filter_id" in data
    finally:
        app.dependency_overrides.clear()
