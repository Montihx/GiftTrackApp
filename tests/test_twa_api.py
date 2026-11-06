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

    # Мокаем валидацию initData + сессию БД (тестовая SQLite вместо PG)
    import api.twa as twa_module

    original_dep = twa_module.get_current_user_init_data
    original_session = twa_module.get_session

    async def fake_dep():
        return {"user": '{"id": 999, "username": "api_test"}'}

    async def fake_session():
        yield session

    app.dependency_overrides[original_dep] = fake_dep
    app.dependency_overrides[original_session] = fake_session

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



@pytest.mark.asyncio
async def test_create_filter_creates_missing_user_and_list_returns_attribute_names(session):
    """Без /start пользователя нет в users: фильтр должен создавать его (иначе FK -> 500).
    GET /filters отдаёт названия атрибутов для карточек (список коллекций атрибутов больше не содержит)."""
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy import select

    import api.twa as twa_module
    from api.main import app
    from db.models import Attribute, Collection, User

    coll = Collection(name="Names Coll", contract_address="EQnames_coll", source="seed")
    session.add(coll)
    await session.flush()
    model = Attribute(collection_id=coll.id, type="model", value="Amalgam")
    sym = Attribute(collection_id=coll.id, type="symbol", value="Wolf Rage")
    session.add_all([model, sym])
    await session.commit()

    async def fake_dep():
        return {"user": '{"id": 4242, "username": "newbie"}'}

    async def fake_session():
        yield session

    app.dependency_overrides[twa_module.get_current_user_init_data] = fake_dep
    app.dependency_overrides[twa_module.get_session] = fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/api/filters", json={
                "collection_id": str(coll.id), "model_id": str(model.id), "symbol_id": str(sym.id)})
            assert resp.status_code == 200
            user = (await session.execute(select(User).where(User.tg_id == 4242))).scalar_one()
            assert user.username == "newbie"
            data = (await client.get("/api/filters")).json()["filters"]
            assert data[0]["model_name"] == "Amalgam" and data[0]["symbol_name"] == "Wolf Rage"
            assert data[0]["background_name"] is None
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_collection_attributes_endpoint(session):
    from httpx import ASGITransport, AsyncClient

    import api.showcase as showcase_module
    from api.main import app
    from db.models import Attribute, Collection

    coll = Collection(name="Attr Coll", contract_address="EQattr_coll", source="seed", items_count=10)
    session.add(coll)
    await session.flush()
    session.add(Attribute(collection_id=coll.id, type="model", value="Zed", items_count=1))
    await session.commit()

    async def fake_session():
        yield session

    app.dependency_overrides[showcase_module.get_session] = fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = (await client.get(f"/api/collections/{coll.id}/attributes")).json()
            assert data["attributes"]["models"][0]["value"] == "Zed" and data["attributes"]["models"][0]["rarity"] == 10.0
            light = (await client.get("/api/collections")).json()["collections"]
            assert light[0]["attributes"]["models"] == [] and light[0]["rarity_summary"]["models"] == 1
            assert (await client.get("/api/collections/00000000-0000-0000-0000-000000000000/attributes")).status_code == 404
    finally:
        app.dependency_overrides.clear()


def test_apply_tg_keeps_onchain_floor_primary(monkeypatch) -> None:
    from api.showcase import _apply_tg
    from core.config import get_settings

    tg = {"issued_count": 2825, "tg_listed": 40, "tg_floor_stars": 20000, "tg_floor_price": 280.5}
    item = {"floor_price": 400.0}
    _apply_tg(item, tg)  # по умолчанию флор Telegram скрыт, основной — ончейн
    assert item["issued_count"] == 2825 and item["tg_floor_price"] is None
    assert item["best_floor"] == 400.0 and item["best_floor_source"] == "onchain"
    monkeypatch.setattr(get_settings(), "tg_floor_show", True)
    shown = {"floor_price": 400.0}
    _apply_tg(shown, tg)  # включено: показывается справочно, но не подменяет ончейн
    assert shown["tg_floor_price"] == 280.5 and shown["best_floor"] == 400.0
    none = {"floor_price": None}
    _apply_tg(none, None)
    assert none["best_floor"] is None and none["issued_count"] is None


@pytest.mark.asyncio
async def test_tg_stats_reads_redis_keys(monkeypatch):
    import orjson

    import api.showcase as showcase_module

    store = {
        "tg_market:floor:c1": orjson.dumps({"min_stars": 10000, "listed": 33}),
        "tg_market:stats:c1": orjson.dumps({"issued": 2825, "listed": 30, "ton_per_star": 0.0136}),
        "tg_market:rate": orjson.dumps(0.0100),
        "tg_market:stats:c2": orjson.dumps({"issued": 100}),
        "tg_market:floor:c2": orjson.dumps({"min_stars": 5000, "listed": None}),
        "market_floor:mrkt:c1": orjson.dumps({"price": 90.5, "ts": 1}),
        "market_floor:portals:c2": orjson.dumps({"price": 40.0, "ts": 1}),
    }

    class FakeRedis:
        async def mget(self, keys):
            return [store.get(k) for k in keys]

        async def aclose(self):
            pass

    import core.redis_client as rc

    monkeypatch.setattr(rc, "create_redis", lambda: FakeRedis())
    out = await showcase_module._tg_stats(["c1", "c2"])
    assert out["c1"] == {"issued_count": 2825, "tg_listed": 33, "tg_floor_stars": 10000, "tg_floor_price": 136.0,
                     "market_floors": {"mrkt": 90.5}}
    assert out["c2"]["market_floors"] == {"portals": 40.0}
    assert out["c2"]["tg_floor_price"] == 50.0 and out["c2"]["tg_listed"] is None  # курс — общий, если нет своего


def test_apply_tg_best_floor_is_min_over_onchain_and_markets() -> None:
    from api.showcase import _apply_tg

    item = {"floor_price": 280.0}
    _apply_tg(item, {"market_floors": {"mrkt": 15.0, "portals": 300.0}})
    assert item["best_floor"] == 15.0 and item["best_floor_source"] == "mrkt"
    only = {"floor_price": 20.0}
    _apply_tg(only, {"market_floors": {"portals": 30.0}})
    assert only["best_floor"] == 20.0 and only["best_floor_source"] == "onchain"


def test_suspect_onchain_floor_is_ignored_for_best_floor() -> None:
    from api.showcase import _apply_tg

    cat = {"floor_price": 15.0}
    _apply_tg(cat, {"market_floors": {"mrkt": 281.0, "portals": 288.0}})  # Scared Cat: 15 против ~285
    assert cat["onchain_suspect"] is True and cat["best_floor"] == 281.0 and cat["best_floor_source"] == "mrkt"
    pepe = {"floor_price": 7300.0}
    _apply_tg(pepe, {"market_floors": {"mrkt": 7293.0, "portals": 7400.0}})
    assert pepe["onchain_suspect"] is False and pepe["best_floor"] == 7293.0
    alone = {"floor_price": 15.0}
    _apply_tg(alone, {})  # маркетов нет — сравнивать не с чем, ончейн остаётся
    assert alone["onchain_suspect"] is False and alone["best_floor"] == 15.0
