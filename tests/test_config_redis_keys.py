# /workspace/tests/test_config_redis_keys.py — тесты конфига и ключей Redis
from __future__ import annotations

import hashlib
import hmac

from core.config import Settings
from core.redis_client import (
    STREAM_ALERTS_HOT,
    collection_attrs_key,
    floor_key,
    owner_history_key,
    recent_sales_key,
    wallet_key,
)


def test_floor_keys() -> None:
    cid = "b1a2c3d4-0000-0000-0000-000000000001"
    assert floor_key(cid) == f"floor:{cid}"
    assert floor_key(cid, "Disco Bear") == f"floor:{cid}:Disco Bear"
    assert recent_sales_key(cid, "Bear") == f"recent_sales:{cid}:Bear"
    assert owner_history_key("EQabc") == "owner_history:EQabc"
    assert wallet_key("EQabc") == "wallet:EQabc"
    assert collection_attrs_key(cid) == f"collection_attrs:{cid}"
    assert STREAM_ALERTS_HOT == "alerts:hot"


def test_twa_secret_derivation(monkeypatch) -> None:
    """Секрет initData = HMAC_SHA256(key='WebAppData', msg=bot_token)."""
    token = "123456:TESTtoken"
    s = Settings(_env_file=None, bot_token=token)
    expected = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    assert s.twa_secret_key() == expected
    # Явный override имеет приоритет
    s2 = Settings(_env_file=None, bot_token=token, twa_auth_secret="custom")
    assert s2.twa_secret_key() == b"custom"


def test_settings_defaults(monkeypatch) -> None:
    monkeypatch.delenv("BOT_TOKEN", raising=False)
    s = Settings(_env_file=None)
    assert s.bot_token == ""  # дефолт без .env
    assert s.environment in ("development", "production")
