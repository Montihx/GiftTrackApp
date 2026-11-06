# tests/test_resolver_login.py — QR-вход Telethon: истёкший токен пересоздаётся
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import pytest


class FakeQR:
    def __init__(self) -> None:
        self.url = "tg://login?token=T0"
        self.expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=30)
        self.waits = 0
        self.recreated = 0

    async def wait(self, timeout: float) -> None:
        self.waits += 1
        assert timeout <= 30  # ждём не дольше жизни токена
        if self.waits == 1:
            raise asyncio.TimeoutError

    async def recreate(self) -> None:
        self.recreated += 1
        self.url = f"tg://login?token=T{self.recreated}"


class FakeClient:
    def __init__(self) -> None:
        self.qr = FakeQR()

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...

    async def is_user_authorized(self) -> bool:
        return False

    async def qr_login(self) -> FakeQR:
        return self.qr

    async def get_me(self) -> Any:
        class Me:
            username, id = "tester", 1
        return Me()


async def test_qr_login_recreates_expired_token(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    from bot import resolver_main

    client = FakeClient()
    monkeypatch.setattr(resolver_main, "_make_client", lambda settings: client)
    await resolver_main._login_qr()
    out = capsys.readouterr().out
    assert client.qr.recreated == 1 and client.qr.waits == 2
    assert "tg://login?token=T1" in out and "Авторизован как @tester" in out
