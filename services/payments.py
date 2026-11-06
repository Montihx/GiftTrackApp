# /workspace/services/payments.py — CryptoBot платежи и управление подпиской
"""
Crypto Pay API: создание инвойсов + активация подписки.

createInvoice:
    POST https://pay.crypt.bot/api/createInvoice
    headers: Crypto-Pay-API-Token: <token>
    body: {"asset": "TON", "amount": "10.0", "payload": "<tg_id>", "description": "..."}
    Ответ: {"ok": true, "result": {"invoice_id": 1, "pay_url": "https://t.me/...", "status": "active"}}

Вебхук оплаты приходит на /api/payments/webhook/{secret} с телом:
    {"update_id": 1, "payload": {"invoice_id": 1, "status": "paid", "payload": "<tg_id>"}}
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import aiohttp
import orjson
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.retry import with_retry
from db.models import User

CRYPTOBOT_API = "https://pay.crypt.bot/api"


class CryptoBotClient:
    """Минимальный клиент Crypto Pay API (createInvoice)."""

    def __init__(self, token: str) -> None:
        self.token = token

    async def create_invoice(
        self, amount: float, payload: str, description: str = "", fiat: str | None = None
    ) -> dict[str, Any]:
        """Создать инвойс; возвращает result с pay_url.

        fiat="USD" -> сумма в долларах, оплата в TON/USDT по курсу CryptoBot;
        без fiat — прежнее поведение (amount в TON).
        """

        async def _do() -> dict[str, Any]:
            headers = {"Crypto-Pay-API-Token": self.token}
            body: dict[str, Any] = {"amount": str(amount), "payload": payload, "description": description}
            if fiat:
                body.update({"currency_type": "fiat", "fiat": fiat, "accepted_assets": "TON,USDT"})
            else:
                body["asset"] = "TON"
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as sess:
                async with sess.post(f"{CRYPTOBOT_API}/createInvoice", json=body, headers=headers) as resp:
                    data = orjson.loads(await resp.read())
                    if not data.get("ok"):
                        raise RuntimeError(f"CryptoBot error: {data.get('description')}")
                    return data["result"]

        return await with_retry(_do, context="cryptobot.createInvoice")


async def activate_subscription(session: AsyncSession, tg_id: int, days: int) -> dt.datetime:
    """
    Выдать/продлить подписку на days дней.

    Если текущая подписка активна — продлеваем от её конца, иначе от сейчас.
    Источник не важен: оплата через вебхук или админ-команда /grant пишут одно и то же.
    """

    def _ensure_aware(d: dt.datetime | None) -> dt.datetime:
        """Привести datetime к timezone-aware (UTC)."""
        if d is None:
            return dt.datetime.now(dt.timezone.utc)
        if d.tzinfo is None:
            return d.replace(tzinfo=dt.timezone.utc)
        return d

    stmt = select(User).where(User.tg_id == tg_id)
    user = (await session.execute(stmt)).scalar_one_or_none()
    if user is None:
        user = User(tg_id=tg_id)
        session.add(user)
        await session.flush()

    now = dt.datetime.now(dt.timezone.utc)
    user_expiry = _ensure_aware(user.sub_expires_at)
    base = user_expiry if user_expiry > now else now
    new_expiry = base + dt.timedelta(days=days)
    user.sub_plan = "premium"
    user.sub_expires_at = new_expiry
    await session.flush()
    logger.info(f"Подписка выдана: user {tg_id} до {new_expiry.isoformat()}")
    return new_expiry



def parse_invoice_payload(raw: str, allowed_days: set[int], default_days: int) -> tuple[int, int] | None:
    """payload инвойса -> (tg_id, days). Форматы: "<tg_id>" (старый) и "<tg_id>:<days>"."""
    head, _, tail = (raw or "").partition(":")
    if not head.isdigit():
        return None
    if not tail:
        return int(head), default_days
    if tail.isdigit() and int(tail) in allowed_days:
        return int(head), int(tail)
    return None
