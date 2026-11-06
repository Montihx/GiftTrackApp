# /workspace/api/payments.py — вебхук CryptoBot
"""POST /api/payments/webhook/{secret} — подтверждение оплаты -> активация подписки.

Защита: секрет в URL + (если задан CRYPTOBOT_TOKEN) подпись Crypto-Pay-Api-Signature.
Идемпотентность: invoice_id пишется в payment_invoices; повторная доставка игнорируется.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

import orjson
from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from api.twa import get_session
from core.config import get_settings
from db.models import PaymentInvoice
from services.payments import activate_subscription, parse_invoice_payload, verify_webhook_signature

def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except InvalidOperation:
        return None


router = APIRouter(prefix="/api/payments", tags=["payments"])


@router.post("/webhook/{secret}")
async def payments_webhook(
    secret: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Вебхук CryptoBot: invoice paid -> активация подписки."""
    settings = get_settings()
    if secret != settings.cryptobot_webhook_secret:
        raise HTTPException(status_code=403, detail="Bad secret")

    raw = await request.body()
    if settings.cryptobot_token and not verify_webhook_signature(
        settings.cryptobot_token, raw, request.headers.get("crypto-pay-api-signature")
    ):
        raise HTTPException(status_code=401, detail="Bad signature")
    try:
        body = orjson.loads(raw)
    except orjson.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Bad JSON") from None
    if body.get("update_type", "invoice_paid") != "invoice_paid":
        return {"ok": True, "ignored": True}
    payload = body.get("payload") or {}
    status = payload.get("status")
    invoice_payload = payload.get("payload") or ""

    parsed = parse_invoice_payload(
        invoice_payload,
        allowed_days={p["days"] for p in settings.subscription_plans},
        default_days=settings.subscription_days,
    )
    if status != "paid" or parsed is None:
        return {"ok": True, "ignored": True}

    tg_id, days = parsed
    invoice_id = payload.get("invoice_id")
    if invoice_id is not None:
        if await session.get(PaymentInvoice, int(invoice_id)) is not None:
            logger.info(f"Webhook: инвойс {invoice_id} уже обработан — повтор игнорирую")
            return {"ok": True, "duplicate": True}
        session.add(PaymentInvoice(
            invoice_id=int(invoice_id), tg_id=tg_id, days=days,
            paid_asset=payload.get("paid_asset"), paid_amount=_decimal(payload.get("paid_amount")),
        ))
        try:
            await session.flush()
        except IntegrityError:  # параллельная доставка того же инвойса
            await session.rollback()
            return {"ok": True, "duplicate": True}
    await activate_subscription(session, tg_id, days)
    logger.info(f"Webhook: подписка оплачена пользователем {tg_id}")
    return {"ok": True}
