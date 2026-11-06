# /workspace/api/payments.py — вебхук CryptoBot
"""POST /api/payments/webhook/{secret} — подтверждение оплаты -> активация подписки."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from api.twa import get_session
from core.config import get_settings
from services.payments import activate_subscription, parse_invoice_payload

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

    body = await request.json()
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
    await activate_subscription(session, tg_id, days)
    logger.info(f"Webhook: подписка оплачена пользователем {tg_id}")
    return {"ok": True}
