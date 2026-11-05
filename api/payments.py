# /workspace/api/payments.py — вебхук CryptoBot
"""POST /api/payments/webhook/{secret} — подтверждение оплаты -> активация подписки."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from api.twa import get_session
from core.config import get_settings
from services.payments import activate_subscription

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

    if status != "paid" or not invoice_payload.isdigit():
        return {"ok": True, "ignored": True}

    tg_id = int(invoice_payload)
    await activate_subscription(session, tg_id, settings.subscription_days)
    logger.info(f"Webhook: подписка оплачена пользователем {tg_id}")
    return {"ok": True}
