# /workspace/api/auth.py — валидация Telegram Web App initData
"""
Валидация initData из Telegram Web App по официальному алгоритму:
https://core.telegram.org/bots/webapps#validating-data-received-via-the-web-app

Алгоритм:
  1. initData — это query-string (key=value&key=value)
  2. Извлекаем hash, убираем его из строки
  3. Сортируем остальные пары по алфавиту, соединяем через \n
  4. secret_key = HMAC_SHA256(key="WebAppData", msg=bot_token)
  5. Проверка: HMAC_SHA256(key=secret_key, msg=data_check_string) == hash
"""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any
from urllib.parse import parse_qsl

from fastapi import HTTPException, Request

from core.config import get_settings

MAX_DATA_AGE = 86400  # 24 часа


def validate_init_data(init_data: str, secret_key: bytes, max_age: int = MAX_DATA_AGE) -> dict[str, Any]:
    """
    Валидирует initData и возвращает распарсенный dict (user, auth_date, и т.д.).

    Бросает HTTPException 401 при невалидной подписи или устаревших данных.
    """
    try:
        parsed = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        raise HTTPException(status_code=401, detail="Malformed initData") from None

    received_hash = parsed.pop("hash", None)
    if not received_hash:
        raise HTTPException(status_code=401, detail="Missing hash")

    # data_check_string: пары key=value, отсортированные по key, через \n
    check_pairs = sorted(f"{k}={v}" for k, v in parsed.items())
    data_check_string = "\n".join(check_pairs)

    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, received_hash):
        raise HTTPException(status_code=401, detail="Invalid signature")

    # Проверка свежести
    auth_date = int(parsed.get("auth_date", 0))
    if time.time() - auth_date > max_age:
        raise HTTPException(status_code=401, detail="Data is outdated")

    return parsed


async def get_current_user_init_data(request: Request) -> dict[str, Any]:
    """FastAPI-зависимость: достаёт initData из заголовка и валидирует."""
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    if not init_data:
        raise HTTPException(status_code=401, detail="Missing X-Telegram-Init-Data header")

    settings = get_settings()
    return validate_init_data(init_data, settings.twa_secret_key())
