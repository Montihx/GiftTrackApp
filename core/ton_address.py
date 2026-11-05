# core/ton_address.py — валидация/нормализация адресов TON (без тяжёлых зависимостей)
from __future__ import annotations

import re

_FRIENDLY_RE = re.compile(r"^[EU]Q[A-Za-z0-9_-]{46}$")
_RAW_RE = re.compile(r"^-?\d+:[0-9a-fA-F]{64}$")


def normalize_ton_address(raw: str) -> str | None:
    """Проверяет адрес TON; user-friendly приводит к bounceable-виду (EQ...), иначе None."""
    raw = raw.strip()
    if not (_FRIENDLY_RE.match(raw) or _RAW_RE.match(raw)):
        return None
    try:  # каноникализация, если pytoniq-core доступен
        from pytoniq_core import Address

        return Address(raw).to_str(is_user_friendly=True, is_bounceable=True)
    except Exception:  # noqa: BLE001 — не валим вызов из-за библиотеки
        return raw
