# services/seed.py — seed-каталог коллекций Telegram Gifts (db/seed/gift_collections.json)
"""
Upsert seed-каталога при старте updater (source="seed").

Правила:
  * существующие записи (по contract_address) НЕ перезаписываются;
  * entry без contract_address -> плейсхолдер "tg_gift:<slug>" (updater такие пропускает,
    floor остаётся NULL -> витрина показывает «—»);
  * если в JSON позже появился реальный адрес, а в БД лежит seed-плейсхолдер этой же
    коллекции — плейсхолдер заменяется реальным адресом (backfill), вместе с icon_url;
  * у существующей записи без иконки icon_url дозаполняется из JSON.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.ton_address import normalize_ton_address
from db.models import Collection

PLACEHOLDER_PREFIX = "tg_gift:"


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def load_seed_entries(path: str | Path) -> list[dict[str, Any]]:
    """Читает и валидирует JSON; невалидные адреса отбрасываются с предупреждением."""
    p = Path(path)
    if not p.is_absolute() and not p.exists():  # относительный путь из env — резолвим от корня проекта
        p = Path(__file__).resolve().parent.parent / p
    raw = json.loads(p.read_text(encoding="utf-8"))
    items = raw.get("collections", []) if isinstance(raw, dict) else raw
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        name = (item.get("name") or "").strip()
        if not name:
            continue
        addr_raw = (item.get("contract_address") or "").strip()
        address: str | None = None
        if addr_raw:
            address = normalize_ton_address(addr_raw)
            if address is None:
                logger.warning(f"seed: некорректный адрес для '{name}' — игнорирую адрес")
        contract = address or f"{PLACEHOLDER_PREFIX}{slugify(name)}"
        if contract in seen:
            continue
        seen.add(contract)
        result.append(
            {
                "name": name,
                "contract_address": contract,
                "slug": slugify(name),
                "has_address": address is not None,
                "icon_url": item.get("icon_url") or None,
            }
        )
    return result


async def upsert_seed(session: AsyncSession, path: str | Path) -> dict[str, int]:
    """Идемпотентный upsert; коммит делает вызывающий. Возвращает статистику."""
    stats = {"created": 0, "skipped": 0, "backfilled": 0}
    for e in load_seed_entries(path):
        existing = (
            await session.execute(
                select(Collection.id, Collection.icon_url).where(
                    Collection.contract_address == e["contract_address"]
                )
            )
        ).one_or_none()
        if existing is not None:
            if existing.icon_url is None and e["icon_url"]:  # иконку дозаполняем, остальное не трогаем
                await session.execute(
                    update(Collection).where(Collection.id == existing.id).values(icon_url=e["icon_url"])
                )
            stats["skipped"] += 1
            continue

        if e["has_address"]:
            # backfill: seed-плейсхолдер этой коллекции -> реальный адрес
            placeholder = (
                await session.execute(
                    select(Collection.id).where(
                        Collection.contract_address == f"{PLACEHOLDER_PREFIX}{e['slug']}",
                        Collection.source == "seed",
                    )
                )
            ).scalar_one_or_none()
            if placeholder is not None:
                await session.execute(
                    update(Collection)
                    .where(Collection.id == placeholder)
                    .values(contract_address=e["contract_address"])
                    .values(**({"icon_url": e["icon_url"]} if e["icon_url"] else {}))
                )
                stats["backfilled"] += 1
                continue

        session.add(
            Collection(
                name=e["name"],
                contract_address=e["contract_address"],
                icon_url=e["icon_url"],
                source="seed",
            )
        )
        stats["created"] += 1
    await session.flush()
    logger.info(f"seed: {stats}")
    return stats
