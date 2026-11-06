# services/gift_discovery.py — автообнаружение новых коллекций Telegram Gifts через TonAPI
"""
Telegram выпускает новые подарки без анонса адресов, а GetGems GraphQL и Bot API
адресов коллекций не дают. Используем on-chain признак, который нельзя подделать:

  * все коллекции Telegram Gifts развёрнуты кошельком «Fragment Gift Minter»
    (MINTER_ADDRESS) — отправитель первой транзакции контракта коллекции;
  * улучшенные, но ещё не выведенные в блокчейн подарки принадлежат этому же кошельку,
    поэтому его NFT содержат предметы почти всех коллекций, в т.ч. только что вышедших.

Алгоритм (раз в tonapi_discovery_interval, внутри listener-market):
  1. читаем tonapi_discovery_pages страниц GET /v2/accounts/{minter}/nfts (курсор в Redis,
     по кругу) — на странице из 1000 NFT обычно 100+ разных коллекций;
  2. коллекции, которых нет в БД, проверяем: деплоер == минтер и метаданные коллекции
     отдаются с https://nft.fragment.com/collection/<slug>.json;
  3. прошедшие проверку добавляем (source='tonapi', имя в единственном числе из имени
     предмета «Spiced Wine #123», иконка nft.fragment.com/collection/<slug>.webp);
     seed-плейсхолдер с тем же именем получает реальный адрес. Не прошедшие запоминаем
     в Redis, чтобы не проверять повторно.
Дальше коллекцию подхватывает TonApiSync в том же цикле.
"""
from __future__ import annotations

import re
import time
from typing import Any

from loguru import logger
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.ton_address import normalize_ton_address
from db.models import Collection
from services.seed import PLACEHOLDER_PREFIX
from services.tonapi import PAGE_LIMIT, TonApiClient

MINTER_ADDRESS = "0:158136239adb15dd59df90c641f9efd312cfeb8664f218f4c3e5fce9d95e6c07"
FRAGMENT_COLLECTION_PREFIX = "https://nft.fragment.com/collection/"
CURSOR_KEY = "gift_discovery:cursor"
REJECTED_KEY = "gift_discovery:rejected"  # set raw-адресов, не прошедших проверку
STATE_TTL = 30 * 86400
_ITEM_NUM_RE = re.compile(r"\s*#\s*\d+\s*$")


def raw_address(addr: str) -> str:
    """Любой формат адреса -> raw '0:<hex>' в нижнем регистре (для сравнения)."""
    addr = addr.strip()
    if re.match(r"^-?\d+:[0-9a-fA-F]{64}$", addr):
        return addr.lower()
    try:
        from pytoniq_core import Address

        return Address(addr).to_str(is_user_friendly=False).lower()
    except Exception:  # noqa: BLE001 — некорректный адрес сравнивается как есть
        return addr


def gift_name_from_item(item_name: str | None) -> str | None:
    """'Spiced Wine #123' -> 'Spiced Wine'."""
    if not item_name:
        return None
    name = _ITEM_NUM_RE.sub("", item_name).strip()
    return name or None


def _s(v: Any) -> str | None:
    return v.decode() if isinstance(v, bytes) else v


class GiftCollectionDiscovery:
    def __init__(self, session: AsyncSession, redis: Any, settings: Settings, client: TonApiClient,
                 page_limit: int = PAGE_LIMIT) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings
        self.client = client
        self.page_limit = page_limit
        self._last_run: float | None = None
        self.stats = {"runs": 0, "added": 0, "backfilled": 0, "rejected": 0}

    def due(self, now: float | None = None) -> bool:
        if not self.settings.tonapi_discovery_enabled:
            return False
        now = time.monotonic() if now is None else now
        return self._last_run is None or now - self._last_run >= self.settings.tonapi_discovery_interval

    async def maybe_run(self) -> dict[str, int] | None:
        if not self.due():
            return None
        self._last_run = time.monotonic()
        try:
            return await self.run()
        except Exception as exc:  # noqa: BLE001 — сбой обнаружения не мешает синхронизации
            logger.warning(f"[gift_discovery] итерация упала: {exc!r}")
            await self.session.rollback()
            return None

    async def _scan_minter(self) -> dict[str, str | None]:
        """{raw адрес коллекции: имя предмета} по нескольким страницам NFT минтера."""
        cursor = int(_s(await self.redis.get(CURSOR_KEY)) or 0)
        found: dict[str, str | None] = {}
        for _ in range(self.settings.tonapi_discovery_pages):
            items = await self.client.fetch_account_nfts(MINTER_ADDRESS, cursor, self.page_limit)
            for it in items:
                coll = (it.get("collection") or {}).get("address")
                if coll:
                    found.setdefault(raw_address(coll), (it.get("metadata") or {}).get("name"))
            cursor = 0 if len(items) < self.page_limit else cursor + len(items)
            if cursor == 0:
                break
        await self.redis.set(CURSOR_KEY, str(cursor).encode(), ex=STATE_TTL)
        return found

    async def _verify(self, raw: str) -> dict[str, Any] | None:
        """Метаданные коллекции, если это настоящая коллекция Telegram Gifts, иначе None."""
        deployer = await self.client.fetch_deployer(raw)
        if deployer is None or raw_address(deployer) != MINTER_ADDRESS:
            return None
        coll = await self.client.fetch_collection(raw)
        url = ((coll.get("metadata_status") or {}).get("url")) or ""
        if not url.startswith(FRAGMENT_COLLECTION_PREFIX):
            return None
        return coll

    async def run(self) -> dict[str, int]:
        found = await self._scan_minter()
        rows = (await self.session.execute(select(Collection.contract_address))).scalars().all()
        known = {raw_address(a) for a in rows if not a.startswith(PLACEHOLDER_PREFIX)}
        candidates = [a for a in found if a not in known]
        added = backfilled = rejected = 0
        for raw in candidates:
            if await self.redis.sismember(REJECTED_KEY, raw):
                continue
            coll = await self._verify(raw)
            if coll is None:
                await self.redis.sadd(REJECTED_KEY, raw)
                await self.redis.expire(REJECTED_KEY, STATE_TTL)
                rejected += 1
                logger.info(f"[gift_discovery] {raw[:12]}… не коллекция Telegram Gifts — пропускаю")
                continue
            meta = coll.get("metadata") or {}
            name = gift_name_from_item(found[raw]) or meta.get("name") or raw
            address = normalize_ton_address(raw) or raw
            icon = meta.get("image")
            placeholder = (await self.session.execute(
                select(Collection).where(
                    func.lower(Collection.name) == name.lower(),
                    Collection.contract_address.startswith(PLACEHOLDER_PREFIX),
                )
            )).scalars().first()
            if placeholder is not None:
                await self.session.execute(
                    update(Collection).where(Collection.id == placeholder.id)
                    .values(contract_address=address, icon_url=placeholder.icon_url or icon)
                )
                backfilled += 1
            else:
                self.session.add(Collection(name=name, contract_address=address, icon_url=icon, source="tonapi"))
                added += 1
            logger.info(f"[gift_discovery] новая коллекция: {name} ({address})")
        await self.session.commit()
        self.stats["runs"] += 1
        self.stats["added"] += added
        self.stats["backfilled"] += backfilled
        self.stats["rejected"] += rejected
        result = {"seen": len(found), "candidates": len(candidates), "added": added,
                  "backfilled": backfilled, "rejected": rejected}
        logger.info(f"[gift_discovery] {result}")
        return result
