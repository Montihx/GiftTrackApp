# /workspace/services/resolver/telethon_worker.py — cold path: скан bio через Telethon
"""
Cold path резолвера (ТЗ, модуль 4): userbot сканит bio известных Telegram-пользователей
на наличие TON-адресов и строит индекс «адрес -> пользователь».

Как это работает:
  1. Кандидаты: username из users, nft_ownership_history и wallet_tg_map.
     Telegram не умеет искать по тексту bio, поэтому сканируем только известных людей.
  2. Для каждого кандидата: GetFullUser -> about -> адреса (EQ/UQ/raw/ton://) ->
     Redis hash `bio_index` (raw-адрес -> {tg_user_id, username}).
  3. Адреса из очереди `telethon_resolve_queue` ищутся в индексе; при попадании
     пишем wallet_tg_map (source=telethon) и Redis-кэш wallet:{addr}.
  4. FloodWait соблюдается строго, между запросами пауза; повторный скан — раз в N дней.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any

import orjson
from loguru import logger
from pytoniq_core import Address
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.redis_client import cache_set_json, wallet_key
from db.models import NftOwnershipHistory, User, WalletTgMap

BIO_INDEX_KEY = "bio_index"
QUEUE_KEY = "telethon_resolve_queue"

# user-friendly: EQ…/UQ…/kQ…/0Q… (48 символов base64url) и raw «0:<64 hex>»
_FRIENDLY_RE = re.compile(r"(?<![A-Za-z0-9_-])[EUk0][QAgw][A-Za-z0-9_-]{46}(?![A-Za-z0-9_-])")
_RAW_RE = re.compile(r"(?<![0-9A-Za-z])-?[01]:[0-9a-fA-F]{64}(?![0-9A-Za-z])")


def normalize_address(addr: str) -> str | None:
    """Приводит любой формат адреса к raw «wc:hex»; None, если адрес невалиден."""
    try:
        a = Address(addr.strip())
    except Exception:  # noqa: BLE001
        return None
    return f"{a.wc}:{a.hash_part.hex()}"


def extract_ton_addresses(text: str | None) -> set[str]:
    """Находит в тексте все валидные TON-адреса (raw-форма)."""
    if not text:
        return set()
    found: set[str] = set()
    for rx in (_FRIENDLY_RE, _RAW_RE):
        for m in rx.findall(text):
            raw = normalize_address(m)
            if raw:
                found.add(raw)
    return found


class BioScanner:
    """DI: client (Telethon), redis, session_factory (callable -> async ctx AsyncSession)."""

    def __init__(self, client: Any, redis: Any, session_scope: Any, settings: Settings | None = None) -> None:
        self.client = client
        self.redis = redis
        self.session_scope = session_scope
        self.settings = settings or get_settings()

    # ---------- кандидаты ----------
    async def candidates(self, session: AsyncSession) -> list[str]:
        names: set[str] = set()
        for col in (User.username, NftOwnershipHistory.owner_username, WalletTgMap.username):
            rows = (await session.execute(select(col).where(col.is_not(None)).distinct())).scalars()
            names.update(n.lstrip("@") for n in rows if n)
        fresh: list[str] = []
        for name in sorted(names):
            if not await self.redis.exists(f"bio_scanned:{name.lower()}"):
                fresh.append(name)
        return fresh[: self.settings.bio_scan_batch]

    # ---------- скан одного пользователя ----------
    async def scan_username(self, username: str) -> int:
        """Возвращает число найденных адресов. Соблюдает FloodWait."""
        from telethon import errors, functions

        ttl = self.settings.bio_rescan_days * 86400
        try:
            full = await self.client(functions.users.GetFullUserRequest(username))
        except errors.FloodWaitError as exc:
            logger.warning(f"FloodWait {exc.seconds}s — пауза")
            await asyncio.sleep(exc.seconds + 1)
            return 0  # не помечаем как просканированного — повторим в след. цикле
        except (errors.UsernameNotOccupiedError, errors.UsernameInvalidError, ValueError):
            await self.redis.set(f"bio_scanned:{username.lower()}", b"1", ex=ttl)
            return 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"GetFullUser @{username}: {exc!r}")
            return 0

        about = getattr(full.full_user, "about", None)
        user_id = full.users[0].id if full.users else None
        addrs = extract_ton_addresses(about)
        for raw in addrs:
            await self.redis.hset(
                BIO_INDEX_KEY, raw, orjson.dumps({"tg_user_id": user_id, "username": username})
            )
        await self.redis.set(f"bio_scanned:{username.lower()}", b"1", ex=ttl)
        if addrs:
            logger.info(f"bio @{username}: найдено адресов {len(addrs)}")
        return len(addrs)

    async def scan_cycle(self) -> int:
        async with self.session_scope() as session:
            names = await self.candidates(session)
        total = 0
        for name in names:
            total += await self.scan_username(name)
            await asyncio.sleep(self.settings.bio_scan_delay)
        logger.info(f"Цикл скана bio: пользователей {len(names)}, адресов {total}")
        return total

    # ---------- адрес из очереди ----------
    async def resolve_queued(self, ton_address: str) -> dict[str, Any] | None:
        raw = normalize_address(ton_address)
        if not raw:
            return None
        hit = await self.redis.hget(BIO_INDEX_KEY, raw)
        if not hit:
            return None
        data = orjson.loads(hit)
        async with self.session_scope() as session:
            row = (
                await session.execute(select(WalletTgMap).where(WalletTgMap.ton_address == ton_address))
            ).scalar_one_or_none()
            if row is None:
                session.add(
                    WalletTgMap(
                        ton_address=ton_address,
                        tg_user_id=data["tg_user_id"],
                        username=data["username"],
                        source="telethon",
                        confidence=70,
                    )
                )
            else:
                row.tg_user_id = data["tg_user_id"] or row.tg_user_id
                row.username = data["username"] or row.username
                row.source = "telethon"
                row.confidence = max(row.confidence or 0, 70)
        result = {**data, "source": "telethon"}
        await cache_set_json(self.redis, wallet_key(ton_address), result, ttl=3600)
        logger.info(f"Telethon resolved {ton_address[:12]}… -> @{data['username']}")
        return result

    # ---------- главный цикл ----------
    async def run(self, stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        next_scan = loop.time()
        while not stop_event.is_set():
            if loop.time() >= next_scan:
                try:
                    await self.scan_cycle()
                except Exception as exc:  # noqa: BLE001
                    logger.exception(f"scan_cycle: {exc!r}")
                next_scan = loop.time() + self.settings.bio_cycle_seconds
            try:
                item = await self.redis.brpop(QUEUE_KEY, timeout=5)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — таймаут/обрыв Redis не должен ронять сервис
                logger.warning(f"redis brpop: {exc!r} — повтор через 2s")
                await asyncio.sleep(2)
                continue
            if item is None:
                continue
            addr = item[1].decode() if isinstance(item[1], bytes) else item[1]
            try:
                await self.resolve_queued(addr)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"resolve_queued {addr[:12]}…: {exc!r}")
