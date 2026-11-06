# /workspace/services/resolver/resolver.py — каскадный резолвинг TON -> Telegram
"""
OwnerResolver: находит Telegram-аккаунт по TON-адресу кошелька.

Каскад (hot path, < 2 сек):
  1. Redis кэш wallet:{ton} (TTL 1h) — 90% попаданий
  2. TON DNS API: GET https://tonviewer.com/api/v2/accounts/{ton}/dns
     -> домены вида "username.ton" -> username
  3. GetGems GraphQL: профиль владельца с привязанным username
  4. История транзакций: переводы от официального @wallet бота
  5. Fallback: ставим в очередь telethon_resolve_queue (фоновый скан bio)

Результат сохраняется в БД wallet_tg_map и Redis кэш.

Пример ответа TON DNS API:
    {"domains": ["wimp.ton"]}

Пример GraphQL-запроса к GetGems:
    query OwnerProfile($address: Address!) {
      account(address: $address) {
        socialLinks { type url }
      }
    }
    Ответ: {"data": {"account": {"socialLinks": [
        {"type": "telegram", "url": "https://t.me/wimp"}
    ]}}}
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import Any

import orjson
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings
from core.retry import PermanentError
from core.redis_client import cache_get_json, cache_set_json, wallet_key
from db.models import WalletTgMap
from services.collection_discovery import _http_request

TONVIEWER_API = "https://tonviewer.com/api/v2"
GETGEMS_API = "https://api.getgems.io/graphql"

GQL_OWNER_PROFILE = """
query OwnerProfile($address: Address!) {
  account(address: $address) {
    socialLinks { type url }
  }
}
"""

_TG_URL_RE = re.compile(r"t\.me/([a-zA-Z0-9_]{4,32})")


class OwnerResolver:
    """
    Каскадный резолвинг TON-адреса в Telegram-аккаунт.

    DI: session (AsyncSession), redis (Redis client), settings (Settings).
    """

    def __init__(
        self,
        session: AsyncSession,
        redis: Any,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings or get_settings()
        self._rate = asyncio.Semaphore(self.settings.api_max_concurrency)
        self.stats = {"cache": 0, "ton_dns": 0, "getgems": 0, "tx_history": 0, "pending": 0}
        self._dead_until: dict[str, float] = {}  # шаг каскада -> до какого момента пропускаем (API мёртв/закрыт)

    def _skip(self, step: str) -> bool:
        return time.monotonic() < self._dead_until.get(step, 0.0)

    def _mark_dead(self, step: str, exc: BaseException) -> None:
        """Постоянная ошибка (HTTP 4xx): не долбим источник на каждом событии — пауза 10 минут."""
        if isinstance(exc, PermanentError):
            self._dead_until[step] = time.monotonic() + 600
            logger.info(f"[resolver] шаг {step} недоступен ({str(exc)[:80]}…) — пропускаю 10 минут")

    async def resolve(self, ton_address: str) -> dict[str, Any]:
        """
        Резолвить TON-адрес в {tg_user_id, username, source}.

        Возвращает dict с полями:
          tg_user_id: int | None
          username: str | None
          source: str (cache | ton_dns | getgems | tx_history | pending)
        """
        # Шаг 1: Redis кэш
        cached = await cache_get_json(self.redis, wallet_key(ton_address))
        if cached:
            self.stats["cache"] += 1
            return cached

        # Шаг 2: TON DNS
        username = await self._check_ton_dns(ton_address)
        if username:
            result = await self._save(ton_address, None, username, "ton_dns")
            self.stats["ton_dns"] += 1
            return result

        # Шаг 3: GetGems профиль
        username = await self._check_getgems(ton_address)
        if username:
            result = await self._save(ton_address, None, username, "getgems")
            self.stats["getgems"] += 1
            return result

        # Шаг 4: история транзакций
        tg_id = await self._check_tx_history(ton_address)
        if tg_id:
            result = await self._save(ton_address, tg_id, None, "tx_history")
            self.stats["tx_history"] += 1
            return result

        # Шаг 5: fallback — очередь на Telethon
        await self.redis.lpush("telethon_resolve_queue", ton_address)
        self.stats["pending"] += 1
        return {"tg_user_id": None, "username": None, "source": "pending"}

    async def _check_ton_dns(self, ton_address: str) -> str | None:
        """Проверяем TON DNS: домен вида username.ton -> username."""
        if not self.settings.tonviewer_enabled or self._skip("ton_dns"):
            return None  # API Tonviewer закрыт (TONVIEWER_ENABLED=false) — шаг пропускаем
        url = f"{TONVIEWER_API}/accounts/{ton_address}/dns"
        headers = {}
        if self.settings.tonviewer_api_key:
            headers["X-Api-Key"] = self.settings.tonviewer_api_key

        try:
            raw = await _http_request(
                self._rate, "GET", url, headers=headers,
                context=f"tonviewer.dns({ton_address[:12]}…)",
            )
            payload = orjson.loads(raw)
            domains = payload.get("domains") or []
            for domain in domains:
                if isinstance(domain, str) and domain.endswith(".ton"):
                    return domain[:-4]  # убираем ".ton"
        except Exception as exc:  # noqa: BLE001
            self._mark_dead("ton_dns", exc)
            logger.debug(f"TON DNS для {ton_address[:12]}…: {exc!r}")
        return None

    async def _check_getgems(self, ton_address: str) -> str | None:
        """Проверяем GetGems профиль: socialLinks с t.me/username."""
        if self._skip("getgems"):
            return None
        try:
            raw = await _http_request(
                self._rate, "POST", GETGEMS_API,
                json_body={"query": GQL_OWNER_PROFILE, "variables": {"address": ton_address}},
                context=f"getgems.ownerProfile({ton_address[:12]}…)",
            )
            payload = orjson.loads(raw)
            account = (payload.get("data") or {}).get("account") or {}
            for link in account.get("socialLinks") or []:
                url = link.get("url") or ""
                m = _TG_URL_RE.search(url)
                if m:
                    return m.group(1)
        except Exception as exc:  # noqa: BLE001
            self._mark_dead("getgems", exc)
            logger.debug(f"GetGems profile для {ton_address[:12]}…: {exc!r}")
        return None

    async def _check_tx_history(self, ton_address: str) -> int | None:
        """
        Проверяем историю транзакций: ищем переводы от официального @wallet.

        В comment транзакции часто лежит user_id Telegram.
        """
        if not self.settings.tonviewer_enabled or self._skip("tx_history"):
            return None
        url = f"{TONVIEWER_API}/accounts/{ton_address}/transactions"
        params = {"limit": "50"}
        headers = {}
        if self.settings.tonviewer_api_key:
            headers["X-Api-Key"] = self.settings.tonviewer_api_key

        try:
            raw = await _http_request(
                self._rate, "GET", url, params=params, headers=headers,
                context=f"tonviewer.tx({ton_address[:12]}…)",
            )
            payload = orjson.loads(raw)
            transactions = payload if isinstance(payload, list) else []
            for tx in transactions:
                in_msg = tx.get("in_msg") or {}
                comment = in_msg.get("comment") or ""
                # Comment вида "123456789" (user_id) или "user_id:..."
                m = re.match(r"^(\d{6,12})", comment.strip())
                if m:
                    return int(m.group(1))
        except Exception as exc:  # noqa: BLE001
            self._mark_dead("tx_history", exc)
            logger.debug(f"TX history для {ton_address[:12]}…: {exc!r}")
        return None

    async def _save(
        self, ton_address: str, tg_user_id: int | None, username: str | None, source: str
    ) -> dict[str, Any]:
        """Сохраняет результат в БД wallet_tg_map и Redis кэш."""
        result = {"tg_user_id": tg_user_id, "username": username, "source": source}

        # БД: upsert по PK ton_address
        stmt = select(WalletTgMap).where(WalletTgMap.ton_address == ton_address)
        row = (await self.session.execute(stmt)).scalar_one_or_none()
        if row is None:
            self.session.add(
                WalletTgMap(
                    ton_address=ton_address,
                    tg_user_id=tg_user_id,
                    username=username,
                    source=source,
                    confidence=80 if username else 60,
                )
            )
        else:
            row.tg_user_id = tg_user_id or row.tg_user_id
            row.username = username or row.username
            row.source = source
        await self.session.flush()

        # Redis кэш (TTL 1h)
        await cache_set_json(self.redis, wallet_key(ton_address), result, ttl=3600)

        logger.info(f"Resolved {ton_address[:12]}… -> {username or tg_user_id} ({source})")
        return result
