# services/telegram_upgrades.py — новые апгрейды подарков со стороны Telegram, с контактом владельца
"""
Апгрейд-события из блокчейна приходят только когда подарок выведен в TON, а владелец там — кошелёк без Telegram-аккаунта.
Сам Telegram отдаёт владельца улучшенного подарка (`owner_id` / `owner_name`) сразу после апгрейда. Номера подарков
в коллекции идут подряд (`PlushPepe-1`, `PlushPepe-2`, …), поэтому новый апгрейд = подарок со следующим номером:

  1. `payments.getUniqueStarGift(slug=Title-N)` для последнего известного N -> `availability_issued` (сколько всего
     улучшено) — стартовая точка (`tg_up:last:{cid}`), событий при этом нет;
  2. каждый цикл спрашиваем `Title-(last+1)`: подарок есть — это новый апгрейд -> событие в `nft_upgrades` с контактом
     владельца (`owner_tg_id` / `owner_username` / `owner_name`), `last += 1`; нет (STARGIFT_SLUG_INVALID) — апгрейдов нет;
  3. «горячие» коллекции (фильтры/избранное) опрашиваются каждый круг, «холодные» — по несколько за круг.

После долгого простоя отставание больше `TG_UPGRADES_SKIP_GAP` пропускается (старые апгрейды не рассылаем).
Дубли с ончейн-событиями гасит диспетчер: дедупликация апгрейда по (коллекция, номер).
"""
from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from core.config import Settings
from core.redis_client import STREAM_NFT_UPGRADES, stream_publish
from services.owner_lookup import gift_slug
from services.telegram_market import parse_resale_gift

LAST_KEY = "tg_up:last:{cid}"
STATE_TTL = 30 * 24 * 3600


def slug_for(title: str, num: int) -> str:
    """'Plush Pepe', 727 -> 'PlushPepe-727' (так же строит slug и gift_slug)."""
    return gift_slug(f"{title} #{num}") or ""


def _is_not_found(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".upper()
    return "SLUG" in text or "NOT_FOUND" in text or "INVALID" in text


class TelegramUpgradeWatcher:
    def __init__(self, client: Any, redis: Any, market: Any, settings: Settings) -> None:
        self.client = client
        self.redis = redis
        self.market = market  # TelegramMarketWatcher: gift_ids, collections, hot, cold
        self.settings = settings
        self._cold_pos = 0
        self.stats = {"probes": 0, "upgrades": 0, "bootstrapped": 0, "skipped_gap": 0, "errors": 0, "flood_waits": 0}

    async def _fetch(self, slug: str) -> Any | None:
        """Подарок по slug или None, если такого номера ещё нет."""
        from telethon import errors
        from telethon.tl import functions

        self.stats["probes"] += 1
        try:
            return await self.client(functions.payments.GetUniqueStarGiftRequest(slug=slug))
        except errors.FloodWaitError as exc:
            self.stats["flood_waits"] += 1
            logger.warning(f"[tg_upgrades] FloodWait {exc.seconds}s")
            await asyncio.sleep(exc.seconds + 1)
            return None
        except Exception as exc:  # noqa: BLE001
            if not _is_not_found(exc):
                self.stats["errors"] += 1
                logger.debug(f"[tg_upgrades] {slug}: {exc!r}")
            return None

    @staticmethod
    def _unpack(res: Any) -> dict[str, Any] | None:
        users = {u.id: u for u in getattr(res, "users", None) or []}
        chats = {c.id: c for c in getattr(res, "chats", None) or []}
        return parse_resale_gift(res.gift, users, chats)

    async def _last(self, cid: Any) -> int:
        raw = await self.redis.get(LAST_KEY.format(cid=cid))
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    async def _set_last(self, cid: Any, n: int) -> None:
        await self.redis.set(LAST_KEY.format(cid=cid), str(n).encode(), ex=STATE_TTL)

    async def poll_collection(self, cid: Any) -> int:
        """Новые апгрейды коллекции -> события (с контактом владельца). Возвращает число событий."""
        name, _address = self.market.collections[cid]
        last = await self._last(cid)
        if last <= 0:  # стартовая точка: сколько подарков уже улучшено
            res = await self._fetch(slug_for(name, 1))
            item = self._unpack(res) if res is not None else None
            if item is None or not item.get("issued"):
                return 0
            await self._set_last(cid, int(item["issued"]))
            self.stats["bootstrapped"] += 1
            return 0
        published = 0
        for _ in range(self.settings.tg_upgrades_max_per_poll):
            res = await self._fetch(slug_for(name, last + 1))
            item = self._unpack(res) if res is not None else None
            if item is None:
                break
            issued = int(item.get("issued") or 0)
            if issued - (last + 1) > self.settings.tg_upgrades_skip_gap:
                # долго не опрашивали: старые апгрейды не рассылаем, прыгаем к свежим
                self.stats["skipped_gap"] += issued - last - 1
                last = max(issued - self.settings.tg_upgrades_max_per_poll, last)
                await self._set_last(cid, last)
                break
            await stream_publish(self.redis, STREAM_NFT_UPGRADES, self.event(cid, name, item))
            last += 1
            await self._set_last(cid, last)
            published += 1
            self.stats["upgrades"] += 1
        return published

    @staticmethod
    def event(cid: Any, name: str, item: dict[str, Any]) -> dict[str, Any]:
        title = item["title"] or name
        return {
            "type": "mint", "source": "telegram", "collection_id": str(cid),
            "nft_address": item["gift_address"] or item["slug"], "index": item["num"], "owner_ton": "",
            "slug": item["slug"], "owner_tg_id": item["seller_tg_id"], "owner_username": item["seller_username"],
            "owner_name": item["seller_name"],
            "metadata": {"name": f"{title} #{item['num']}", **item["attributes"]},
        }

    def next_collections(self) -> list[Any]:
        """Круг опроса: все горячие + несколько холодных по очереди."""
        cold = self.market.cold
        k = self.settings.tg_upgrades_cold_per_round
        part = cold[self._cold_pos:self._cold_pos + k]
        if cold:
            self._cold_pos = (self._cold_pos + k) % len(cold)
        return list(self.market.hot) + part

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("[tg_upgrades] старт: новые апгрейды из Telegram с контактом владельца")
        rounds = 0
        while not stop_event.is_set():
            batch = self.next_collections()
            if not batch:
                await asyncio.sleep(5)  # каталог маркета ещё не загружен
                continue
            for cid in batch:
                if stop_event.is_set():
                    break
                try:
                    await self.poll_collection(cid)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    self.stats["errors"] += 1
                    logger.warning(f"[tg_upgrades] {exc!r}")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.settings.tg_upgrades_interval)
                except asyncio.TimeoutError:
                    pass
            rounds += 1
            if rounds % 50 == 0:
                logger.info(f"[tg_upgrades] статистика: {self.stats}")
        logger.info(f"[tg_upgrades] остановлен: {self.stats}")
