# /workspace/services/collection_discovery.py — МОДУЛЬ 1: автообнаружение коллекций и атрибутов
"""
Collection Discovery (ЭТАП 2, ТЗ «МОДУЛЬ 1»).

Источники коллекций:
  A) Telegram Bot API 8.2+: GET https://api.telegram.org/bot{token}/getAvailableGifts
     -> список подарков (stars_price, upgrade_stars_cost, total_count).
  B) GetGems GraphQL: POST https://api.getgems.io/graphql
     query nftCollections(tags:["telegram_gifts"]) -> EQ-адрес контракта, имя, floor.
     Fallback при ошибке: Tonviewer API (/api/v2/search?query=gift).
  C) Парсинг атрибутов: первые N (по умолчанию 100) NFT коллекции через pytoniq:
        get_nft_address_by_index(i) -> get_nft_data(owner/content) -> get_nft_content(metadata)
     скачиваем metadata JSON (https:// | ipfs:// -> gateway), извлекаем attributes[],
     маппим trait_type -> model/background/symbol/pattern (core.config.TRAIT_ALIASES).
  D) Upsert в БД: collections + attributes БЕЗ дубликатов, обновление items_count.

Пример ответа getAvailableGifts (урезанно):
    {"ok": true, "result": {"gifts": [
        {"sticker": {"emoji": "🧸", "set_name": "teddyv2_Telegrambots"},
         "total_count": 300000, "available_count": 57645,
         "upgrade_stars_cost": 35000, "stars_price": 5000,
         "base_name": "Teddy", "is_limited": true,
         "successor": {"name": "Teddy v2", "total_count": 5000}}
    ]}}

Пример GraphQL-запроса к GetGems:
    query {
      nftCollections(input: {tags: ["telegram_gifts"], limit: 50}) {
        edges { node { address name description icon { url }
                       statistics { itemsCount minPrice { amount { value } } } } }
      }
    }

Пример metadata JSON одного NFT (Telegram gift on TON):
    {"name": "Teddy #1234", "image": "ipfs://Qm...",
     "attributes": [
        {"trait_type": "Model",      "value": "Brown Bear"},
        {"trait_type": "Background", "value": "Red Scarf"},
        {"trait_type": "Symbol",     "value": "Fire"},
        {"trait_type": "Pattern",    "value": "Pixel"}
     ]}
"""
from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any

import aiohttp
import orjson
from loguru import logger
from sqlalchemy import Text, func, select
from sqlalchemy import update as pg_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, get_settings, map_trait_type
from core.redis_client import cache_set_json, collection_attrs_key
from core.retry import with_retry
from db.models import Attribute, Collection

# nanoton (pytoniq-core) — ленивый импорт: нужен только на боевом пути блокчейна,
# в unit-тестах подменяется фикстурами (см. tests/test_collection_discovery.py).
try:  # pragma: no cover - guard для окружения без нативных либов
    from pytoniq_core import Address, Builder, Cell, Slice
except ImportError:  # pragma: no cover
    Address = None  # type: ignore[assignment,misc]
    Builder = Cell = Slice = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# DTO результатов обнаружения
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DiscoveredAttribute:
    """Один трейт из метаданных NFT после маппинга (type уже канонический)."""

    type: str  # model | background | symbol | pattern
    value: str
    icon_url: str | None = None


@dataclass(slots=True)
class DiscoveredCollection:
    """Коллекция/подарок, найденный в одном из источников."""

    name: str
    contract_address: str  # EQ... (workchain 0); для telegram_api — эрзац-ключ tg_gift:{base_name}
    source: str  # telegram_api | getgems | manual
    icon_url: str | None = None
    floor_price: Decimal = Decimal(0)
    items_count: int = 0
    attributes: list[DiscoveredAttribute] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["floor_price"] = str(self.floor_price)  # Decimal не сериализуется в JSON напрямую
        return d

    def content_fingerprint(self) -> str:
        """
        Отпечаток «полезного» содержимого (без last_updated): совпадает => повторный
        upsert не нужен. Атрибуты включены — их состав тоже часть отпечатка.
        """
        parts = [self.name, self.source, self.icon_url or "", str(self.floor_price), str(self.items_count)]
        parts += sorted(f"{a.type}={a.value}" for a in self.attributes)
        return "|".join(parts)


# ---------------------------------------------------------------------------
# GraphQL-запросы GetGems (документированы в докстрингах методов)
# ---------------------------------------------------------------------------
GQL_COLLECTIONS = """
query GiftCollections($limit: Int!, $cursor: String) {
  nftCollections(input: {tags: ["telegram_gifts"], limit: $limit, cursor: $cursor}) {
    edges {
      node {
        address            # EQ... адрес коллекционного контракта
        name
        description
        icon { url }
        statistics {
          itemsCount       # всего минтов
          minPrice { amount { value } }   # floor в TON (строка-дробь)
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


class CollectionDiscovery:
    """
    Оркестратор discovery-цикла. Один экземпляр на процесс updater;
    session/redis внедряются извне (для тестируемости).
    """

    # Ключ-префикс для «коллекций» из Bot API, у которых нет известного EQ-адреса
    TG_GIFT_PREFIX = "tg_gift:"

    def __init__(
        self,
        session: AsyncSession,
        redis: Any | None = None,
        settings: Settings | None = None,
        blockchain: Any | None = None,
    ) -> None:
        self.session = session
        self.redis = redis
        self.settings = settings or get_settings()
        # external_rate limiter — общий семафор на все HTTP-вызовы модуля
        self._rate = asyncio.Semaphore(self.settings.api_max_concurrency)
        # pytoniq LiteClient (или mock в тестах); None => парсинг атрибутов пропускается
        self.blockchain = blockchain
        # Кэш соответствия base_name подарка -> EQ-адрес коллекции (из GetGems)
        self._gift_to_contract: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Источник A: Telegram Bot API
    # ------------------------------------------------------------------
    async def fetch_telegram_gifts(self) -> list[DiscoveredCollection]:
        """
        GET /bot{token}/getAvailableGifts (Bot API 8.2+).
        Маппинг Gift -> DiscoveredCollection(source='telegram_api'):
          name        <- successor.name или base_name
          address     <- эрзац tg_gift:{base_name} (EQ-адрес подставит GetGems-этап)
          floor_price <- stars_price (условная «цена входа»)
          items_count <- total_count
        """
        if not self.settings.bot_token:
            logger.warning("BOT_TOKEN пуст — пропускаю getAvailableGifts")
            return []
        url = f"{self.settings.telegram_api_base}/bot{self.settings.bot_token}/getAvailableGifts"
        payload = await self._get_json(url, context="telegram.getAvailableGifts")
        # _get_json уже разворачивает {"ok":..,"result":{...}} -> здесь {"gifts": [...]}
        gifts: list[dict[str, Any]] = (payload.get("gifts") if isinstance(payload.get("gifts"), list)
                                       else ((payload.get("result") or {}).get("gifts", [])))
        result: list[DiscoveredCollection] = []
        for g in gifts:
            base_name = g.get("base_name") or ""
            name = (g.get("successor") or {}).get("name") or base_name or g.get("short_name") or "Unknown"
            if not base_name:
                continue
            icon = ((g.get("sticker") or {}).get("thumb") or {}).get("file_path")
            result.append(
                DiscoveredCollection(
                    name=name,
                    contract_address=f"{self.TG_GIFT_PREFIX}{base_name}",
                    source="telegram_api",
                    icon_url=icon,
                    floor_price=Decimal(g.get("stars_price") or 0),
                    items_count=int(g.get("total_count") or 0),
                )
            )
        logger.info(f"Telegram Bot API: найдено {len(result)} подарков")
        return result

    # ------------------------------------------------------------------
    # Источник B: GetGems GraphQL (+ Tonviewer fallback)
    # ------------------------------------------------------------------
    async def fetch_getgems_collections(self) -> list[DiscoveredCollection]:
        """
        POST GraphQL nftCollections(tags:["telegram_gifts"]) (см. GQL_COLLECTIONS).

        Пример фрагмента ответа:
            {"data": {"nftCollections": {"edges": [{"node": {
                "address": "EQDTwwzDZK39EYHdBcnd_A6CnpQQGwRqLXjxcJkeN2vPXLkY",
                "name": "Teddy", "icon": {"url": "https://cdn.getgems.io/..."},
                "statistics": {"itemsCount": 300000,
                               "minPrice": {"amount": {"value": "5.5"}}}}}], ...}}}

        При недоступности GetGems — fallback на Tonviewer API.
        """
        try:
            data = await self._graphql(GQL_COLLECTIONS, {"limit": 100}, context="getgems.nftCollections")
        except Exception as exc:  # noqa: BLE001 — любой сбой => fallback
            logger.warning(f"GetGems недоступен ({exc!r}), использую Tonviewer fallback")
            return await self._fetch_tonviewer_fallback()

        edges = ((data.get("nftCollections") or {}).get("edges")) or []
        result: list[DiscoveredCollection] = []
        for e in edges:
            node = e.get("node") or {}
            addr = node.get("address")
            if not addr:
                continue
            stats = node.get("statistics") or {}
            floor_raw = (((stats.get("minPrice") or {}).get("amount") or {}).get("value")) or "0"
            result.append(
                DiscoveredCollection(
                    name=node.get("name") or "Unknown",
                    contract_address=addr,
                    source="getgems",
                    icon_url=(node.get("icon") or {}).get("url"),
                    floor_price=_to_decimal(floor_raw),
                    items_count=int(stats.get("itemsCount") or 0),
                )
            )
        logger.info(f"GetGems: найдено {len(result)} коллекций telegram_gifts")
        return result

    async def _fetch_tonviewer_fallback(self) -> list[DiscoveredCollection]:
        """Fallback: Tonviewer search по слову 'gift' (минимум: адрес + имя + floor)."""
        url = "https://tonviewer.com/api/v2/search"
        params = {"query": "gift", "limit": "50"}
        payload = await self._get_json(
            url, context="tonviewer.search", params=params,
            headers={"X-Api-Key": self.settings.tonviewer_api_key} if self.settings.tonviewer_api_key else None,
        )
        accounts = payload.get("accounts") or payload.get("result") or []
        result: list[DiscoveredCollection] = []
        for a in accounts:
            addr = a.get("address") or a.get("workchain") and a.get("hex")
            name = a.get("name")
            if not addr or not name:
                continue
            result.append(
                DiscoveredCollection(
                    name=name,
                    contract_address=addr,
                    source="getgems",  # CHECK в БД допускает только telegram_api/getgems/manual
                    floor_price=_to_decimal(a.get("floor_price") or 0),
                )
            )
        logger.info(f"Tonviewer fallback: {len(result)} коллекций")
        return result

    # ------------------------------------------------------------------
    # Источник C: парсинг атрибутов коллекции через pytoniq
    # ------------------------------------------------------------------
    async def parse_collection_attributes(self, collection: DiscoveredCollection) -> list[DiscoveredAttribute]:
        """
        Для коллекции берём первые settings.attributes_sample_size NFT:
            1. get_nft_address_by_index(i)          (метод коллекционного контракта)
            2. get_nft_data()                        -> (owner, content Cell)
            3. get_nft_content(content_cell)         -> metadata URI (https/ipfs/data)
            4. скачиваем JSON, извлекаем attributes[]
        trait_type -> канонический тип через map_trait_type(); unmapped игнорируются.
        Возвращает уникальные (type, value) по выборке.
        """
        if self.blockchain is None:
            logger.debug(f"Blockchain-клиент не подключён — атрибуты для '{collection.name}' пропускаю")
            return []
        if collection.contract_address.startswith(self.TG_GIFT_PREFIX):
            return []  # у эрзац-коллекций нет контракта

        attributes: dict[tuple[str, str], DiscoveredAttribute] = {}
        n = self.settings.attributes_sample_size
        tasks = [self._parse_one_nft(collection.contract_address, i) for i in range(n)]
        # parallelism ограничивается внутренним семафором _rate (HTTP-часть)
        for chunk in _chunks(tasks, size=self.settings.api_max_concurrency * 2):
            for attrs in await asyncio.gather(*chunk, return_exceptions=False):
                for attr in attrs:
                    attributes.setdefault((attr.type, attr.value), attr)
        out = list(attributes.values())
        logger.info(f"'{collection.name}': просэмплировано {n} NFT, уникальных трейтов: {len(out)}")
        return out

    async def _parse_one_nft(self, contract_addr: str, index: int) -> list[DiscoveredAttribute]:
        """Один NFT: адрес -> content -> metadata JSON -> трейты. Ошибки не фатальны."""
        try:
            # --- шаг 1: get_nft_address_by_index(index) на контракте коллекции ---
            # method_id = CRC16('get_nft_address_by_index') << 1 | 1 = 0x4b3f5f89; аргумент — index (int)
            nft_addr_raw = await self._run_method(
                contract_addr, 0x4B3F5F89, index, method="get_nft_address_by_index"
            )
            nft_addr = _slice_to_address(nft_addr_raw)
            if nft_addr is None:
                return []
            nft_addr_str = str(nft_addr)
            # --- шаг 2: get_nft_data() -> (queryId, owner, content Cell, ...) ---
            # method_id = CRC16('get_nft_data')<<1|1 = 0x57cf1c6e
            data = await self._run_method(nft_addr_str, 0x57CF1C6E, 0, method="get_nft_data")
            content_cell = _find_content_cell(data)
            if content_cell is None:
                return []
            # --- шаг 3: get_nft_content(index, contentCell) -> metadata URI ---
            # method_id = CRC16('get_nft_content')<<1|1 = 0x73c607df
            uri_parts = await self._run_method(
                nft_addr_str, 0x73C607DF, index, content_cell, method="get_nft_content"
            )
            metadata_uri = _slice_to_string(uri_parts)
            # --- шаг 4: скачать metadata JSON и разобрать attributes ---
            meta = await self._fetch_metadata_json(metadata_uri)
            return _extract_attributes(meta)
        except Exception as exc:  # noqa: BLE001 — один битый NFT не роняет весь цикл
            logger.debug(f"NFT #{index} коллекции {contract_addr}: разбор не удался: {exc!r}")
            return []

    async def _run_method(self, addr: str, method_id: int, *args: Any, method: str = "") -> Any:
        """Вызов get-method через внедрённый blockchain-клиент (LiteClient/pytoniq)."""
        async with self._rate:
            return await with_retry(
                self.blockchain.run_method, addr, method_id, *args,
                context=f"run_method({method} @ {addr[:12]}…)",
            )

    async def _fetch_metadata_json(self, uri: str) -> dict[str, Any]:
        """Скачать metadata по URI: https(s):// | ipfs:// (через gateway) | data:...;base64 (ончейн)."""
        import base64

        # data-URI декодируем без сети
        m = _DATA_JSON_B64_RE.match(uri.strip())
        if m:
            return orjson.loads(base64.b64decode(m.group(1)))
        url = normalize_metadata_uri(uri, gateway=self.settings.ipfs_gateway)
        if url is None:
            raise ValueError(f"Неподдерживаемый metadata URI: {uri[:80]}")
        raw_bytes = await self._get_bytes(url, context="metadata.fetch")
        return orjson.loads(raw_bytes)

    # ------------------------------------------------------------------
    # Источник D: upsert в БД (идемпотентный)
    # ------------------------------------------------------------------
    async def discover_all(self) -> dict[str, int]:
        """
        Полный цикл: источники A+B -> merge -> (опц.) парсинг атрибутов C -> upsert D.
        Повторный запуск НЕ создаёт дубликатов (ON CONFLICT по contract_address /
        (collection_id,type,value)); обновляет items_count/icon/floor и last_updated.
        Возвращает статистику {"collections": n, "attributes": m}.
        """
        tg_gifts, gg_cols = await asyncio.gather(
            self.fetch_telegram_gifts(), self.fetch_getgems_collections()
        )

        # Merge: GetGems даёт EQ-адреса; пробуем связать tg_gift:{base_name} с EQ по имени
        by_name = {_norm_name(c.name): c for c in gg_cols}
        merged: dict[str, DiscoveredCollection] = {}
        for c in gg_cols:
            merged[c.contract_address] = c
        for g in tg_gifts:
            key_name = _norm_name(g.name)
            match = by_name.get(key_name)
            if match:  # подарок из Bot API == коллекция GetGems: запоминаем соответствие
                self._gift_to_contract[g.contract_address] = match.contract_address
                continue  # дубль не добавляем — берём запись GetGems с реальным адресом
            merged.setdefault(g.contract_address, g)

        attr_total = 0
        collections_out: list[dict[str, Any]] = []
        for coll in merged.values():
            # Атрибуты парсим только для коллекций без них и если есть blockchain-клиент
            if not coll.attributes and self.blockchain is not None:
                coll.attributes = await self.parse_collection_attributes(coll)
            coll_uuid = await self._upsert_collection(coll)
            attr_total += await self._upsert_attributes(coll_uuid, coll.attributes)
            collections_out.append({"id": str(coll_uuid), **coll.to_dict()})

        # Кэш collection_attrs:{id} (Redis, TTL 1h) — для TWA-автокомплита и диспетчера
        if self.redis is not None:
            for item in collections_out:
                await cache_set_json(
                    self.redis,
                    collection_attrs_key(item["id"]),
                    item,
                    ttl=self.settings.attrs_cache_ttl,
                )

        await self.session.commit()
        stats = {"collections": len(collections_out), "attributes": attr_total}
        logger.info(f"Discovery завершён: {stats}")
        return stats

    async def _upsert_collection(self, coll: DiscoveredCollection) -> uuid.UUID:
        """
        INSERT .. ON CONFLICT (contract_address) DO UPDATE -> id коллекции.
        Идемпотентность: если содержимое не изменилось (content_fingerprint), строку
        НЕ трогаем — last_updated отражает реальные изменения данных.
        """
        fp = coll.content_fingerprint()
        stmt = (
            pg_insert(Collection.__table__)
            .values(
                name=coll.name,
                contract_address=coll.contract_address,
                icon_url=coll.icon_url,
                source=coll.source,
                floor_price=coll.floor_price,
                items_count=coll.items_count,
                last_updated=func.now(),
            )
            .on_conflict_do_update(
                index_elements=[Collection.__table__.c.contract_address],
                set_={
                    "name": coll.name,
                    "icon_url": coll.icon_url,
                    # telegram_api не перетирает getgems-запись (источник с апстрима не понижаем)
                    "source": Collection.__table__.c.source,
                    "items_count": coll.items_count,
                    "last_updated": func.now(),
                },
                # WHERE: обновляем только при фактическом изменении содержимого
                where=(
                    Collection.__table__.c.name
                    + "|"
                    + Collection.__table__.c.source
                    + "|"
                    + func.coalesce(Collection.__table__.c.icon_url, "")
                    + "|"
                    + func.cast(Collection.__table__.c.floor_price, Text)
                    + "|"
                    + func.cast(Collection.__table__.c.items_count, Text)
                    != fp
                ),
            )
            .returning(Collection.__table__.c.id)
        )
        res = await self.session.execute(stmt)
        row = res.fetchone()
        if row is not None:
            return uuid.UUID(str(row[0]))
        # Строка существует, но изменений нет. Разные СУБД по-разному ведут себя с
        # RETURNING при DO UPDATE ... WHERE false (SQLite возвращает старую строку),
        # поэтому сверяем отпечаток из БД и при расхождении делаем явный UPDATE —
        # так поведение идентично на PG (прод) и SQLite (тесты).
        existing = (
            await self.session.execute(
                select(Collection.id, Collection.name, Collection.source, Collection.icon_url,
                       Collection.floor_price, Collection.items_count)
                .where(Collection.contract_address == coll.contract_address)
            )
        ).one()
        db_fp = "|".join([
            existing[1], existing[2], existing[3] or "", str(existing[4]), str(existing[5]),
        ])
        if db_fp != fp:
            await self.session.execute(
                pg_update(Collection.__table__)
                .where(Collection.__table__.c.id == existing[0])
                .values(name=coll.name, icon_url=coll.icon_url, items_count=coll.items_count,
                        floor_price=coll.floor_price, last_updated=func.now())
            )
        return uuid.UUID(str(existing[0]))

    async def _upsert_attributes(self, collection_id: uuid.UUID, attrs: list[DiscoveredAttribute]) -> int:
        """Upsert атрибутов (UNIQUE collection_id+type+value). Возвращает число затронутых."""
        if not attrs:
            return 0
        stmt = (
            pg_insert(Attribute.__table__)
            .values([
                {
                    "id": uuid.uuid4(),
                    "collection_id": collection_id,
                    "type": a.type,
                    "value": a.value,
                    "icon_url": a.icon_url,
                }
                for a in attrs
            ])
            .on_conflict_do_update(
                index_elements=["collection_id", "type", "value"],
                set_={"icon_url": pg_insert(Attribute.__table__).excluded.icon_url},
            )
            .returning(Attribute.__table__.c.id)
        )
        res = await self.session.execute(stmt)
        return len(res.fetchall())


# ---------------------------------------------------------------------------
# Вспомогательные чистые функции (легко тестируются)
# ---------------------------------------------------------------------------

_IPFS_RE = re.compile(r"^ipfs://(/ipfs/)?(.+)$", re.IGNORECASE)
_DATA_JSON_B64_RE = re.compile(r"^data:application/json;base64,(.+)$", re.IGNORECASE | re.DOTALL)


def normalize_metadata_uri(uri: str, gateway: str = "https://ipfs.io/ipfs/") -> str | None:
    """
    Нормализация metadata URI в скачиваемый http(s)-URL:
        ipfs://QmAbc...        -> {gateway}QmAbc...
        ipfs://ipfs/QmAbc...   -> {gateway}QmAbc...  (двойной путь убирается)
        https://meta.io/1.json -> как есть
        data:application/json;base64,eyJ... -> None (декодируется decode_data_uri без сети)
    """
    uri = uri.strip()
    m = _IPFS_RE.match(uri)
    if m:
        path = m.group(2)
        # защита от двойного /ipfs/: ipfs://ipfs/Qm... и ipfs:///ipfs/Qm...
        while path.startswith("ipfs/"):
            path = path[len("ipfs/") :]
        return gateway.rstrip("/") + "/" + path
    if uri.startswith(("http://", "https://")):
        return uri
    return None


def decode_data_uri(uri: str) -> bytes | None:
    """Поддержка data-URI в content (base64 JSON лежит прямо в ончейне)."""
    m = _DATA_JSON_B64_RE.match(uri.strip())
    if not m:
        return None
    import base64

    return base64.b64decode(m.group(1))


def _extract_attributes(metadata: dict[str, Any]) -> list[DiscoveredAttribute]:
    """
    Из metadata JSON берём attributes[]; trait_type/name -> канонический тип.
    Пример элемента: {"trait_type": "Character", "value": "Panda"} -> DiscoveredAttribute('model','Panda')
    """
    out: list[DiscoveredAttribute] = []
    for a in metadata.get("attributes", []) or []:
        trait_type = a.get("trait_type") or a.get("name") or ""
        value = a.get("value")
        if not isinstance(value, str) or not value.strip():
            continue
        mapped = map_trait_type(str(trait_type))
        if mapped is None:
            continue  # Rarity/Edition и т.п. — нас не интересуют
        out.append(DiscoveredAttribute(type=mapped, value=value.strip()))
    return out


def _to_decimal(value: Any) -> Decimal:
    """Безопасное приведение '5.5'/5/None -> Decimal (NaN/inf -> 0)."""
    try:
        d = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return Decimal(0)
    if not d.is_finite():
        return Decimal(0)
    return d


def _norm_name(name: str) -> str:
    """Нормализация имени для сопоставления Bot API <-> GetGems."""
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def _chunks(seq: list[Any], size: int) -> list[list[Any]]:
    """Разбиение списка на чанки (для пакетного gather)."""
    return [seq[i : i + size] for i in range(0, len(seq), size)]


# ---------------------------------------------------------------------------
# Мост к pytoniq (только на боевом пути; вынесен, чтобы mocking был точечным)
# ---------------------------------------------------------------------------

def _slice_to_address(part: Any) -> Any | None:
    """Slice/Address/str -> pytoniq Address (или строка-маркер для mock'ов), None при мусоре."""
    if part is None:
        return None
    # Уже Address из pytoniq
    if hasattr(part, "toStr") or hasattr(part, "writeToBaseChain"):
        return part
    # Slice -> load_addr()
    if hasattr(part, "load_addr"):
        try:
            return part.load_addr()
        except Exception:  # noqa: BLE001
            return None
    # Строка (EQ_/UQ_ base64url или raw hex) — в тестах это mock-адрес 'EQnft{i}'
    s = str(part).strip()
    if s.startswith(("EQ", "UQ", "0:", "-", "x")) and len(s) >= 8:
        if Address is not None:
            try:
                return Address(s)  # type: ignore[misc]
            except Exception:  # noqa: BLE001 — невалидный адрес => оставляем строку ниже
                pass
        return s
    return None


def _slice_to_string(parts: Any) -> str:
    """Из стека результатов get_nft_content достать строковый metadata URI."""
    cell = parts[-1] if isinstance(parts, (list, tuple)) else parts
    sl = cell.begin_parse() if hasattr(cell, "begin_parse") else cell
    try:
        return sl.load_str()
    except Exception:  # noqa: BLE001 — onion/external reference форматы разберём позже
        return str(sl)


def _find_content_cell(parts: Any) -> Any | None:
    """Из результатов get_nft_data найти Cell с content (первый ref в стеке после owner)."""
    seq = parts if isinstance(parts, (list, tuple)) else [parts]
    for p in reversed(seq):
        if hasattr(p, "begin_parse"):
            return p
    return None


# ---------------------------------------------------------------------------
# HTTP-обёртки (семафор + ретраи)
# ---------------------------------------------------------------------------

async def _http_request(
    rate: asyncio.Semaphore,
    method: str,
    url: str,
    *,
    json_body: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 20.0,
    context: str = "",
) -> bytes:
    """Единая точка выхода в интернет: семафор + exponential backoff (core/retry)."""

    async def _do() -> bytes:
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        async with rate:
            async with aiohttp.ClientSession(timeout=client_timeout, headers=headers) as sess:
                async with sess.request(method, url, json=json_body, params=params) as resp:
                    if resp.status >= 400:
                        body = (await resp.text())[:300]
                        raise RuntimeError(f"HTTP {resp.status} {context}: {body}")
                    return await resp.read()

    return await with_retry(_do, context=context or url)


# Привязываем хелперы к экземпляру (методы класса выше их так используют)
async def _bound_get_json(self: CollectionDiscovery, url: str, context: str = "", **kw: Any) -> dict[str, Any]:
    raw = await _http_request(self._rate, "GET", url, context=context, **kw)
    payload = orjson.loads(raw)
    # Bot API оборачивает ответ в {"ok":..., "result":...}
    return payload.get("result", payload) if isinstance(payload, dict) else payload


async def _bound_get_bytes(self: CollectionDiscovery, url: str, context: str = "") -> bytes:
    return await _http_request(self._rate, "GET", url, context=context)


async def _bound_graphql(
    self: CollectionDiscovery, query: str, variables: dict[str, Any], context: str = ""
) -> dict[str, Any]:
    """POST GraphQL на settings.getgems_api_url; бросает RuntimeError при errors/HTTP-ошибке."""
    body = {"query": query, "variables": variables}
    raw = await _http_request(
        self._rate, "POST", self.settings.getgems_api_url, json_body=body, context=context
    )
    payload = orjson.loads(raw)
    if isinstance(payload, dict) and payload.get("errors"):
        raise RuntimeError(f"GraphQL errors [{context}]: {payload['errors'][:2]}")
    return payload.get("data", {}) if isinstance(payload, dict) else {}


CollectionDiscovery._get_json = _bound_get_json  # type: ignore[attr-defined]
CollectionDiscovery._get_bytes = _bound_get_bytes  # type: ignore[attr-defined]
CollectionDiscovery._graphql = _bound_graphql  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Точка запуска модуля по команде (python -m services.collection_discovery)
# ---------------------------------------------------------------------------
async def _cli_once() -> None:  # pragma: no cover - ручной запуск
    """Разовый прогон discovery (например, из cron или вручную)."""
    from core.logging import setup_logging
    from core.redis_client import create_redis
    from db.engine import dispose_engine, session_scope

    setup_logging()
    redis = create_redis()
    async with session_scope() as s:
        disc = CollectionDiscovery(session=s, redis=redis)
        stats = await disc.discover_all()
        print(orjson.dumps(stats).decode())
    await redis.aclose()
    await dispose_engine()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(_cli_once())
