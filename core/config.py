# /workspace/core/config.py — конфигурация через pydantic-settings (.env)
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Все настройки сервиса. Загружаются из переменных окружения / .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    
    # --- Окружение ---
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")

    # --- Telegram Bot ---
    bot_token: str = Field(default="", description="Токен бота от @BotFather")

    # --- Telethon (только cold-path resolver) ---
    api_id: int = Field(default=0)
    api_hash: str = Field(default="")
    phone: str = Field(default="")
    telethon_session: str = Field(default="sessions/resolver_session")
    # Маркет подарков Telegram (официальный MTProto, тот же Telethon-аккаунт; сервис resolver)
    # Название нативной монеты в интерфейсе и уведомлениях (TonAPI и GetGems называют её GRAM; было TON)
    coin_label: str = Field(default="GRAM")
    # показывать флор маркета Telegram (отдельной строкой на странице коллекции); на карточках всегда ончейн-флор
    tg_floor_show: bool = Field(default=False)
    # шина событий диспетчера: повторы и забор зависших сообщений
    # лимиты отправки (ТЗ 8.7) и доступ по подписке
    send_rate_global: int = Field(default=25, ge=1)  # сообщений в секунду на весь бот
    send_rate_user_sec: int = Field(default=1, ge=1)
    send_rate_user_min: int = Field(default=20, ge=1)
    subscription_required: bool = Field(default=False)  # True — алерты только пользователям с активной платной подпиской
    dispatch_max_attempts: int = Field(default=5, ge=1)
    dispatch_retry_base: float = Field(default=0.5, ge=0)  # пауза перед повтором: base * 2**попытка, с
    dispatch_claim_idle_ms: int = Field(default=60_000, ge=1000)  # pending дольше — забираем у упавшего потребителя
    dispatch_dedup_ttl: int = Field(default=3600, ge=1)  # дедупликация событий, с
    # флоры MRKT/Portals (вход через их мини-приложения вашим Telegram-аккаунтом; без обхода защиты)
    market_floors_enabled: bool = Field(default=False)
    market_floors_sources: str = Field(default="mrkt,portals")
    market_floors_interval: float = Field(default=1.5, ge=0.5)  # пауза между запросами, с
    market_floors_ttl: int = Field(default=1800, ge=60)
    # Владелец подарка по slug через Telegram (для листингов из блокчейна и апгрейдов)
    owner_lookup_enabled: bool = Field(default=True)
    owner_lookup_timeout: float = Field(default=4.0, ge=0)  # сколько уведомление ждёт ответ воркера, с
    owner_lookup_max_queue: int = Field(default=20, ge=0)  # очередь воркера длиннее — событие не ждёт владельца (0 — без защиты)
    owner_lookup_interval: float = Field(default=1.0, ge=0.2)  # пауза воркера между запросами к Telegram, с
    # Апгрейды из скана: больше стольких новых предметов за один проход чанка — не рассылать (сбой, не апгрейды)
    upgrade_burst_max: int = Field(default=20, ge=1)
    # апгрейды из Telegram (с контактом владельца), см. services/telegram_upgrades.py
    tg_upgrades_enabled: bool = Field(default=True)
    tg_upgrades_interval: float = Field(default=0.8, ge=0.2)  # пауза между запросами к Telegram, с
    tg_upgrades_cold_per_round: int = Field(default=5, ge=0)  # «холодных» коллекций за круг
    tg_upgrades_max_per_poll: int = Field(default=5, ge=1)  # событий с одной коллекции за опрос
    tg_upgrades_skip_gap: int = Field(default=20, ge=1)  # отставание больше — старые апгрейды не рассылаем
    tg_market_enabled: bool = Field(default=True)
    tg_market_interval: float = Field(default=2.0, ge=0.5)  # пауза между запросами листингов, с
    tg_market_catalog_interval: int = Field(default=300, ge=30)  # каталог/флоры маркета, с
    tg_market_page: int = Field(default=30, ge=1, le=100)  # листингов за запрос (новые сверху)
    tg_market_cold_per_round: int = Field(default=3, ge=0)  # «холодных» коллекций за круг
    # Предохранитель от спама: больше стольких «новых» листингов за один опрос коллекции — не рассылать
    tg_market_max_new: int = Field(default=10, ge=1)
    # Анти-FloodWait: пауза между запросами, лимит за цикл, повторный скан не чаще N дней
    bio_scan_delay: float = Field(default=2.0)
    bio_scan_batch: int = Field(default=100)
    bio_rescan_days: int = Field(default=7)
    bio_cycle_seconds: int = Field(default=600)

    # --- БД / Redis ---
    db_url: str = Field(
        default="postgresql+asyncpg://gt:pass@localhost:5432/gifttrack"
    )
    redis_url: str = Field(default="redis://localhost:6379/0")

    # --- TWA ---
    twa_url: str = Field(default="http://localhost:3000")
    # Если пусто — секрет вычисляется автоматически из bot_token
    twa_auth_secret: str = Field(default="")

    # --- CryptoBot ---
    cryptobot_token: str = Field(default="")
    cryptobot_webhook_secret: str = Field(default="")
    # Crypto Pay API: боевой https://pay.crypt.bot/api, тестовая сеть https://testnet-pay.crypt.bot/api
    cryptobot_api_url: str = Field(default="https://pay.crypt.bot/api")

    # --- Внешние API ---
    getgems_ws_url: str = Field(default="wss://api.getgems.io/graphql")
    getgems_api_url: str = Field(default="https://api.getgems.io/graphql")
    tonviewer_api_key: str = Field(default="")
    # Tonviewer (listener-ton) выключен по умолчанию: API недоступен (404). Включить: TONVIEWER_ENABLED=true
    tonviewer_enabled: bool = Field(default=False)
    # TonAPI (tonconsole.com). Ключ есть -> TonAPI primary; нет -> seed-каталог + GetGems fallback
    tonapi_key: str = Field(default="")
    tonapi_url: str = Field(default="https://tonapi.io")
    tonapi_poll_interval: float = Field(default=15.0)
    # Общий лимит запросов к TonAPI на процесс (free-тариф tonconsole — 1 RPS; на платном поднимите)
    tonapi_rps: float = Field(default=0.8)  # чуть ниже лимита: ровно 1.0 даёт частые 429 на границе окна
    # Постраничный скан: сколько страниц (по 1000 items) на коллекцию за цикл. Коллекция больше
    # pages*1000 сканируется за несколько циклов; floor/атрибуты пересчитываются по завершении прохода.
    tonapi_pages_per_cycle: int = Field(default=3, ge=1)
    # Автообнаружение новых коллекций Telegram Gifts (listener-market, только с TONAPI_KEY):
    # раз в interval читаем pages страниц NFT кошелька Fragment Gift Minter, новые коллекции
    # проверяем (деплоер = минтер, метаданные nft.fragment.com) и добавляем в БД.
    # Реальное время: SSE-поток всех переводов NFT в TON (GET /v2/sse/accounts/transactions,
    # op 0x5fcc3d14). Перевод нашего подарка -> GET /v2/nfts/{addr} через tonapi_stream_delay с
    # -> «ЛИСТИНГ» / «GIFT SOLD!» за секунды, без полного скана. Квоту почти не тратит.
    # Кэш лёгкого списка коллекций в API (общий для всех пользователей), секунды; 0 — выкл
    showcase_cache_ttl: float = Field(default=10.0, ge=0)
    tonapi_stream_enabled: bool = Field(default=True)
    tonapi_stream_delay: float = Field(default=2.0, ge=0)
    tonapi_stream_recheck: float = Field(default=8.0, ge=0)  # повтор, если индексатор не успел; 0 — выкл
    # Полный скан (floors/атрибуты/страховка): коллекции без фильтров и избранного пользователей
    # сканируются раз в cold_every циклов, «горячие» — каждый цикл.
    tonapi_cold_every: int = Field(default=3, ge=1)
    tonapi_discovery_enabled: bool = Field(default=True)
    tonapi_discovery_interval: float = Field(default=3600.0)
    tonapi_discovery_pages: int = Field(default=5, ge=1)
    # Seed-каталог коллекций (upsert при старте updater)
    seed_catalog_path: str = Field(default="db/seed/gift_collections.json")
    telegram_api_base: str = Field(
        default="https://api.telegram.org", description="База Bot API (для тестов можно подменить)"
    )

    # --- Rate limiting внешних API (ЭТАП 2) ---
    api_max_concurrency: int = Field(
        default=5, description="Сколько параллельных запросов к внешним HTTP/GraphQL API"
    )

    # --- Периоды фоновых задач updater (сек) ---
    floor_update_interval: float = Field(default=10.0)
    sales_update_interval: float = Field(default=30.0)
    discovery_interval: float = Field(default=3600.0)
    floor_cache_ttl: int = Field(default=30)
    sales_cache_ttl: int = Field(default=30)
    attrs_cache_ttl: int = Field(default=3600)

    # --- Collection Discovery ---
    attributes_sample_size: int = Field(
        default=100, description="Сколько NFT коллекции опрашивать для извлечения атрибутов"
    )
    ipfs_gateway: str = Field(default="https://ipfs.io/ipfs/")

    # --- TON Listener (ЭТАП 3) ---
    ton_poll_interval: float = Field(
        default=3.0, description="Интервал поллинга Tonviewer API в секундах"
    )
    # --- Market Listener (ЭТАП 4) ---
    market_poll_interval: float = Field(
        default=5.0, description="Интервал поллинга маркетплейсов в секундах"
    )

    # --- Rarity counts (ЭТАП 6.5) ---
    counts_update_interval: float = Field(
        default=600.0, description="Пересчёт counts атрибутов (сек)"
    )
    rarity_sample_size: int = Field(
        default=500, description="Сколько NFT брать для подсчёта редкости"
    )

    # --- Payments (ЭТАП 8) ---
    subscription_price_ton: float = Field(default=10.0, description="Цена подписки в TON")
    subscription_days: int = Field(default=30, description="Длительность подписки в днях")
    admin_tg_ids: str = Field(default="", description="TG ID админов через запятую")
    # Тарифы премиума (USD): 30/60/90 дней. Оплата — инвойс CryptoBot в фиате USD
    sub_price_usd_30: float = Field(default=49.0)
    sub_price_usd_60: float = Field(default=89.0)
    sub_price_usd_90: float = Field(default=119.0)

    @property
    def subscription_plans(self) -> list[dict]:
        return [
            {"id": "month1", "title": "1 месяц", "days": 30, "price_usd": self.sub_price_usd_30},
            {"id": "month2", "title": "2 месяца", "days": 60, "price_usd": self.sub_price_usd_60},
            {"id": "month3", "title": "3 месяца", "days": 90, "price_usd": self.sub_price_usd_90},
        ]

    @property
    def tonapi_enabled(self) -> bool:
        return bool(self.tonapi_key.strip())

    def is_admin(self, tg_id: int) -> bool:
        """Проверка, является ли пользователь админом."""
        return str(tg_id) in [x.strip() for x in self.admin_tg_ids.split(",") if x.strip()]


    def twa_secret_key(self) -> bytes:
        """
        Ключ для валидации initData Telegram Web App.
        Стандарт: HMAC-SHA256(key='WebAppData', msg=bot_token), digest — ключ.
        https://core.telegram.org/bots/webapps#validating-data-received-via-the-web-app
        """
        import hashlib
        import hmac

        if self.twa_auth_secret:
            return self.twa_auth_secret.encode()
        return hmac.new(b"WebAppData", self.bot_token.encode(), hashlib.sha256).digest()


# ---------------------------------------------------------------------------
# Маппинг trait_type из метаданных NFT -> тип атрибута в БД (ЭТАП 2, ТЗ п.1.C)
# Ключи — канонические типы collections.attributes.type; значения — алиасы
# (в нижнем регистре), встречающиеся в метаданных Telegram-подарков.
# ---------------------------------------------------------------------------
TRAIT_ALIASES: dict[str, list[str]] = {
    "model": ["model", "character"],
    "background": ["background", "bg"],
    "symbol": ["symbol", "icon"],
    "pattern": ["pattern", "decoration"],
}


def map_trait_type(trait_type: str) -> str | None:
    """
    Нормализация trait_type -> канонический тип атрибута.

        map_trait_type("Character")  -> "model"
        map_trait_type("Bg")          -> "background"
        map_trait_type("Rarity")      -> None  (не маппится — игнорируем)
    """
    normalized = trait_type.strip().lower()
    for attr_type, aliases in TRAIT_ALIASES.items():
        if normalized in aliases:
            return attr_type
    return None


@lru_cache
def get_settings() -> Settings:
    """Кэш настроек на процесс (один раз распарсили .env — и хватит)."""
    return Settings()
