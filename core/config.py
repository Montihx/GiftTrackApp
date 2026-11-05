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

    # --- Внешние API ---
    getgems_ws_url: str = Field(default="wss://api.getgems.io/graphql")
    getgems_api_url: str = Field(default="https://api.getgems.io/graphql")
    tonviewer_api_key: str = Field(default="")
    # TonAPI (tonconsole.com). Ключ есть -> TonAPI primary; нет -> seed-каталог + GetGems fallback
    tonapi_key: str = Field(default="")
    tonapi_url: str = Field(default="https://tonapi.io")
    # У TonAPI нет эндпоинта floor: считаем min(sale.price) по items коллекции.
    # Если items больше лимита — floor НЕ пишем (частичный скан даёт неверную цену).
    tonapi_floor_scan_limit: int = Field(default=3000)
    tonapi_poll_interval: float = Field(default=15.0)
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
