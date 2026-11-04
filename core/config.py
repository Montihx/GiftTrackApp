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


@lru_cache
def get_settings() -> Settings:
    """Кэш настроек на процесс (один раз распарсили .env — и хватит)."""
    return Settings()
