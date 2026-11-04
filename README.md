# Gift Track Bot — клон @GiftTrackBot (Этап 1 готов)

Бот мониторит **апгрейды (mint)** и **листинги** Telegram-подарков (NFT на TON)
и шлёт уведомления < 10 секунд с резолвингом владельца в Telegram-аккаунт.

## Стек
Python 3.11 · FastAPI · Aiogram 3 · Pytoniq · Telethon · PostgreSQL 16 (asyncpg + SQLAlchemy 2.0) · Redis 7 · React 18 + TS (TWA) · Docker Compose · Nginx · CryptoBot API

## Структура (Этап 1)
```
core/                  # config (pydantic-settings), logging (loguru), redis_client, retry, shutdown
db/
  models.py            # SQLAlchemy 2.0: 9 таблиц по ТЗ
  engine.py            # async engine + session_scope()
  migrations/          # Alembic (env.py берёт DB_URL из .env)
    versions/0001_initial_schema.py
bot/  api/  services/  # код следующих этапов (модули 1–10)
frontend/              # React TWA (Этап 7)
tests/                 # pytest: модели, конфиг, ключи Redis
docker-compose.yml     # postgres, redis, migrate, bot, listener-ton, listener-market,
                       # resolver, updater, twa-api, twa-frontend, nginx
nginx.conf             # reverse proxy + SSE (proxy_buffering off)
.env.example           # все переменные окружения
```

## Быстрый старт
```bash
cp .env.example .env      # заполнить BOT_TOKEN, DB_PASS и т.д.
docker compose up -d      # migrate отработает alembic upgrade head автоматически
```

Локальная разработка без Docker:
```bash
pip install -e ".[dev]"
export DB_URL=postgresql+asyncpg://gt:pass@localhost:5432/gifttrack
alembic upgrade head
pytest                    # 9 passing tests
```

## Проверенные артефакты этапа
- `alembic upgrade head --sql` генерирует корректный PostgreSQL DDL (9 таблиц, 10 индексов);
- тесты моделей проходят на in-memory SQLite;
- все python-файлы компилируются без ошибок.

## План этапов
| Этап | Содержание | Статус |
|---|---|---|
| 1 | Структура, pyproject, .env, docker-compose, Alembic, модели | ✅ готов |
| 2 | Collection Discovery + Floor Price Updater | ⏳ ждём «далее» |
| 3 | TON Listener (WebSocket) | ⏳ |
| 4 | Marketplace Listener (GetGems WS и др.) | ⏳ |
| 5 | Owner Resolver (каскад + Telethon fallback) | ⏳ |
| 6 | Dispatcher + Aiogram handlers | ⏳ |
| 7 | TWA API (FastAPI + SSE) + React frontend | ⏳ |
| 8 | Payment (CryptoBot) + subscription middleware | ⏳ |
| 9 | Nginx + деплой + README | ⏳ |
