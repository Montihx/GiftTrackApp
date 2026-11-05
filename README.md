# Gift Track Bot — клон @GiftTrackBot

Бот мониторит **апгрейды (mint)** и **листинги** Telegram-подарков (NFT на TON)
и шлёт уведомления с резолвингом владельца в Telegram-аккаунт.
Фильтры настраиваются в Telegram Web App (TWA).

## Стек

Python 3.11 · FastAPI · Aiogram 3 · Pytoniq · Telethon · PostgreSQL 16 ·
Redis 7 · React 18 + Vite (TWA) · Docker Compose · Nginx · CryptoBot API

## Сервисы (docker-compose)

| Сервис | Команда | Назначение |
|---|---|---|
| `postgres`, `redis` | — | хранилище и очереди |
| `migrate` | `alembic upgrade head` | миграции БД (одноразово перед стартом) |
| `bot` | `python -m bot.bot_main` | Aiogram (polling) + рассылка уведомлений |
| `listener-ton` | `python -m bot.listener_main` | события mint/апгрейдов из TON |
| `listener-market` | `python -m bot.market_main` | листинги маркетплейсов (GetGems) |
| `resolver` | `python -m bot.resolver_main` | фоновый Telethon-воркер (опционально) |
| `updater` | `python -m services.updater` | флор-цены и обновление коллекций |
| `twa-api` | `uvicorn api.main:app` | REST + SSE для TWA, вебхук оплаты |
| `twa-frontend` | nginx со статикой Vite | интерфейс TWA |
| `nginx` | — | reverse proxy на портах 80/443 |

## Структура

```
core/       config, logging, redis, retry, graceful shutdown
db/         модели SQLAlchemy 2.0, Alembic-миграции
bot/        точки входа сервисов, handlers, admin-команды
services/   dispatcher, ton/, market/, resolver/, updater, payments, collection_discovery
api/        FastAPI: /api/collections, /api/filters, SSE, /api/payments/webhook/<secret>
frontend/   React TWA (Vite), Dockerfile + nginx.conf для раздачи статики
tests/      pytest (57 тестов)
```

## Быстрый старт

```bash
git clone https://github.com/Montihx/GiftTrackApp.git && cd GiftTrackApp
cp .env.example .env
nano .env                       # заполните BOT_TOKEN, CRYPTOBOT_TOKEN, ADMIN_TG_IDS, DB_PASS, TWA_URL
docker compose up -d --build
docker compose ps
docker compose logs -f bot
```

`migrate` отрабатывает автоматически, остальные сервисы ждут его завершения.
Порты postgres/redis/api/frontend привязаны к `127.0.0.1`, наружу открыты
только 80/443 через nginx.

### Важно про `.env`

- `DB_PASS` в `.env` должен совпадать с паролем в `DB_URL`
  (`postgresql+asyncpg://gt:<DB_PASS>@postgres/gifttrack`).
- **Никогда не публикуйте токены** (скриншоты, чаты, коммиты). Если токен утёк,
  перевыпустите его: `/revoke` в @BotFather, ключ в настройках CryptoBot.
  `.env` добавлен в `.gitignore`.

### HTTPS для Telegram Web App

Telegram открывает TWA только по HTTPS. Укажите домен в `TWA_URL`, получите
сертификат (certbot) и добавьте в `nginx.conf` `listen 443 ssl;` с путями к
сертификату (папку сертификатов смонтируйте в сервис `nginx`). Затем задайте
Menu Button/Web App URL у бота через @BotFather.

### Telethon (необязательно)

Сервис `resolver` без `API_ID`/`API_HASH`/`PHONE` просто простаивает.
Первый вход Telethon требует кода из Telegram:
`docker compose run --rm -it resolver` (один раз, сессия сохранится).

## Локальная разработка

```bash
pip install -e ".[dev]"
pytest                          # 57 тестов
cd frontend && npm install && npm run dev   # /api проксируется на localhost:8000
```

## Диагностика

| Симптом | Причина / решение |
|---|---|
| `failed to read dockerfile` для `twa-frontend` | нет `frontend/Dockerfile`, выполните `git pull` |
| `No module named bot.main` | устаревший `docker-compose.yml`, `git pull` |
| `docker compose ps` пуст | сборка упала, смотрите `docker compose up -d --build` до первой ошибки |
| SSE-поток флор-цены обрывается | в nginx должен быть блок `location ~ ^/api/collections/[^/]+/stream$` с `proxy_buffering off` |
| бот не отвечает | `docker compose logs --tail=50 bot`, проверьте `BOT_TOKEN` |

## Статус

Реализованы все модули: discovery и updater, TON-листенер, листенер маркетплейсов,
резолвер владельцев, диспетчер и хендлеры бота, TWA API + React-фронтенд,
оплата через CryptoBot, nginx. Не реализовано: скан bio в Telethon-воркере
(`TODO` в `bot/resolver_main.py`), поэтому резолвинг идёт по основному каскаду.
