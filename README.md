# Gift Track Bot — клон @GiftTrackBot

Бот мониторит **апгрейды (mint)**, **листинги** и **продажи** Telegram-подарков (NFT на TON)
и шлёт уведомления («ЛИСТИНГ», «GIFT SOLD!») с резолвингом владельца в Telegram-аккаунт.
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
| `listener-ton` | `python -m bot.listener_main` | события mint из Tonviewer (выключен по умолчанию, `TONVIEWER_ENABLED`) |
| `listener-market` | `python -m bot.market_main` | TonAPI-синхронизация (с ключом) или листинги GetGems (без ключа) |
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
tests/      pytest (116 тестов)
```

## Быстрый старт

```bash
git clone https://github.com/Montihx/GiftTrackApp.git && cd GiftTrackApp
cp .env.example .env
nano .env                       # заполните BOT_TOKEN, CRYPTOBOT_TOKEN, ADMIN_TG_IDS, DB_PASS, TWA_URL
# на слабом сервере (< 2 ГБ RAM) собирайте по одному, иначе сборка может повесить машину:
COMPOSE_PARALLEL_LIMIT=1 docker compose build
docker compose up -d
docker compose ps
docker compose logs -f bot
```

`migrate` отрабатывает автоматически, остальные сервисы ждут его завершения.
Порты postgres/redis/api/frontend привязаны к `127.0.0.1`, наружу открыты
только 80/443 через nginx.

Все python-сервисы используют один образ `gifttrack-app` (общий кэш слоёв), фронтенд
собирается отдельно. Если сервер падает при сборке — добавьте swap:
`sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile`.

### Важно про `.env`

- `DB_PASS` в `.env` должен совпадать с паролем в `DB_URL`
  (`postgresql+asyncpg://gt:<DB_PASS>@postgres/gifttrack`).
- **Никогда не публикуйте токены** (скриншоты, чаты, коммиты). Если токен утёк,
  перевыпустите его: `/revoke` в @BotFather, ключ в настройках CryptoBot.
  `.env` добавлен в `.gitignore`.

### HTTPS и автопродление сертификата

Боевой хост: `gifttrack.155.248.234.68.sslip.io`. Сертификат Let's Encrypt лежит
в `/etc/letsencrypt/live/gifttrack.155.248.234.68.sslip.io/` и монтируется в nginx
только на чтение; `nginx.conf` уже содержит редирект 80 → 443 и TLS-блок. Порт 3000
хоста не используется (фронтенд слушает `127.0.0.1:3001`). Для другого домена
замените имя в `nginx.conf` и выпустите сертификат через certbot.

Автопродление (certbot на хосте, standalone: на время продления nginx останавливается).
Разрешите `ubuntu` запускать certbot без пароля: `sudo visudo -f /etc/sudoers.d/certbot`,
строка `ubuntu ALL=(root) NOPASSWD: /usr/bin/certbot`. Затем `crontab -e`:

```cron
0 4 * * * cd ~/gifts/GiftTrackApp && docker compose stop nginx && sudo -n certbot renew --quiet; docker compose start nginx
```

`start` стоит после `;`, чтобы nginx поднялся, даже если продление не удалось.
Telegram открывает TWA только по HTTPS; адрес задаётся в `TWA_URL` и в Menu Button у @BotFather.

### Telethon: cold path резолвера (необязательно)

Воркер `resolver` сканирует bio известных пользователей (username из `users`,
истории владельцев и `wallet_tg_map`) на TON-адреса (`EQ…`/`UQ…`/raw/`ton://`),
строит индекс «адрес → пользователь» в Redis и разбирает очередь
`telethon_resolve_queue`: найденные владельцы пишутся в `wallet_tg_map`
(`source=telethon`) и кэш. Telegram не умеет искать по тексту bio, поэтому
охватываются только известные боту пользователи.

Настройка: заполните `API_ID`, `API_HASH`, `PHONE` в `.env` (лучше отдельный
аккаунт, не основной), затем один раз войдите (код из Telegram):

```bash
docker compose run --rm -it resolver python -m bot.resolver_main --login
docker compose restart resolver
```

Сессия хранится в томе `tgsessions`. Без авторизации воркер просто простаивает
и пишет подсказку в лог. Пауза между запросами, размер пачки и период повторного
скана настраиваются: `BIO_SCAN_DELAY` (2 с), `BIO_SCAN_BATCH` (100),
`BIO_RESCAN_DAYS` (7), `BIO_CYCLE_SECONDS` (600). `FloodWait` соблюдается автоматически.

## Источники данных

**Каталог коллекций** — `db/seed/gift_collections.json`: 21 коллекция Telegram Gifts с адресами
контрактов и иконками (`nft.fragment.com/collection/<slug>.webp`). Каждый адрес проверен on-chain
через TonAPI: коллекция в whitelist TonAPI, `approved_by: getgems`, метаданные отдаются с
`nft.fragment.com/collection/<slug>.json`. В TonAPI много поддельных коллекций с тем же именем и
описанием — по имени адрес не выбирать. Seed применяется при старте `updater` (идемпотентно;
плейсхолдер `tg_gift:<slug>` заменяется адресом, пустая иконка дозаполняется).

**TonAPI (основной источник, нужен `TONAPI_KEY`)** — `services/tonapi_sync.py` в `listener-market`:
постранично читает `GET /v2/nfts/collections/{addr}/items` и сравнивает с прошлым состоянием
предмета (Redis-хэш на коллекцию):

- новый листинг или смена цены → `nft_listings` → уведомление «ЛИСТИНГ»;
- выставленный предмет сменил владельца → строка в `sales` + `nft_sales` → «GIFT SOLD!»
  (цена в TON и ≈USD по курсу `GET /v2/rates`);
- по завершении прохода — floor коллекции (+снапшот для графика и 24h change), floor и количество
  по моделям/фонам/символам, `items_count`.

Особенности живого API (проверено в 2026-11): цена листинга приходит как
`{"currency_type": "native", "token_name": "Gram", ...}`; поле `index` у подарков — хэш, номер
берётся из `metadata.name` («Plush Pepe #2675»); ещё не выведенные в блокчейн подарки принадлежат
«Fragment Gift Minter» (владелец неизвестен); маркет — `sale.market.name` («Getgems Sales»,
«Marketapp Marketplace»). Первый проход каждой коллекции — без уведомлений.

**Лимиты.** Free-тариф tonconsole — 1 RPS; все запросы процесса идут через общий limiter
(`TONAPI_RPS`). За цикл на коллекцию читается до `TONAPI_PAGES_PER_CYCLE` страниц по 1000 items;
крупные коллекции (Toy Bear ~30k, Homemade Cake ~34k) проходятся за несколько циклов, и их floor
обновляется раз в проход. 21 коллекция ≈ 217k items ≈ 217 запросов на полный проход: на 1 RPS цикл
занимает ~1 мин, мелкие коллекции обновляются каждую минуту, самые крупные — раз в ~12 мин.
На платном тарифе поднимите `TONAPI_RPS` — цикл упрётся в `TONAPI_POLL_INTERVAL` (15 с).
Ключ общий для `listener-market` и `twa-api` (цена предмета на витрине) — учитывайте это в `TONAPI_RPS`.

**Без ключа** витрина показывает seed-каталог, floor «—», график — empty-state; `listener-market`
работает через GetGems GraphQL, `updater` считает floors через GetGems.

**Другие маркетплейсы.** Листинги on-chain маркетов (GetGems, Marketapp и др.) TonAPI отдаёт сам,
имя маркета берётся из `sale.market.name`. Portals, Tonnel и MRKT торгуют подарками внутри Telegram
(off-chain) и публичного API не имеют — их внутренние API требуют авторизации Telegram-аккаунтом,
поэтому они не подключены.

## Переменные окружения (дополнительно к `.env.example`)

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `TONAPI_KEY` | пусто | ключ tonconsole.com; пусто → seed-каталог + GetGems |
| `TONAPI_POLL_INTERVAL` | 15 | пауза между циклами TonAPI-синхронизации, с |
| `TONAPI_RPS` | 1 | лимит запросов к TonAPI на процесс |
| `TONAPI_PAGES_PER_CYCLE` | 3 | страниц по 1000 items на коллекцию за цикл |
| `TONVIEWER_ENABLED` | false | включить `listener-ton` (Tonviewer API сейчас недоступен) |
| `SUB_PRICE_USD_30/60/90` | 49 / 89 / 119 | цены тарифов, $ (инвойс CryptoBot в USD, оплата TON/USDT) |
| `VITE_OWNER_USERNAME` | пусто | username владельца для «Не получается оплатить? → Написать владельцу» (build-arg фронтенда) |

**Оплата.** Вебхук CryptoBot: `/api/payments/webhook/<CRYPTOBOT_WEBHOOK_SECRET>`. Если задан
`CRYPTOBOT_TOKEN`, проверяется подпись `Crypto-Pay-Api-Signature`; повторная доставка одного
инвойса не продлевает подписку дважды (таблица `payment_invoices`). Продление активной подписки
добавляет дни к текущему сроку, истёкшей — отсчитывает от момента оплаты.

## Локальная разработка

```bash
pip install -e ".[dev]"
pytest                          # 116 тестов
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

Реализованы все модули ТЗ: discovery и updater, TON-листенер, листенер
маркетплейсов, каскадный резолвер владельцев (включая Telethon-скан bio),
диспетчер и хендлеры бота, TWA API + React-фронтенд, оплата через CryptoBot, nginx.
Отличия от ТЗ: фронтенд на JavaScript (JSX), а не TypeScript; точки входа сервисов
лежат в `bot/*_main.py`.
