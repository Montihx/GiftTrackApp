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
| `updater` | `python -m services.updater` | seed-каталог и метаданные коллекций; без ключа TonAPI — floors/продажи через GetGems |
| `twa-api` | `uvicorn api.main:app` | REST + SSE для TWA, вебхук оплаты |
| `twa-frontend` | nginx со статикой Vite | интерфейс TWA |
| `nginx` | — | reverse proxy на портах 80/443 |

## Структура

```
core/       config, logging, redis, retry, graceful shutdown, ton_address
db/         модели SQLAlchemy 2.0, Alembic-миграции (0001–0004), seed/gift_collections.json
bot/        точки входа сервисов (bot/*_main.py), handlers, admin-команды
services/   tonapi + tonapi_sync + tonapi_stream + gift_discovery (TonAPI), market/ (GetGems), ton/ (Tonviewer), updater, floors,
            seed, showcase, dispatcher, resolver/, payments, collection_discovery
api/        FastAPI: витрина (/api/collections, /api/items, /api/favorites, /api/subscription),
            фильтры (/api/filters), SSE, вебхук /api/payments/webhook/<secret>
frontend/   React TWA (Vite, JSX, без роутера/UI-библиотек), Dockerfile + nginx.conf
tests/      pytest (125 тестов, SQLite in-memory)
```

## Быстрый старт

```bash
git clone https://github.com/Montihx/GiftTrackApp.git && cd GiftTrackApp
cp .env.example .env
nano .env                       # BOT_TOKEN, CRYPTOBOT_TOKEN, ADMIN_TG_IDS, DB_PASS, TWA_URL, TONAPI_KEY
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

**Новые коллекции — автоматически (нужен `TONAPI_KEY`).** `services/gift_discovery.py` в
`listener-market` раз в `TONAPI_DISCOVERY_INTERVAL` (1 ч) читает `TONAPI_DISCOVERY_PAGES` (5) страниц
NFT кошелька «Fragment Gift Minter» (`0:158136…6c07`): улучшенные, но не выведенные подарки лежат
у него, поэтому там видны почти все коллекции, включая только что вышедшие (на странице из 1000
NFT — 100+ коллекций). Новая коллекция добавляется в БД (`source='tonapi'`, имя в единственном
числе, иконка с fragment), только если её контракт развёрнут этим кошельком и метаданные отдаются
с `nft.fragment.com/collection/<slug>.json` — подделки (их в TonAPI десятки на каждую коллекцию)
так не проходят. Отклонённые адреса запоминаются в Redis. Добавленную коллекцию TonAPI-sync
начинает сканировать в том же цикле, она сразу появляется на витрине и в форме фильтров.
Выключить: `TONAPI_DISCOVERY_ENABLED=false` (останется seed-каталог).

**Реальное время — поток переводов NFT (нужен `TONAPI_KEY`, хватает free-тарифа).**
`services/tonapi_stream.py` держит SSE-соединение `GET /v2/sse/accounts/transactions?accounts=ALL&operations=0x5fcc3d14`
— каждую финализированную транзакцию «перевод NFT» по всей сети TON. Листинг, продажа, снятие с продажи и
перевод — это всё переводы NFT. Адрес проверяется по `processed_nfts` локально (чужие NFT отбрасываются без
запросов к API); для наших подарков через `TONAPI_STREAM_DELAY` (2 с) делается один `GET /v2/nfts/{addr}` →
«ЛИСТИНГ» / «GIFT SOLD!» за секунды, флор коллекции и модели снижается сразу. Квоту почти не тратит: запрос —
только на перевод нашего подарка. Запросы потока идут вне очереди: пока такой запрос ждёт, полный скан
слот лимита не получает (`RateLimiter(high=True)`). Если индексатор TonAPI ещё не обновил подарок, он
перепроверяется через `TONAPI_STREAM_RECHECK` (8 с). Штатные обрывы соединения сервером — переподключение
сразу, с `Last-Event-ID`. При обрыве — переподключение с backoff; пропущенное догонит полный скан.
Поток работает для коллекций, у которых завершён первый проход скана.

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

**Лимиты.** Free-тариф tonconsole — 1 RPS (проверено: ~3–10% запросов получают 429 и проходят
с ретраем); все запросы процесса идут через общий limiter (`TONAPI_RPS`). За цикл на коллекцию
читается до `TONAPI_PAGES_PER_CYCLE` страниц по 1000 items; крупные коллекции проходятся за
несколько циклов, и их floor обновляется раз в проход. Замеры (2026-11):

| Каталог | Items | Запросов за цикл | Цикл на 1 RPS | Полный проход самой крупной |
|---|---|---|---|---|
| seed, 21 коллекция | ~217 тыс. | ~65 | ~1 мин | ~12 мин (Homemade Cake, 34k) |
| с автообнаружением, ~122 коллекции | ~1,5 млн | ~330 | ~5–6 мин | ~2 ч (Lol Pop, 70k) |

Листинги и продажи приходят из потока за секунды на любом тарифе — скан нужен для floors, атрибутов
и как страховка. Чтобы скан успевал на free-тарифе, «горячие» коллекции (на которые у пользователей есть
фильтры или избранное) сканируются каждый цикл и первыми, остальные — раз в `TONAPI_COLD_EVERY` (3) цикла.
Рост floor крупной коллекции (продан самый дешёвый лот) виден после её прохода: на free-тарифе — до ~2 ч,
на Lite ($9.9, 10 RPS) — ~15 мин; снижение floor — сразу из потока. Тарифы: tonapi.io → Pricing.
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
| `TONAPI_STREAM_ENABLED` | true | события в реальном времени из SSE-потока переводов NFT |
| `TONAPI_STREAM_DELAY` | 2 | пауза перед запросом NFT после события (индексатор TonAPI), с |
| `TONAPI_STREAM_RECHECK` | 8 | повторная проверка подарка, если индексатор TonAPI не успел, с (0 — выкл) |
| `TONAPI_COLD_EVERY` | 3 | коллекции без фильтров/избранного сканируются раз в N циклов |
| `TONAPI_DISCOVERY_ENABLED` | true | автодобавление новых коллекций Telegram Gifts |
| `TONAPI_DISCOVERY_INTERVAL` | 3600 | период автообнаружения, с |
| `TONAPI_DISCOVERY_PAGES` | 5 | страниц NFT минтера за один запуск автообнаружения |
| `TONVIEWER_ENABLED` | false | включить `listener-ton` (Tonviewer API сейчас недоступен) |
| `CRYPTOBOT_API_URL` | `https://pay.crypt.bot/api` | для тестов — `https://testnet-pay.crypt.bot/api` (токен из @CryptoTestnetBot) |
| `SUB_PRICE_USD_30/60/90` | 49 / 89 / 119 | цены тарифов, $ (инвойс CryptoBot в USD, оплата TON/USDT) |
| `VITE_OWNER_USERNAME` | пусто | username владельца для «Не получается оплатить? → Написать владельцу» (build-arg фронтенда) |

**Оплата.** Вебхук CryptoBot: `/api/payments/webhook/<CRYPTOBOT_WEBHOOK_SECRET>`. Если задан
`CRYPTOBOT_TOKEN`, проверяется подпись `Crypto-Pay-Api-Signature`; повторная доставка одного
инвойса не продлевает подписку дважды (таблица `payment_invoices`). Продление активной подписки
добавляет дни к текущему сроку, истёкшей — отсчитывает от момента оплаты.

## Локальная разработка

```bash
pip install -e ".[dev]"
pytest                          # 125 тестов
python -m compileall -q core db services bot tests        # то же, что в CI
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
| floors «—» при заданном ключе | первый проход ещё идёт (крупные коллекции — до ~12 мин на 1 RPS); `docker compose logs listener-market` |
| `HTTP 429` от TonAPI в логах | `TONAPI_RPS` выше лимита тарифа (учтите, что ключ делят `listener-market` и `twa-api`) |
| `HTTP 401` от TonAPI | неверный `TONAPI_KEY` |
| вебхук оплаты отвечает 401 | `CRYPTOBOT_TOKEN` в `.env` не совпадает с токеном приложения, на которое настроен вебхук |

## Ограничения

- Время листинга/продажи — момент обнаружения: из потока — секунды, если событие пропущено (обрыв потока) — до прохода коллекции.
- Офчейн-листинги (подарок выставлен, но не выведен в блокчейн) поток не видит.
- Новые контракты продажи Marketapp TonAPI пока не распознаёт (нет `sale`, владелец — контракт): такие
  листинги без цены не объявляются, а их продажи видны как переводы. Getgems и старые контракты Marketapp
  распознаются полностью. Снимок «нет листинга, владелец — контракт» не порождает событий (защита от
  ложных «GIFT SOLD!»).
- Portals, Tonnel, MRKT не подключены (нет публичного API, см. «Источники данных»).
- `listener-ton` выключен по умолчанию: API Tonviewer недоступен.
- Отличия от ТЗ: фронтенд на JavaScript (JSX), а не TypeScript.
