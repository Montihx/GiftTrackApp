# CLAUDE.md — GiftTrackApp (передача контекста между сессиями)

GiftTrackApp — клон @GiftTrackBot: трекер NFT-подарков Telegram (сеть TON). Telegram-бот + мини-приложение (TWA).
Бот присылает алерты: ЛИСТИНГ, АПГРЕЙД, GIFT SOLD!; в мини-приложении — витрина коллекций, фильтры, избранное, подписка.

## Правила (от владельца проекта)
- Фронтенд — JSX на React+Vite. Без react-router и тяжёлых библиотек.
- Никогда не коммитить `.env`, ключи, токены, `docker-compose.override.yml`. Секреты в чат не вставляются — ключи вводятся скрытым вводом (скрипт обновления) или через форму.
- `bot/handlers.py` без необходимости не трогать.
- Только бесплатные источники данных (TonAPI free tier, Telegram MTProto, бесплатные API маркетов). Платный GetGems API не используется.
- Обход защиты от ботов (Cloudflare, имитация браузера `curl_cffi impersonate`, подмена fingerprint) не делаем. Если источник отвечает отказом/проверкой — отключаем источник, а не обходим. Tonnel поэтому не подключён.
- Контакт владельца («Написать владельцу») важен. Для листингов и апгрейдов есть две отдельные галочки фильтра: `require_owner` (листинги) и `require_owner_upgrade` (апгрейды).
- Один PR на изменение из ветки сессии; мерж после зелёного CI; PR-описание — на русском, с блоком «Проверки».
- Коммиты и PR заканчиваются атрибуцией (см. раздел в конце).

## CI (обязательно зелёное)
```
python -m compileall -q core db services bot tests
python -m pytest tests/ -q
cd frontend && npm install && npm run build
```
Локально: `pip install -e ".[dev]"` (Python 3.11+), `npm install` во `frontend/`.

## Устройство
Стек: Python 3.11, FastAPI, aiogram 3, SQLAlchemy 2 async (Postgres в проде, SQLite в тестах), Alembic (миграции 0001–0006 в `db/migrations/versions`), Redis, Telethon (MTProto), aiohttp; фронтенд React+Vite; nginx; docker compose.

Сервисы docker compose: `postgres`, `redis`, `migrate`, `bot`, `listener-ton` (отключён: Tonviewer API мёртв, `TONVIEWER_ENABLED=false`), `listener-market`, `resolver`, `updater`, `twa-api`, `twa-frontend`, `nginx`.

Основные модули:
- `services/tonapi.py`, `tonapi_sync.py`, `tonapi_stream.py` — TonAPI (free 1 RPS, `TONAPI_RPS=0.8`): постраничный скан коллекций, SSE-поток переводов NFT, определение листингов/продаж, пересчёт флоров. Сигнатура «продажа» — смена владельца у листингового предмета; «неуверенные» снимки игнорируются.
- `services/gift_discovery.py` — автообнаружение новых коллекций (деплоит минтер Fragment `0:158136239adb15dd59df90c641f9efd312cfeb8664f218f4c3e5fce9d95e6c07`).
- `services/telegram_market.py` — маркет подарков Telegram через Telethon (`payments.getStarGifts`, `getResaleStarGifts`, `getStarGiftUpgradeAttributes`): новые листинги в поток `nft_listings`, число выпущенных («Выпущено»), атрибуты новых коллекций. Дедуп листингов по слагу + порог смены цены 5%; предохранитель `TG_MARKET_MAX_NEW`.
- `services/market_floors.py` — флоры MRKT и Portals: вход через мини-приложение маркета вашим Telegram-аккаунтом (`requestAppWebView`), дальше обычный HTTP; 403/429/503 → пауза, без обхода.
- `services/owner_lookup.py` — владелец и «оценка» подарка из Telegram по slug (воркер в `resolver`).
- `services/resolver/` — резолвер TON-адреса → Telegram. Шаги Tonviewer мёртвы и пропускаются; 4xx не ретраятся (`core/retry.py: PermanentError`), недоступный шаг на паузе 10 минут.
- `services/dispatcher.py` — читает потоки Redis `nft_listings`, `nft_sales`, `nft_upgrades` обычным XREAD (без consumer-групп), подбирает фильтры, шлёт алерты. ВАЖНО: перед поиском владельца идёт дешёвая проверка `_has_candidates` — иначе очередь копится (воркер владельцев отвечает ~1 запрос/с, событий сотни в минуту).
- `api/` — FastAPI для мини-приложения: `twa.py` (фильтры, избранное), `showcase.py` (витрина: флоры, `issued_count`, `best_floor`, флоры маркетов, недостоверный ончейн-флор), `payments.py` (CryptoBot).
- `frontend/src/` — `App.jsx` (фильтры), `Home.jsx` (витрина), `CollectionPage.jsx`, `Subscription.jsx`, `Favorites.jsx`, `api.js` (`cachedGet`, `COIN`, `fmtTon`).

Фильтр: коллекция (пустая = «Любой подарок»), модель/фон/символ/узор, диапазоны «Флор модели», «Номер подарка», «Цена» (цена листинга/продажи), две галочки контакта. Если диапазон задан, а значение события неизвестно — не шлём.

Флор на карточке: минимум из ончейна и флоров MRKT/Portals; ончейн-флор меньше 25% от минимума маркетов считается недостоверным (нерабочий листинг). Флор маркета Telegram показывается только при `TG_FLOOR_SHOW=true`. Монета везде «GRAM» (`COIN_LABEL`, `VITE_COIN`).

## Работа с сервером
Обновление одной командой (бэкап `.env` и БД, `git pull`, новые настройки, сборка, миграции, проверка Telegram-входа):
```
cd ~/gifts/GiftTrackApp
git fetch origin main && git show origin/main:scripts/update_server.sh > /tmp/gt_update.sh && bash /tmp/gt_update.sh
```
После обновления в Telegram полностью закрыть и открыть мини-приложение (кэш WebView).
Полезные логи:
```
docker compose logs --since 10m bot | grep -E "Алерт|dispatcher" | tail -n 20
docker compose logs --since 20m resolver | grep -E "tg_market|market_floors|owner_lookup" | tail -n 30
docker compose logs --since 10m listener-market | tail -n 30
```
Сессия Telethon (sqlite) открывается только одним процессом: на время входа/проверки `resolver` останавливается. Вход: QR (`--login-qr`) или код (`--login`).

## Что проверено на реальных данных
- Флоры MRKT и Portals работают (поля: MRKT `salePrice`, Portals `price`; единицы в монете верные).
- «Выпущено» из Telegram (Plush Pepe 2 825, Scared Cat 19 289), атрибуты из `getStarGiftUpgradeAttributes` — по логам сервера не подтверждены полностью.
- GIFT SOLD! приходит. Листинговые алерты после PR #23 (диспетчер) — ждём подтверждения с сервера.

## Не сделано / открыто
- Tonnel (требует имитации браузера — не делаем).
- Фильтры из see.tg: только в продаже, валюта, scrafted, burned, monochrome, «с комментарием», «давно держат».
- Иконки части коллекций иногда показываются буквой (не загрузились).
- Причина листинга за 15 GRAM у Scared Cat (ончейн) не найдена; скрыт эвристикой.
- Подтвердить на сервере: единицы «Оценки», листинговые алерты, предохранитель tg_market (WARNING только для «горячих»).

## Окружение сессии
Облачная сессия (Claude Code on the web) не имеет SSH-доступа к серверу: исходящий HTTPS только через прокси, прямые SSH-подключения блокируются. Локальная сессия на ПК с настроенным SSH может выполнять команды на сервере сама. Ключи и пароли в чат не присылать.

## Атрибуция в коммитах и PR
Коммиты заканчиваются строками:
```
Co-Authored-By: Claude <noreply@anthropic.com>
```
(и ссылкой на сессию, если она есть). Описание PR заканчивается строкой «🤖 Generated with Claude Code».
