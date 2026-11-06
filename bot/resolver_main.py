# /workspace/bot/resolver_main.py — точка входа для сервиса resolver
"""
Точка входа для сервиса resolver:
    python -m bot.resolver_main

Запускает фоновые джобы на Telethon-аккаунте: скан bio пользователей (резолвер владельцев)
и наблюдение за маркетом подарков Telegram (services/telegram_market.py, TG_MARKET_ENABLED).
Запускается только если API_ID / API_HASH / PHONE заданы в .env.
"""
from __future__ import annotations

import asyncio

from loguru import logger

from core.config import get_settings
from core.logging import setup_logging
from core.redis_client import create_redis
from core.shutdown import ShutdownManager
from db.engine import dispose_engine, session_scope
from services.resolver.telethon_worker import BioScanner


async def _telethon_worker(redis, stop_event: asyncio.Event) -> None:
    """
    Cold path: скан bio известных пользователей через Telethon + разбор очереди
    telethon_resolve_queue (см. services/resolver/telethon_worker.py).

    Требует авторизованной сессии. Первый вход (код из Telegram, один раз):
        docker compose run --rm -it resolver python -m bot.resolver_main --login
    """
    settings = get_settings()
    if not (settings.api_id and settings.api_hash and settings.phone):
        logger.warning("Telethon не настроен (API_ID/API_HASH/PHONE пусты) — worker простаивает")
        await stop_event.wait()  # не завершаемся, чтобы контейнер не уходил в crash-loop
        return

    client = _make_client(settings)
    await client.connect()
    if not await client.is_user_authorized():
        logger.error(
            "Telethon не авторизован. Выполните один раз: "
            "docker compose run --rm -it resolver python -m bot.resolver_main --login"
        )
        await stop_event.wait()
        await client.disconnect()
        return

    logger.info("Telethon worker запущен")
    scanner = BioScanner(client, redis, session_scope, settings)
    jobs = [_supervise("bio_scanner", lambda: scanner.run(stop_event), stop_event)]
    if settings.owner_lookup_enabled:
        from services.owner_lookup import OwnerLookupWorker

        lookup = OwnerLookupWorker(client, redis, settings.owner_lookup_interval)
        jobs.append(_supervise("owner_lookup", lambda: lookup.run(stop_event), stop_event))
    if settings.tg_market_enabled:
        from services.telegram_market import TelegramMarketWatcher

        watcher = TelegramMarketWatcher(client, redis, session_scope, settings)
        jobs.append(_supervise("tg_market", lambda: watcher.run(stop_event), stop_event))
        if settings.tg_upgrades_enabled:
            from services.telegram_upgrades import TelegramUpgradeWatcher

            upgrades = TelegramUpgradeWatcher(client, redis, watcher, settings)
            jobs.append(_supervise("tg_upgrades", lambda: upgrades.run(stop_event), stop_event))
    if settings.market_floors_enabled:
        from services.market_floors import MarketFloorWatcher

        floors = MarketFloorWatcher(client, redis, session_scope, settings)
        jobs.append(_supervise("market_floors", lambda: floors.run(stop_event), stop_event))
        if settings.market_listings_enabled:
            from services.market_listings import MarketListingWatcher

            listings = MarketListingWatcher(floors, redis, session_scope, settings)
            jobs.append(_supervise("market_listings", lambda: listings.run(stop_event), stop_event))
    try:
        await asyncio.gather(*jobs)
    finally:
        await client.disconnect()


async def _supervise(name: str, factory, stop_event: asyncio.Event, backoff: float = 2.0) -> None:  # noqa: ANN001
    """Перезапуск упавшей задачи с паузой: сбой одной (сеть, Redis) не роняет весь resolver."""
    while not stop_event.is_set():
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(f"[{name}] упал: {exc!r} — перезапуск через {backoff:.0f}s", exc_info=exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)


def _make_client(settings):
    from pathlib import Path

    from telethon import TelegramClient

    Path(settings.telethon_session).parent.mkdir(parents=True, exist_ok=True)
    return TelegramClient(settings.telethon_session, settings.api_id, settings.api_hash)


async def _login() -> None:
    """Интерактивный вход userbot (код из Telegram / пароль 2FA)."""
    settings = get_settings()
    client = _make_client(settings)
    await client.start(phone=settings.phone)
    me = await client.get_me()
    print(f"Авторизован как @{me.username or me.id}. Сессия сохранена.")
    await client.disconnect()


async def _login_qr() -> None:
    """Вход по QR-коду: код из Telegram никуда вводить не нужно.

    Сервер печатает QR (и ссылку tg://login?token=…); отсканируйте его в Telegram на телефоне:
    Настройки → Устройства → Подключить устройство. Если включён облачный пароль — спросит его.
    """
    from getpass import getpass

    from telethon import errors

    settings = get_settings()
    client = _make_client(settings)
    await client.connect()
    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"Уже авторизован как @{me.username or me.id}.")
        await client.disconnect()
        return
    import datetime as dt

    qr = await client.qr_login()
    deadline = asyncio.get_running_loop().time() + 600  # 10 минут на вход, дальше — запустить заново
    while True:
        print("\033[2J\033[H", end="")  # очистить экран: виден только актуальный QR
        _print_qr(qr.url)
        # Токен QR живёт ~30 с: ждём до его истечения и показываем новый, иначе Telegram ответит «неверный QR»
        left = (qr.expires - dt.datetime.now(dt.timezone.utc)).total_seconds() - 2
        print(f"Отсканируйте в Telegram: Настройки → Устройства → Подключить устройство. Код обновится через {max(1, int(left))} с.")
        try:
            await qr.wait(timeout=max(3.0, left))
            break
        except asyncio.TimeoutError:
            if asyncio.get_running_loop().time() > deadline:
                print("Время вышло. Запустите вход ещё раз.")
                await client.disconnect()
                return
            await qr.recreate()
        except errors.SessionPasswordNeededError:
            await client.sign_in(password=getpass("Облачный пароль Telegram: "))
            break
    me = await client.get_me()
    print(f"Авторизован как @{me.username or me.id}. Сессия сохранена.")
    await client.disconnect()


def _print_qr(url: str) -> None:
    try:
        import qrcode

        q = qrcode.QRCode(border=1)
        q.add_data(url)
        q.print_ascii(invert=True)
    except ImportError:
        print("(для QR в терминале: pip install qrcode)")
    print(url)


async def _main() -> None:
    setup_logging(service_name="resolver")
    redis = create_redis()
    sm = ShutdownManager(timeout=15)

    async def run() -> None:
        async with session_scope():
            await _telethon_worker(redis, sm._stop_event)  # noqa: SLF001

    try:
        await sm.run(run)
    finally:
        await redis.aclose()
        await dispose_engine()


if __name__ == "__main__":
    import sys

    if "--login-qr" in sys.argv:
        asyncio.run(_login_qr())
    elif "--login" in sys.argv:
        asyncio.run(_login())
    else:
        asyncio.run(_main())
