# /workspace/core/shutdown.py — graceful shutdown для всех long-running сервисов
from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable

from loguru import logger


class ShutdownManager:
    """
    Подписывается на SIGTERM/SIGINT (docker stop шлёт SIGTERM) и ждёт
    завершения основных задач. Даёт задачам `timeout` секунд на доработку,
    после чего отменяет их принудительно.
    """

    def __init__(self, timeout: float = 10.0) -> None:
        self.timeout = timeout
        self._stop_event = asyncio.Event()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        loop = loop or asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.request_stop, sig.name)

    def request_stop(self, reason: str = "unknown") -> None:
        if self._stop_event.is_set():
            return
        logger.info(f"Запрошена остановка ({reason}) — graceful shutdown...")
        self._stop_event.set()

    async def wait_stopping(self) -> None:
        """Блокирует до сигнала остановки (для цикла `while not sm.stopping`)."""
        await self._stop_event.wait()

    async def run(self, main_coro_factory: Callable[[], Awaitable[None]]) -> None:
        """
        Запуск основной задачи под управлением shutdown:
        как только пришёл сигнал — задача отменяется с таймаутом на доработку.
        """
        self.install_signal_handlers()
        task = asyncio.create_task(main_coro_factory(), name="main-task")
        stop_waiter = asyncio.create_task(self.wait_stopping(), name="stop-waiter")

        done, _pending = await asyncio.wait(
            {task, stop_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_waiter in done and task not in done:
            logger.info("Останавливаю основную задачу...")
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=self.timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Задача не завершилась за {self.timeout}s — принудительная отмена")
            except (asyncio.CancelledError, Exception) as exc:  # noqa: BLE001
                logger.info(f"Основная задача завершена: {exc!r}")
        else:
            # Основная задача завершила работу сама (штатно или с ошибкой)
            stop_waiter.cancel()
            exc = task.exception()
            if exc:
                logger.error(f"Основная задача упала с исключением: {exc!r}", exc_info=exc)
                raise exc
