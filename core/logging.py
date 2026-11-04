# /workspace/core/logging.py — единая настройка loguru для всех сервисов
import logging
import sys

from loguru import logger

from core.config import get_settings


def setup_logging(service_name: str = "app") -> None:
    """
    Инициализация loguru: убираем стандартные хендлеры, добавляем свой.
    structlog-совместимый формат: время | уровень | сервис.module:line | сообщение
    """
    settings = get_settings()
    level = settings.log_level.upper()

    # Перехват логов сторонних библиотек -> перенаправляем в loguru
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>" + service_name + "</cyan>.<yellow>{module}</yellow>:<magenta>{line}</magenta> | "
            "<level>{message}</level>"
        ),
        enqueue=True,  # потоко-/процессо-безопасно (важно для asyncio + gather)
        backtrace=False,
        diagnose=settings.environment == "development",
    )
    logger.info(f"Логирование инициализировано: service={service_name}, level={level}")


class InterceptHandler(logging.Handler):
    """Пробрасывает записи stdlib logging (aiogram, aiohttp, sqlalchemy) в loguru."""

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # type: ignore[assignment]
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())
