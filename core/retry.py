# /workspace/core/retry.py — ретраи с exponential backoff (tenacity)
from __future__ import annotations

from typing import Any, Callable

from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


def make_retrying(
    attempts: int = 5,
    min_wait: float = 0.5,
    max_wait: float = 30.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> AsyncRetrying:
    """
    Фабрика асинхронных ретраев: exponential backoff 0.5s -> 1s -> 2s ... до max_wait.
    Использование:
        async for attempt in make_retrying():
            with attempt:
                await risky_call()
    """
    return AsyncRetrying(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=min_wait, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(retry_on),
        reraise=True,
    )


async def with_retry(fn: Callable[..., Any], *args: Any, context: str = "", **kwargs: Any) -> Any:
    """Обёртка «вызов с ретраями + логирование каждой неудачи»."""
    last_exc: Exception | None = None
    async for attempt in make_retrying():
        with attempt:
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    f"[{context}] попытка #{attempt.retry_state.attempt_number} "
                    f"не удалась: {exc!r}"
                )
                raise
    # Сюда приходим только если все попытки исчерпаны (reraise=True должен был бросить)
    assert last_exc is not None
    raise last_exc
