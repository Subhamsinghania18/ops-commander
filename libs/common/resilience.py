from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry_with_backoff(
    fn: Callable[[], T],
    max_attempts: int = 4,
    base_delay_seconds: float = 0.05,
    max_delay_seconds: float = 1.0,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> T:
    attempt = 1
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_attempts:
                raise
            if on_retry:
                on_retry(attempt, exc)
            delay = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
            jitter = random.uniform(0, delay * 0.2)
            time.sleep(delay + jitter)
            attempt += 1
