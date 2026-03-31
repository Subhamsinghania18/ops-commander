from datetime import datetime, timezone, timedelta
import random


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def maybe_shift_timestamp(ts: datetime, out_of_order_pct: float, max_seconds: int = 6) -> datetime:
    if random.random() >= out_of_order_pct:
        return ts
    return ts - timedelta(seconds=random.randint(1, max_seconds))
