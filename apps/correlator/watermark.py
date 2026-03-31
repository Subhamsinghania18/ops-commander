from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class WatermarkTracker:
    allowed_lateness_seconds: int = 20
    max_event_time: datetime | None = None

    def observe(self, event_time: datetime) -> None:
        normalized = event_time.astimezone(timezone.utc)
        if self.max_event_time is None or normalized > self.max_event_time:
            self.max_event_time = normalized

    @property
    def watermark(self) -> datetime:
        if self.max_event_time is None:
            return datetime.now(timezone.utc) - timedelta(days=3650)
        return self.max_event_time - timedelta(seconds=self.allowed_lateness_seconds)

    def is_late(self, event_time: datetime) -> bool:
        return event_time.astimezone(timezone.utc) < self.watermark
