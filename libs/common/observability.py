from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import time


@dataclass
class ServiceMetrics:
    service_name: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processed_total: int = 0
    dropped_total: int = 0
    error_total: int = 0
    retried_total: int = 0
    last_error: str | None = None
    process_latency_ms_sum: float = 0.0
    process_latency_ms_count: int = 0

    def record_processed(self, latency_ms: float) -> None:
        self.processed_total += 1
        self.process_latency_ms_sum += max(0.0, latency_ms)
        self.process_latency_ms_count += 1

    def record_dropped(self) -> None:
        self.dropped_total += 1

    def record_error(self, error: str) -> None:
        self.error_total += 1
        self.last_error = error[:512]

    def record_retry(self) -> None:
        self.retried_total += 1

    def snapshot(self, extra: dict | None = None) -> dict:
        uptime_seconds = max(0, int((datetime.now(timezone.utc) - self.started_at).total_seconds()))
        avg_latency = (
            self.process_latency_ms_sum / self.process_latency_ms_count
            if self.process_latency_ms_count > 0
            else 0.0
        )
        payload = {
            "service": self.service_name,
            "started_at": self.started_at.isoformat(),
            "uptime_seconds": uptime_seconds,
            "processed_total": self.processed_total,
            "dropped_total": self.dropped_total,
            "error_total": self.error_total,
            "retried_total": self.retried_total,
            "avg_process_latency_ms": round(avg_latency, 3),
            "last_error": self.last_error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            payload["extra"] = extra
        return payload


class Stopwatch:
    def __init__(self) -> None:
        self._start = time.perf_counter()

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self._start) * 1000.0
