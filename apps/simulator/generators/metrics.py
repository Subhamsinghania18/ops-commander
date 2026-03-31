from __future__ import annotations

import random


def build_metric_events(service_name: str, base: dict[str, float], effects: dict[str, float]) -> list[dict]:
    latency_mul = effects.get("p95_latency_multiplier", effects.get("latency_multiplier", 1.0))
    err_mul = effects.get("error_rate_multiplier", 1.0)
    backlog_mul = effects.get("queue_backlog_multiplier", 1.0)

    cpu = min(0.99, base["cpu"] * random.uniform(0.9, 1.2) * max(1.0, err_mul / 2))
    memory = min(0.99, base["memory"] * random.uniform(0.95, 1.15))
    error_rate = max(0.0, base["error_rate"] * random.uniform(0.8, 1.2) * err_mul)
    p95_latency_ms = max(1.0, base["p95_latency_ms"] * random.uniform(0.9, 1.1) * latency_mul)
    queue_backlog = max(0.0, base["queue_backlog"] * random.uniform(0.7, 1.3) * backlog_mul)

    return [
        {"metric_name": "cpu", "value": cpu, "unit": "ratio", "tags": {"service": service_name}},
        {
            "metric_name": "memory",
            "value": memory,
            "unit": "ratio",
            "tags": {"service": service_name},
        },
        {
            "metric_name": "error_rate",
            "value": error_rate,
            "unit": "ratio",
            "tags": {"service": service_name},
        },
        {
            "metric_name": "p95_latency_ms",
            "value": p95_latency_ms,
            "unit": "ms",
            "tags": {"service": service_name},
        },
        {
            "metric_name": "queue_backlog",
            "value": queue_backlog,
            "unit": "count",
            "tags": {"service": service_name},
        },
    ]
