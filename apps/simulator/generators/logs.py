from __future__ import annotations

import random


def build_log_event(service_name: str, effects: dict[str, float]) -> dict:
    err_mul = effects.get("error_rate_multiplier", 1.0)
    if err_mul >= 3.0:
        return {
            "message": f"{service_name}: upstream timeout while handling request",
            "logger": "runtime",
            "exception": "TimeoutError",
            "code": "UPSTREAM_TIMEOUT",
        }

    templates = [
        f"{service_name}: request processed successfully",
        f"{service_name}: cache miss for key",
        f"{service_name}: background heartbeat ok",
    ]
    return {
        "message": random.choice(templates),
        "logger": "runtime",
        "exception": None,
        "code": None,
    }
