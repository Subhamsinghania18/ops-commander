from __future__ import annotations


def build_alert_events(service_name: str, effects: dict[str, float]) -> list[dict]:
    alerts: list[dict] = []

    if effects.get("error_rate_multiplier", 1.0) >= 2.0:
        alerts.append(
            {
                "alert_name": "high_error_rate",
                "status": "firing",
                "threshold": 0.02,
                "observed": 0.02 * effects.get("error_rate_multiplier", 1.0),
                "summary": f"Elevated error rate detected for {service_name}",
            }
        )

    if effects.get("p95_latency_multiplier", effects.get("latency_multiplier", 1.0)) >= 2.0:
        alerts.append(
            {
                "alert_name": "latency_slo_violation",
                "status": "firing",
                "threshold": 200,
                "observed": 200 * effects.get("p95_latency_multiplier", 1.0),
                "summary": f"P95 latency SLO violation for {service_name}",
            }
        )

    if effects.get("queue_backlog_multiplier", 1.0) >= 3.0:
        alerts.append(
            {
                "alert_name": "worker_backlog",
                "status": "firing",
                "threshold": 50,
                "observed": 50 * effects.get("queue_backlog_multiplier", 1.0),
                "summary": f"Queue backlog growing for {service_name}",
            }
        )

    return alerts
