from __future__ import annotations

import argparse
from datetime import timedelta, timezone
from typing import Any

from libs.common.ids import new_event_id
from libs.common.kafka import build_producer, ensure_topics, produce_json_with_retry
from libs.common.time import utc_now

TOPIC_LOGS = "events.raw.logs"
TOPIC_METRICS = "events.raw.metrics"
TOPIC_ALERTS = "events.raw.alerts"


def _event_time(offset_seconds: int) -> str:
    return (utc_now() + timedelta(seconds=offset_seconds)).astimezone(timezone.utc).isoformat()


def _raw_event(
    *,
    signal_type: str,
    service_name: str,
    instance_id: str,
    environment: str,
    region: str,
    severity: str,
    correlation_keys: dict[str, str],
    payload: dict[str, Any],
    offset_seconds: int,
) -> dict[str, Any]:
    return {
        "event_id": new_event_id(),
        "signal_type": signal_type,
        "event_time": _event_time(offset_seconds),
        "service_name": service_name,
        "instance_id": instance_id,
        "environment": environment,
        "region": region,
        "severity": severity,
        "correlation_keys": correlation_keys,
        "payload": payload,
    }


def build_distributed_demo(
    chain_id: str,
    environment: str,
    region: str,
    include_watermark_marker: bool = True,
) -> list[tuple[str, dict[str, Any]]]:
    common = {
        "fault_chain_id": chain_id,
        "request_id": f"{chain_id}-req-0001",
        "trace_id": f"{chain_id}-trace-0001",
        "scenario": "elite_distributed_demo",
        "fault_root_service": "orders-db",
    }

    events: list[tuple[str, dict[str, Any]]] = []

    # Root failure: orders-db latency and timeout pressure.
    events.append(
        (
            TOPIC_METRICS,
            _raw_event(
                signal_type="metric",
                service_name="orders-db",
                instance_id="orders-db-0",
                environment=environment,
                region=region,
                severity="critical",
                correlation_keys=common,
                payload={"metric_name": "p95_latency_ms", "value": 780.0, "unit": "ms", "tags": {"service": "orders-db"}},
                offset_seconds=0,
            ),
        )
    )
    events.append(
        (
            TOPIC_LOGS,
            _raw_event(
                signal_type="log",
                service_name="orders-db",
                instance_id="orders-db-0",
                environment=environment,
                region=region,
                severity="error",
                correlation_keys=common,
                payload={
                    "message": "orders-db query timeout under lock contention",
                    "logger": "postgres",
                    "exception": "TimeoutError",
                    "code": "DB_TIMEOUT",
                },
                offset_seconds=2,
            ),
        )
    )
    events.append(
        (
            TOPIC_ALERTS,
            _raw_event(
                signal_type="alert",
                service_name="orders-db",
                instance_id="orders-db-0",
                environment=environment,
                region=region,
                severity="critical",
                correlation_keys=common,
                payload={
                    "alert_name": "orders_db_latency_slo",
                    "status": "firing",
                    "threshold": 200.0,
                    "observed": 780.0,
                    "summary": "Orders DB latency exceeded SLO",
                },
                offset_seconds=3,
            ),
        )
    )

    # Propagation: orders-service starts failing from DB latency.
    events.append(
        (
            TOPIC_METRICS,
            _raw_event(
                signal_type="metric",
                service_name="orders-service",
                instance_id="orders-service-1",
                environment=environment,
                region=region,
                severity="critical",
                correlation_keys=common,
                payload={"metric_name": "error_rate", "value": 0.082, "unit": "ratio", "tags": {"service": "orders-service"}},
                offset_seconds=9,
            ),
        )
    )
    events.append(
        (
            TOPIC_METRICS,
            _raw_event(
                signal_type="metric",
                service_name="orders-service",
                instance_id="orders-service-1",
                environment=environment,
                region=region,
                severity="error",
                correlation_keys=common,
                payload={"metric_name": "p95_latency_ms", "value": 620.0, "unit": "ms", "tags": {"service": "orders-service"}},
                offset_seconds=10,
            ),
        )
    )
    events.append(
        (
            TOPIC_LOGS,
            _raw_event(
                signal_type="log",
                service_name="orders-service",
                instance_id="orders-service-1",
                environment=environment,
                region=region,
                severity="error",
                correlation_keys=common,
                payload={
                    "message": "orders-service upstream orders-db timeout",
                    "logger": "runtime",
                    "exception": "TimeoutError",
                    "code": "UPSTREAM_TIMEOUT",
                },
                offset_seconds=11,
            ),
        )
    )
    events.append(
        (
            TOPIC_ALERTS,
            _raw_event(
                signal_type="alert",
                service_name="orders-service",
                instance_id="orders-service-1",
                environment=environment,
                region=region,
                severity="critical",
                correlation_keys=common,
                payload={
                    "alert_name": "orders_service_error_burst",
                    "status": "firing",
                    "threshold": 0.02,
                    "observed": 0.082,
                    "summary": "Orders service 5xx error rate exceeded threshold",
                },
                offset_seconds=12,
            ),
        )
    )

    # Edge impact: api-gateway latency + failures.
    events.append(
        (
            TOPIC_METRICS,
            _raw_event(
                signal_type="metric",
                service_name="api-gateway",
                instance_id="api-gateway-2",
                environment=environment,
                region=region,
                severity="error",
                correlation_keys=common,
                payload={"metric_name": "p95_latency_ms", "value": 510.0, "unit": "ms", "tags": {"service": "api-gateway"}},
                offset_seconds=18,
            ),
        )
    )
    events.append(
        (
            TOPIC_METRICS,
            _raw_event(
                signal_type="metric",
                service_name="api-gateway",
                instance_id="api-gateway-2",
                environment=environment,
                region=region,
                severity="error",
                correlation_keys=common,
                payload={"metric_name": "error_rate", "value": 0.041, "unit": "ratio", "tags": {"service": "api-gateway"}},
                offset_seconds=19,
            ),
        )
    )
    events.append(
        (
            TOPIC_LOGS,
            _raw_event(
                signal_type="log",
                service_name="api-gateway",
                instance_id="api-gateway-2",
                environment=environment,
                region=region,
                severity="error",
                correlation_keys=common,
                payload={
                    "message": "api-gateway checkout route degraded due upstream timeout",
                    "logger": "nginx",
                    "exception": "GatewayTimeout",
                    "code": "UPSTREAM_TIMEOUT",
                },
                offset_seconds=20,
            ),
        )
    )
    events.append(
        (
            TOPIC_ALERTS,
            _raw_event(
                signal_type="alert",
                service_name="api-gateway",
                instance_id="api-gateway-2",
                environment=environment,
                region=region,
                severity="critical",
                correlation_keys=common,
                payload={
                    "alert_name": "api_gateway_checkout_failures",
                    "status": "firing",
                    "threshold": 0.02,
                    "observed": 0.041,
                    "summary": "Checkout API error spike observed at edge",
                },
                offset_seconds=21,
            ),
        )
    )

    # Async symptom to expand blast radius and ranking competition.
    events.append(
        (
            TOPIC_METRICS,
            _raw_event(
                signal_type="metric",
                service_name="worker-service",
                instance_id="worker-service-0",
                environment=environment,
                region=region,
                severity="critical",
                correlation_keys=common,
                payload={"metric_name": "queue_backlog", "value": 245.0, "unit": "count", "tags": {"service": "worker-service"}},
                offset_seconds=25,
            ),
        )
    )
    events.append(
        (
            TOPIC_LOGS,
            _raw_event(
                signal_type="log",
                service_name="worker-service",
                instance_id="worker-service-0",
                environment=environment,
                region=region,
                severity="warn",
                correlation_keys=common,
                payload={
                    "message": "worker-service backlog growth due upstream order processing failures",
                    "logger": "runtime",
                    "exception": None,
                    "code": "QUEUE_BACKLOG",
                },
                offset_seconds=26,
            ),
        )
    )

    if include_watermark_marker:
        # Advance watermark so clusters can close quickly even without long waits.
        marker_keys = {
            "scenario": "elite_distributed_demo",
            "marker": "watermark-advance",
            "trace_id": f"{chain_id}-marker",
        }
        events.append(
            (
                TOPIC_METRICS,
                _raw_event(
                    signal_type="metric",
                    service_name="inventory-db",
                    instance_id="inventory-db-0",
                    environment=environment,
                    region=region,
                    severity="info",
                    correlation_keys=marker_keys,
                    payload={"metric_name": "cpu", "value": 0.22, "unit": "ratio", "tags": {"service": "inventory-db"}},
                    offset_seconds=90,
                ),
            )
        )

    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Inject deterministic distributed multi-signal incident data")
    parser.add_argument("--chain-id", default="elite-chain-demo", help="Correlation fault chain id")
    parser.add_argument("--environment", default="dev", help="Environment tag")
    parser.add_argument("--region", default="local", help="Region tag")
    parser.add_argument(
        "--no-watermark-marker",
        action="store_true",
        help="Do not emit the future marker event used to advance watermark",
    )
    args = parser.parse_args()

    ensure_topics([TOPIC_LOGS, TOPIC_METRICS, TOPIC_ALERTS])
    producer = build_producer("distributed-demo-injector")

    events = build_distributed_demo(
        chain_id=args.chain_id,
        environment=args.environment,
        region=args.region,
        include_watermark_marker=not args.no_watermark_marker,
    )

    for topic, event in events:
        produce_json_with_retry(producer=producer, topic=topic, key=event["service_name"], value=event)

    producer.flush(10)
    print(f"Injected {len(events)} events for chain_id={args.chain_id}")


if __name__ == "__main__":
    main()
