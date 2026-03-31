from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import timezone

from libs.common.ids import new_event_id
from libs.common.kafka import build_producer, ensure_topics, produce_json_with_retry
from libs.common.time import utc_now


def prom_query(base_url: str, query: str) -> list[dict]:
    encoded = urllib.parse.urlencode({"query": query})
    url = f"{base_url.rstrip('/')}/api/v1/query?{encoded}"
    with urllib.request.urlopen(url, timeout=15) as resp:  # noqa: S310
        body = json.loads(resp.read().decode("utf-8"))
    if body.get("status") != "success":
        return []
    return body.get("data", {}).get("result", [])


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull Prometheus metrics and feed raw metrics topic")
    parser.add_argument("--prom-url", required=True, help="Prometheus base URL, e.g. http://localhost:9090")
    parser.add_argument("--service", default="external-service")
    parser.add_argument("--topic", default=os.getenv("TOPIC_RAW_METRICS", "events.raw.metrics"))
    parser.add_argument("--environment", default=os.getenv("ENVIRONMENT", "dev"))
    parser.add_argument("--region", default=os.getenv("REGION", "local"))
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="PromQL query in form metric_name=promql_expression",
    )
    args = parser.parse_args()

    metric_queries = args.query or [
        "cpu=avg(rate(process_cpu_seconds_total[1m]))",
        "memory=avg(process_resident_memory_bytes)",
        "error_rate=sum(rate(http_requests_total{status=~\"5..\"}[1m]))",
        "p95_latency_ms=histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))*1000",
    ]

    ensure_topics([args.topic])
    producer = build_producer("external-prom-feed")

    now = utc_now().astimezone(timezone.utc).isoformat()
    published = 0

    for spec in metric_queries:
        if "=" not in spec:
            continue
        metric_name, query = spec.split("=", 1)
        result = prom_query(args.prom_url, query)
        if not result:
            continue

        values = []
        for item in result:
            value = item.get("value", [None, None])[1]
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                continue
        if not values:
            continue

        metric_value = sum(values) / len(values)
        severity = "warn" if metric_name in {"error_rate", "p95_latency_ms"} and metric_value > 0 else "info"

        event = {
            "event_id": new_event_id(),
            "signal_type": "metric",
            "event_time": now,
            "service_name": args.service,
            "instance_id": f"{args.service}-prom-feed",
            "environment": args.environment,
            "region": args.region,
            "severity": severity,
            "correlation_keys": {"source": "prometheus-feed", "query_metric": metric_name},
            "payload": {
                "metric_name": metric_name,
                "value": metric_value,
                "unit": "ratio" if metric_name in {"cpu", "error_rate"} else "value",
                "tags": {"source": "prometheus", "service": args.service},
            },
        }

        produce_json_with_retry(
            producer=producer,
            topic=args.topic,
            key=args.service,
            value=event,
        )
        published += 1

    producer.flush(5)
    print(json.dumps({"published_metric_events": published, "service": args.service, "topic": args.topic}))


if __name__ == "__main__":
    main()
