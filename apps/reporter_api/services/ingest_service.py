from __future__ import annotations

import os
from datetime import timezone
from typing import Any

from libs.common.ids import new_event_id
from libs.common.kafka import build_producer, ensure_topics, produce_json_with_retry
from libs.common.time import utc_now


class IngestService:
    def __init__(self) -> None:
        self.environment = os.getenv("ENVIRONMENT", "dev")
        self.region = os.getenv("REGION", "local")
        self.topic_alerts = os.getenv("TOPIC_RAW_ALERTS", "events.raw.alerts")
        ensure_topics([self.topic_alerts])
        self.producer = build_producer("reporter-ingest")

    def ingest_alertmanager_payload(self, payload: dict[str, Any]) -> dict:
        alerts = payload.get("alerts")
        if isinstance(alerts, list):
            items = alerts
        else:
            items = [payload]

        published = 0
        dropped = 0
        for raw in items:
            event = self._to_alert_event(raw)
            if event is None:
                dropped += 1
                continue

            produce_json_with_retry(
                producer=self.producer,
                topic=self.topic_alerts,
                key=event["service_name"],
                value=event,
            )
            published += 1

        # IngestService is request-scoped in the API dependency graph.
        # Flush to avoid losing queued messages when the producer gets disposed.
        self.producer.flush(5)

        return {"published": published, "dropped": dropped, "topic": self.topic_alerts}

    def _to_alert_event(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        raw_labels = raw.get("labels")
        raw_annotations = raw.get("annotations")
        labels: dict[str, Any] = raw_labels if isinstance(raw_labels, dict) else {}
        annotations: dict[str, Any] = raw_annotations if isinstance(raw_annotations, dict) else {}

        service_name = (
            labels.get("service")
            or labels.get("app")
            or labels.get("job")
            or labels.get("instance")
            or "external-service"
        )

        alert_name = labels.get("alertname") or raw.get("alert_name") or "external_alert"
        status = raw.get("status") if isinstance(raw.get("status"), str) else "firing"
        summary = annotations.get("summary") or annotations.get("description") or "External alert received"

        if not isinstance(summary, str) or not summary.strip():
            return None

        sev = str(labels.get("severity", "critical")).lower()
        if sev not in {"debug", "info", "warn", "error", "critical"}:
            sev = "critical"

        now = utc_now().astimezone(timezone.utc).isoformat()

        return {
            "event_id": new_event_id(),
            "signal_type": "alert",
            "event_time": now,
            "service_name": service_name,
            "instance_id": labels.get("instance", f"{service_name}-external"),
            "environment": self.environment,
            "region": self.region,
            "severity": sev,
            "correlation_keys": {
                "source": "alertmanager-webhook",
                "fingerprint": str(raw.get("fingerprint", "")),
                "alertname": alert_name,
            },
            "payload": {
                "alert_name": alert_name,
                "status": "resolved" if str(status).lower() == "resolved" else "firing",
                "threshold": None,
                "observed": None,
                "summary": summary,
            },
        }
