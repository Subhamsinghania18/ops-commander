from __future__ import annotations

import json
from pathlib import Path

from libs.common.models import AlertEventPayload, EventEnvelope, LogEventPayload, MetricEventPayload


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out = root / "schemas"
    out.mkdir(parents=True, exist_ok=True)

    mapping = {
        "event_envelope_v1.json": EventEnvelope.model_json_schema(),
        "log_event_v1.json": LogEventPayload.model_json_schema(),
        "metric_event_v1.json": MetricEventPayload.model_json_schema(),
        "alert_event_v1.json": AlertEventPayload.model_json_schema(),
    }

    for filename, schema in mapping.items():
        (out / filename).write_text(json.dumps(schema, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
