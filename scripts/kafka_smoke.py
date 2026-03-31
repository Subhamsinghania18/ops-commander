from __future__ import annotations

import json
import time
import importlib
from datetime import timezone

from libs.common.ids import build_instance_id, new_event_id
from libs.common.kafka import build_producer, deserialize_json, ensure_topics, serialize_json
from libs.common.time import utc_now

RAW_TOPIC = "events.raw.logs"
NORMALIZED_TOPIC = "events.normalized"


def main() -> None:
    Consumer = getattr(importlib.import_module("confluent_kafka"), "Consumer")

    ensure_topics([RAW_TOPIC, NORMALIZED_TOPIC])

    producer = build_producer("smoke-producer")
    event = {
        "event_id": new_event_id(),
        "signal_type": "log",
        "event_time": utc_now().astimezone(timezone.utc).isoformat(),
        "service_name": "api-gateway",
        "instance_id": build_instance_id("api-gateway", 0),
        "environment": "dev",
        "region": "local",
        "severity": "info",
        "correlation_keys": {"smoke": "true"},
        "payload": {
            "message": "smoke event",
            "logger": "smoke",
            "exception": None,
            "code": None,
        },
    }
    producer.produce(topic=RAW_TOPIC, key="api-gateway", value=serialize_json(event))
    producer.flush(5)
    print("Produced raw event:", json.dumps(event))

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "ops-smoke-v1",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([NORMALIZED_TOPIC])

    deadline = time.time() + 20
    while time.time() < deadline:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            continue
        payload = deserialize_json(msg.value())
        if payload.get("correlation_keys", {}).get("smoke") == "true":
            print("Observed normalized event:", json.dumps(payload))
            consumer.close()
            return

    consumer.close()
    raise SystemExit("Timed out waiting for normalized smoke event. Ensure ingestor is running.")


if __name__ == "__main__":
    main()
