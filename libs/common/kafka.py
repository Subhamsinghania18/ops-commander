from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
import importlib
import time
from typing import Any

logger = logging.getLogger(__name__)


def kafka_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")


def kafka_client_id(suffix: str) -> str:
    base = os.getenv("KAFKA_CLIENT_ID", "ops-commander")
    return f"{base}-{suffix}"


def build_producer(client_suffix: str = "producer") -> Any:
    Producer = getattr(importlib.import_module("confluent_kafka"), "Producer")

    return Producer(
        {
            "bootstrap.servers": kafka_bootstrap_servers(),
            "client.id": kafka_client_id(client_suffix),
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
        }
    )


def build_consumer(group_id: str, topics: list[str], client_suffix: str = "consumer") -> Any:
    Consumer = getattr(importlib.import_module("confluent_kafka"), "Consumer")

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap_servers(),
            "group.id": group_id,
            "client.id": kafka_client_id(client_suffix),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )
    consumer.subscribe(topics)
    return consumer


def ensure_topics(topic_names: Iterable[str], partitions: int = 3, replication_factor: int = 1) -> None:
    admin_module = importlib.import_module("confluent_kafka.admin")
    AdminClient = getattr(admin_module, "AdminClient")
    NewTopic = getattr(admin_module, "NewTopic")

    admin = AdminClient({"bootstrap.servers": kafka_bootstrap_servers()})
    metadata = admin.list_topics(timeout=10)
    existing = set(metadata.topics.keys())
    missing = [
        NewTopic(name, num_partitions=partitions, replication_factor=replication_factor)
        for name in topic_names
        if name not in existing
    ]
    if not missing:
        return
    futures = admin.create_topics(missing)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Created topic %s", topic)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Topic create result for %s: %s", topic, exc)


def serialize_json(value: dict) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def deserialize_json(value: bytes | None) -> dict:
    if not value:
        return {}
    return json.loads(value.decode("utf-8"))


def produce_json_with_retry(
    producer: Any,
    topic: str,
    key: str,
    value: dict,
    max_attempts: int = 4,
    on_retry: Any = None,
) -> None:
    payload = serialize_json(value)
    attempt = 1
    while True:
        try:
            producer.produce(topic=topic, key=key, value=payload)
            producer.poll(0)
            return
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_attempts:
                raise
            if on_retry:
                on_retry(attempt, exc)
            producer.poll(0.1)
            time.sleep(min(0.8, 0.05 * (2 ** (attempt - 1))))
            attempt += 1


def commit_with_retry(consumer: Any, message: Any, max_attempts: int = 4, on_retry: Any = None) -> None:
    attempt = 1
    while True:
        try:
            consumer.commit(message=message, asynchronous=False)
            return
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_attempts:
                raise
            if on_retry:
                on_retry(attempt, exc)
            time.sleep(min(0.8, 0.05 * (2 ** (attempt - 1))))
            attempt += 1


def consumer_lag_snapshot(consumer: Any) -> dict:
    lag_by_partition: dict[str, int] = {}
    total_lag = 0

    try:
        assignments = consumer.assignment() or []
        for tp in assignments:
            low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
            position = consumer.position([tp])[0].offset
            lag = max(0, int(high - max(0, position)))
            key = f"{tp.topic}:{tp.partition}"
            lag_by_partition[key] = lag
            total_lag += lag
    except Exception:  # noqa: BLE001
        return {"total_lag": -1, "lag_by_partition": {}}

    return {"total_lag": total_lag, "lag_by_partition": lag_by_partition}
