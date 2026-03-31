from __future__ import annotations

import logging
import os
import random
import signal
import time

from apps.ingestor.dedupe import IdempotencyFilter
from apps.ingestor.normalizer import normalize_event
from apps.ingestor.quarantine import save_quarantine_event
from apps.ingestor.validators import validate_raw_event
from libs.common.kafka import (
    build_consumer,
    build_producer,
    commit_with_retry,
    consumer_lag_snapshot,
    deserialize_json,
    ensure_topics,
    produce_json_with_retry,
)
from libs.common.logging import configure_logging
from libs.common.observability import ServiceMetrics, Stopwatch
from libs.storage.db import init_db
from libs.storage.repositories.incidents import IngestedEventsRepository

logger = logging.getLogger("ingestor")


class Ingestor:
    def __init__(self) -> None:
        self.topic_raw_logs = os.getenv("TOPIC_RAW_LOGS", "events.raw.logs")
        self.topic_raw_metrics = os.getenv("TOPIC_RAW_METRICS", "events.raw.metrics")
        self.topic_raw_alerts = os.getenv("TOPIC_RAW_ALERTS", "events.raw.alerts")
        self.topic_normalized = os.getenv("TOPIC_NORMALIZED", "events.normalized")
        self.group_id = os.getenv("KAFKA_GROUP_INGESTOR", "ops-ingestor-v1")
        self.metrics_log_interval_seconds = int(os.getenv("METRICS_LOG_INTERVAL_SECONDS", "30"))
        self.chaos_drop_rate = float(os.getenv("CHAOS_INGESTOR_DROP_RATE", "0.0"))

        ensure_topics(
            [
                self.topic_raw_logs,
                self.topic_raw_metrics,
                self.topic_raw_alerts,
                self.topic_normalized,
            ]
        )

        init_db()
        self.repo = IngestedEventsRepository()
        self.idem_cache = IdempotencyFilter(ttl_seconds=1200)
        self.consumer = build_consumer(
            group_id=self.group_id,
            topics=[self.topic_raw_logs, self.topic_raw_metrics, self.topic_raw_alerts],
            client_suffix="ingestor",
        )
        self.producer = build_producer("ingestor")
        self.metrics = ServiceMetrics("ingestor")
        self.running = True

    def stop(self, *_args) -> None:
        self.running = False

    def _handle_message(self, source_topic: str, payload: dict) -> None:
        raw = validate_raw_event(payload)
        envelope = normalize_event(raw)
        idem_key = envelope.idempotency_key()

        if self.idem_cache.seen_recently(idem_key):
            logger.debug("Duplicate dropped by in-memory cache event_id=%s", envelope.event_id)
            return

        if self.repo.exists_by_idempotency_key(idem_key):
            logger.debug("Duplicate dropped by persistent lookup event_id=%s", envelope.event_id)
            return

        serialized = envelope.model_dump(mode="json")
        produce_json_with_retry(
            producer=self.producer,
            topic=self.topic_normalized,
            key=envelope.service_name,
            value=serialized,
            on_retry=lambda _attempt, _exc: self.metrics.record_retry(),
        )

        self.repo.insert_event(
            event_id=envelope.event_id,
            event_type=envelope.event_type.value,
            service_name=envelope.service_name,
            event_time=envelope.event_time,
            ingest_time=envelope.ingest_time,
            idempotency_key=idem_key,
            late=envelope.quality_flags.late,
            normalized_payload=serialized,
        )

    def run(self) -> None:
        logger.info("Ingestor started group_id=%s", self.group_id)
        next_metrics_log = time.time() + self.metrics_log_interval_seconds
        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is None:
                if time.time() >= next_metrics_log:
                    logger.info("Ingestor metrics=%s", self.metrics.snapshot(consumer_lag_snapshot(self.consumer)))
                    next_metrics_log = time.time() + self.metrics_log_interval_seconds
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            watch = Stopwatch()
            payload = deserialize_json(msg.value())
            source_topic = msg.topic()
            try:
                if self.chaos_drop_rate > 0 and random.random() < self.chaos_drop_rate:
                    self.metrics.record_dropped()
                else:
                    self._handle_message(source_topic, payload)
                    self.metrics.record_processed(watch.elapsed_ms())
            except Exception as exc:  # noqa: BLE001
                self.metrics.record_error(f"{type(exc).__name__}: {exc}")
                reason = f"{type(exc).__name__}: {exc}"
                save_quarantine_event(source_topic=source_topic, reason=reason, payload=payload)
                logger.warning("Quarantined event from %s: %s", source_topic, reason)
            finally:
                try:
                    commit_with_retry(
                        consumer=self.consumer,
                        message=msg,
                        on_retry=lambda _attempt, _exc: self.metrics.record_retry(),
                    )
                except Exception as exc:  # noqa: BLE001
                    self.metrics.record_error(f"commit:{type(exc).__name__}: {exc}")
                    logger.exception("Commit failure: %s", exc)

            if time.time() >= next_metrics_log:
                logger.info("Ingestor metrics=%s", self.metrics.snapshot(consumer_lag_snapshot(self.consumer)))
                next_metrics_log = time.time() + self.metrics_log_interval_seconds

        self.producer.flush(10)
        self.consumer.close()
        logger.info("Ingestor stopped")


def main() -> None:
    configure_logging()
    ingestor = Ingestor()
    signal.signal(signal.SIGINT, ingestor.stop)
    signal.signal(signal.SIGTERM, ingestor.stop)
    ingestor.run()


if __name__ == "__main__":
    main()
