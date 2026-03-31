from __future__ import annotations

import logging
import os
import random
import signal
import time

from pydantic import ValidationError

from apps.correlator.linker import CandidateLinker, DependencyGraph, LinkConfig
from apps.correlator.watermark import WatermarkTracker
from apps.correlator.windows import EventTimeWindowManager
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
from libs.common.models import EventEnvelope
from libs.common.observability import ServiceMetrics, Stopwatch

logger = logging.getLogger("correlator")


class Correlator:
    def __init__(self) -> None:
        self.topic_normalized = os.getenv("TOPIC_NORMALIZED", "events.normalized")
        self.topic_candidates = os.getenv("TOPIC_INCIDENT_CANDIDATES", "incidents.candidates")
        self.group_id = os.getenv("KAFKA_GROUP_CORRELATOR", "ops-correlator-v1")

        self.allowed_lateness_seconds = int(os.getenv("CORR_ALLOWED_LATENESS_SECONDS", "20"))
        self.merge_gap_seconds = int(os.getenv("CORR_MERGE_GAP_SECONDS", "15"))
        self.close_after_seconds = int(os.getenv("CORR_CLOSE_AFTER_SECONDS", "45"))
        self.late_correction_grace_seconds = int(
            os.getenv("CORR_LATE_CORRECTION_GRACE_SECONDS", "40")
        )
        self.max_temporal_gap_seconds = int(os.getenv("CORR_MAX_LINK_GAP_SECONDS", "45"))
        self.metrics_log_interval_seconds = int(os.getenv("METRICS_LOG_INTERVAL_SECONDS", "30"))
        self.chaos_drop_rate = float(os.getenv("CHAOS_CORRELATOR_DROP_RATE", "0.0"))

        ensure_topics([self.topic_normalized, self.topic_candidates])

        self.consumer = build_consumer(
            group_id=self.group_id,
            topics=[self.topic_normalized],
            client_suffix="correlator",
        )
        self.producer = build_producer("correlator")

        self.watermark_tracker = WatermarkTracker(allowed_lateness_seconds=self.allowed_lateness_seconds)
        self.window_manager = EventTimeWindowManager(
            merge_gap_seconds=self.merge_gap_seconds,
            close_after_seconds=self.close_after_seconds,
            late_correction_grace_seconds=self.late_correction_grace_seconds,
        )
        self.linker = CandidateLinker(
            graph=DependencyGraph.from_services_config(),
            link_config=LinkConfig(max_temporal_gap_seconds=self.max_temporal_gap_seconds),
        )

        self.metrics = ServiceMetrics("correlator")
        self.running = True

    def stop(self, *_args) -> None:
        self.running = False

    def _emit_candidates(self, candidates: list[dict]) -> None:
        for candidate in candidates:
            key = candidate["affected_services"][0] if candidate["affected_services"] else "unknown"
            produce_json_with_retry(
                producer=self.producer,
                topic=self.topic_candidates,
                key=key,
                value=candidate,
                on_retry=lambda _attempt, _exc: self.metrics.record_retry(),
            )

    def _drain_finalized(self) -> None:
        finalized_clusters = self.window_manager.pop_finalized(self.watermark_tracker.watermark)
        if not finalized_clusters:
            return
        candidates = self.linker.ingest_finalized(finalized_clusters)
        if not candidates:
            return
        self._emit_candidates(candidates)
        logger.info(
            "Emitted %d incident candidates from %d finalized clusters",
            len(candidates),
            len(finalized_clusters),
        )

    def _handle_payload(self, payload: dict) -> None:
        envelope = EventEnvelope.model_validate(payload)
        self.watermark_tracker.observe(envelope.event_time)

        if self.watermark_tracker.is_late(envelope.event_time):
            cluster = self.window_manager.ingest_late(envelope)
            if cluster is None:
                logger.debug("Late event dropped no eligible correction window event_id=%s", envelope.event_id)
        else:
            self.window_manager.ingest(envelope)

    def run(self) -> None:
        logger.info("Correlator started group_id=%s", self.group_id)
        next_metrics_log = time.time() + self.metrics_log_interval_seconds

        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is None:
                self._drain_finalized()
                if time.time() >= next_metrics_log:
                    logger.info("Correlator metrics=%s", self.metrics.snapshot(consumer_lag_snapshot(self.consumer)))
                    next_metrics_log = time.time() + self.metrics_log_interval_seconds
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            watch = Stopwatch()
            payload = deserialize_json(msg.value())
            try:
                if self.chaos_drop_rate > 0 and random.random() < self.chaos_drop_rate:
                    self.metrics.record_dropped()
                else:
                    self._handle_payload(payload)
                    self.metrics.record_processed(watch.elapsed_ms())
            except ValidationError as exc:
                self.metrics.record_error(f"ValidationError: {exc}")
                logger.warning("Invalid normalized event dropped: %s", exc)
            except Exception as exc:  # noqa: BLE001
                self.metrics.record_error(f"{type(exc).__name__}: {exc}")
                logger.exception("Correlator event handling failure: %s", exc)
            finally:
                self._drain_finalized()
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
                logger.info("Correlator metrics=%s", self.metrics.snapshot(consumer_lag_snapshot(self.consumer)))
                next_metrics_log = time.time() + self.metrics_log_interval_seconds

        self.producer.flush(10)
        self.consumer.close()
        logger.info("Correlator stopped")


def main() -> None:
    configure_logging()
    correlator = Correlator()
    signal.signal(signal.SIGINT, correlator.stop)
    signal.signal(signal.SIGTERM, correlator.stop)
    correlator.run()


if __name__ == "__main__":
    main()
