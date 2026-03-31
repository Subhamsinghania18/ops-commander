from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
from datetime import timezone
from pathlib import Path

from apps.simulator.faults.injector import active_effects, load_scenario
from apps.simulator.generators.alerts import build_alert_events
from apps.simulator.generators.logs import build_log_event
from apps.simulator.generators.metrics import build_metric_events
from apps.simulator.topology import load_service_topology
from libs.common.ids import build_instance_id, new_event_id
from libs.common.kafka import build_producer, ensure_topics, produce_json_with_retry
from libs.common.logging import configure_logging
from libs.common.observability import ServiceMetrics
from libs.common.time import maybe_shift_timestamp, utc_now

logger = logging.getLogger("simulator")


class Simulator:
    def __init__(self) -> None:
        self.environment = os.getenv("ENVIRONMENT", "dev")
        self.region = os.getenv("REGION", "local")
        self.tick_seconds = float(os.getenv("SIM_TICK_SECONDS", "1.0"))
        self.out_of_order_pct = float(os.getenv("SIM_OUT_OF_ORDER_PCT", "0.05"))
        self.scenario_name = os.getenv("SIM_SCENARIO", "default_cascade")
        self.archive_enabled = os.getenv("SIM_ARCHIVE_ENABLED", "true").lower() == "true"
        self.archive_path = Path(os.getenv("SIM_ARCHIVE_PATH", "data/archive"))
        self.chaos_drop_rate = float(os.getenv("CHAOS_SIM_DROP_RATE", "0.0"))
        self.metrics_log_interval_seconds = int(os.getenv("METRICS_LOG_INTERVAL_SECONDS", "30"))

        self.topic_logs = os.getenv("TOPIC_RAW_LOGS", "events.raw.logs")
        self.topic_metrics = os.getenv("TOPIC_RAW_METRICS", "events.raw.metrics")
        self.topic_alerts = os.getenv("TOPIC_RAW_ALERTS", "events.raw.alerts")

        self.topology = load_service_topology()
        self.scenario = load_scenario(self.scenario_name)
        self.producer = build_producer("simulator")
        ensure_topics([self.topic_logs, self.topic_metrics, self.topic_alerts])

        self.metrics = ServiceMetrics("simulator")
        self._archive_file = self._open_archive_file()
        self.running = True

    def stop(self, *_args) -> None:
        self.running = False

    def _emit(self, topic: str, event: dict) -> None:
        if self.chaos_drop_rate > 0 and random.random() < self.chaos_drop_rate:
            self.metrics.record_dropped()
            return

        produce_json_with_retry(
            producer=self.producer,
            topic=topic,
            key=event["service_name"],
            value=event,
            on_retry=lambda _attempt, _exc: self.metrics.record_retry(),
        )
        self.metrics.record_processed(0.0)

        if self._archive_file is not None:
            archive_record = {
                "topic": topic,
                "key": event["service_name"],
                "event_time": event.get("event_time"),
                "value": event,
            }
            self._archive_file.write(json.dumps(archive_record, separators=(",", ":")) + "\n")

    @staticmethod
    def _metric_severity(payload: dict) -> str:
        name = str(payload.get("metric_name", ""))
        value = float(payload.get("value", 0.0) or 0.0)

        if name == "error_rate":
            if value >= 0.04:
                return "critical"
            if value >= 0.02:
                return "error"
            if value >= 0.01:
                return "warn"
            return "info"

        if name == "p95_latency_ms":
            if value >= 400:
                return "critical"
            if value >= 250:
                return "error"
            if value >= 140:
                return "warn"
            return "info"

        if name == "queue_backlog":
            if value >= 120:
                return "critical"
            if value >= 60:
                return "error"
            if value >= 25:
                return "warn"
            return "info"

        if name in {"cpu", "memory"}:
            if value >= 0.95:
                return "error"
            if value >= 0.85:
                return "warn"
            return "info"

        return "info"

    def run(self) -> None:
        logger.info("Starting simulator scenario=%s services=%d", self.scenario_name, len(self.topology))
        start = time.monotonic()
        next_metrics_log = time.time() + self.metrics_log_interval_seconds
        fault_chain_id = (
            f"{self.scenario_name}-{self.scenario.root_fault.get('service', 'unknown')}-"
            f"{self.scenario.start_after_seconds}"
        )

        while self.running:
            elapsed = int(time.monotonic() - start)
            service_effects = active_effects(elapsed, self.scenario)

            for idx, service_name in enumerate(self.topology.keys()):
                now = utc_now()
                event_time = maybe_shift_timestamp(now, self.out_of_order_pct)
                effects = service_effects.get(service_name, {})
                correlation_keys = {
                    "scenario": self.scenario_name,
                    "tick": str(elapsed),
                    "trace_id": f"sim-{elapsed:08d}-{idx:02d}",
                }
                if effects:
                    # Propagated faults share a stable chain id and coarse request id
                    # so cross-service correlation has meaningful linkage.
                    correlation_keys.update(
                        {
                            "fault_chain_id": fault_chain_id,
                            "fault_root_service": str(self.scenario.root_fault.get("service", "unknown")),
                            "request_id": f"{fault_chain_id}-req-{elapsed // 5:06d}",
                            "trace_id": f"{fault_chain_id}-{elapsed:08d}",
                        }
                    )

                log_payload = build_log_event(service_name, effects)
                log_severity = (
                    "error"
                    if log_payload.get("exception")
                    else "warn"
                    if effects.get("latency_multiplier", 1.0) >= 2.0
                    else "info"
                )
                log_event = {
                    "event_id": new_event_id(),
                    "signal_type": "log",
                    "event_time": event_time.astimezone(timezone.utc).isoformat(),
                    "service_name": service_name,
                    "instance_id": build_instance_id(service_name, idx % 3),
                    "environment": self.environment,
                    "region": self.region,
                    "severity": log_severity,
                    "correlation_keys": correlation_keys,
                    "payload": log_payload,
                }
                self._emit(self.topic_logs, log_event)

                base = {
                    "cpu": 0.35,
                    "memory": 0.45,
                    "error_rate": 0.005,
                    "p95_latency_ms": 75,
                    "queue_backlog": 5,
                }
                for payload in build_metric_events(service_name, base, effects):
                    metric_event = {
                        "event_id": new_event_id(),
                        "signal_type": "metric",
                        "event_time": event_time.astimezone(timezone.utc).isoformat(),
                        "service_name": service_name,
                        "instance_id": build_instance_id(service_name, idx % 3),
                        "environment": self.environment,
                        "region": self.region,
                        "severity": self._metric_severity(payload),
                        "correlation_keys": correlation_keys,
                        "payload": payload,
                    }
                    self._emit(self.topic_metrics, metric_event)

                for payload in build_alert_events(service_name, effects):
                    alert_event = {
                        "event_id": new_event_id(),
                        "signal_type": "alert",
                        "event_time": event_time.astimezone(timezone.utc).isoformat(),
                        "service_name": service_name,
                        "instance_id": build_instance_id(service_name, idx % 3),
                        "environment": self.environment,
                        "region": self.region,
                        "severity": "critical",
                        "correlation_keys": correlation_keys,
                        "payload": payload,
                    }
                    self._emit(self.topic_alerts, alert_event)

                # Emit low-rate malformed traffic to exercise quarantine path.
                if random.random() < 0.01:
                    malformed = {
                        "event_id": new_event_id(),
                        "signal_type": "metric",
                        "service_name": service_name,
                        "payload": {"metric_name": "error_rate"},
                    }
                    self._emit(self.topic_metrics, malformed)

            self.producer.flush(2)
            if self._archive_file is not None:
                self._archive_file.flush()

            if time.time() >= next_metrics_log:
                logger.info("Simulator metrics=%s", self.metrics.snapshot())
                next_metrics_log = time.time() + self.metrics_log_interval_seconds

            time.sleep(self.tick_seconds)

        self.producer.flush(5)
        if self._archive_file is not None:
            self._archive_file.close()
        logger.info("Simulator stopped")

    def _open_archive_file(self):
        if not self.archive_enabled:
            return None

        date_part = utc_now().strftime("%Y-%m-%d")
        dir_path = self.archive_path / date_part
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"sim-{int(time.time())}.jsonl"
        logger.info("Simulator archive file=%s", file_path)
        return file_path.open("a", encoding="utf-8")


def main() -> None:
    configure_logging()
    simulator = Simulator()
    signal.signal(signal.SIGINT, simulator.stop)
    signal.signal(signal.SIGTERM, simulator.stop)
    simulator.run()


if __name__ == "__main__":
    main()
