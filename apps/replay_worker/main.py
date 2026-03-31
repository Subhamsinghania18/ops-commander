from __future__ import annotations

import importlib
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from apps.replay_worker.evaluator import ReplayEvaluator
from apps.replay_worker.reader import ArchiveReader
from apps.replay_worker.replayer import StreamReplayer
from libs.common.kafka import build_producer, deserialize_json, ensure_topics
from libs.common.logging import configure_logging
from libs.common.observability import ServiceMetrics, Stopwatch
from libs.storage.db import init_db
from libs.storage.repositories.replay_jobs import ReplayJobRepository

logger = logging.getLogger("replay-worker")


class ReplayWorker:
    def __init__(self) -> None:
        self.mode = os.getenv("REPLAY_WORKER_MODE", "queue")
        self.archive_root = Path(os.getenv("REPLAY_ARCHIVE_PATH", "data/archive"))
        self.evaluations_dir = Path(os.getenv("REPLAY_EVALUATIONS_PATH", "data/archive/evaluations"))
        self.poll_seconds = float(os.getenv("REPLAY_POLL_SECONDS", "3"))
        self.linger_seconds = float(os.getenv("REPLAY_DIAGNOSIS_LINGER_SECONDS", "15"))
        self.default_speed = float(os.getenv("REPLAY_DEFAULT_SPEED_FACTOR", "1.0"))
        self.metrics_log_interval_seconds = int(os.getenv("METRICS_LOG_INTERVAL_SECONDS", "30"))

        self.topic_diagnosed = os.getenv("TOPIC_INCIDENT_DIAGNOSED", "incidents.diagnosed")
        self.topics_replay_default = [
            os.getenv("TOPIC_RAW_LOGS", "events.raw.logs"),
            os.getenv("TOPIC_RAW_METRICS", "events.raw.metrics"),
            os.getenv("TOPIC_RAW_ALERTS", "events.raw.alerts"),
            os.getenv("TOPIC_NORMALIZED", "events.normalized"),
            os.getenv("TOPIC_INCIDENT_CANDIDATES", "incidents.candidates"),
            self.topic_diagnosed,
        ]

        self.running = True
        self.jobs = ReplayJobRepository()
        self.reader = ArchiveReader(self.archive_root)
        self.evaluator = ReplayEvaluator()
        self.scenario_root_map = self._load_scenario_roots()
        self.metrics = ServiceMetrics("replay-worker")

        init_db()
        ensure_topics(self.topics_replay_default)
        self.evaluations_dir.mkdir(parents=True, exist_ok=True)

    def stop(self, *_args) -> None:
        self.running = False

    def run(self) -> None:
        logger.info("Replay worker started mode=%s", self.mode)

        if self.mode == "oneshot":
            self._run_oneshot()
            return

        next_metrics_log = time.time() + self.metrics_log_interval_seconds

        while self.running:
            queued = self.jobs.get_queued_jobs(limit=5)
            if not queued:
                time.sleep(self.poll_seconds)
                if time.time() >= next_metrics_log:
                    logger.info("Replay worker metrics=%s", self.metrics.snapshot())
                    next_metrics_log = time.time() + self.metrics_log_interval_seconds
                continue

            for job in queued:
                if not self.running:
                    break
                self._process_job(job.job_id, dict(job.request_payload or {}))

            if time.time() >= next_metrics_log:
                logger.info("Replay worker metrics=%s", self.metrics.snapshot())
                next_metrics_log = time.time() + self.metrics_log_interval_seconds

        logger.info("Replay worker stopped")

    def _run_oneshot(self) -> None:
        payload = {
            "archive_path": os.getenv("REPLAY_ARCHIVE_PATH", "data/archive"),
            "speed_factor": float(os.getenv("REPLAY_DEFAULT_SPEED_FACTOR", "1.0")),
            "scenario_name": os.getenv("REPLAY_SCENARIO", None),
        }
        self._process_job(job_id=f"oneshot-{int(time.time())}", payload=payload, persist=False)

    def _process_job(self, job_id: str, payload: dict, persist: bool = True) -> None:
        logger.info("Processing replay job_id=%s", job_id)
        watch = Stopwatch()
        if persist:
            self.jobs.update_status(job_id, "running")

        try:
            archive_path = Path(payload.get("archive_path") or self.archive_root)
            reader = ArchiveReader(archive_path)

            start_time = self._parse_dt(payload.get("start_time"))
            end_time = self._parse_dt(payload.get("end_time"))
            scenario_name = payload.get("scenario_name")
            speed_factor = float(payload.get("speed_factor", self.default_speed))

            records = reader.read_records(
                start_time=start_time,
                end_time=end_time,
                scenario_name=scenario_name,
            )
            if not records:
                raise RuntimeError("no replay records found for selected criteria")

            scenario_name = scenario_name or self._infer_scenario(records)
            expected_root = self.scenario_root_map.get(scenario_name) if scenario_name else None

            diagnosed_consumer = self._build_diagnosed_consumer(group_suffix=job_id)
            producer = build_producer(f"replay-worker-{job_id}")

            replay_stats = StreamReplayer(producer=producer, speed_factor=speed_factor).replay(records)
            diagnosed_items = self._collect_diagnosed(diagnosed_consumer)
            diagnosis_eval = self.evaluator.evaluate(diagnosed_items, expected_root_service=expected_root)

            report = {
                "job_id": job_id,
                "status": "completed",
                "scenario_name": scenario_name,
                "expected_root_service": expected_root,
                "archive_path": str(archive_path),
                "replay_stats": replay_stats.__dict__,
                "diagnosed_count": len(diagnosed_items),
                "evaluation": diagnosis_eval,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            report_path = self._write_evaluation_report(report)
            self.metrics.record_processed(watch.elapsed_ms())

            if persist:
                updated_payload = dict(payload)
                updated_payload["result_path"] = str(report_path)
                updated_payload["scenario_name"] = scenario_name
                updated_payload["expected_root_service"] = expected_root
                updated_payload["diagnosed_count"] = len(diagnosed_items)
                updated_payload["summary"] = diagnosis_eval["summary"]
                self.jobs.update_payload_and_status(job_id, payload=updated_payload, status="completed")

            logger.info("Replay job completed job_id=%s report=%s", job_id, report_path)
        except Exception as exc:  # noqa: BLE001
            self.metrics.record_error(f"{type(exc).__name__}: {exc}")
            logger.exception("Replay job failed job_id=%s error=%s", job_id, exc)
            if persist:
                failed_payload = dict(payload)
                failed_payload["error"] = f"{type(exc).__name__}: {exc}"
                self.jobs.update_payload_and_status(job_id, payload=failed_payload, status="failed")

    def _collect_diagnosed(self, consumer) -> list[dict]:
        diagnosed: list[dict] = []
        deadline = time.time() + self.linger_seconds

        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            payload = deserialize_json(msg.value())
            if isinstance(payload, dict) and payload.get("incident_id"):
                diagnosed.append(payload)

        consumer.close()
        return diagnosed

    def _build_diagnosed_consumer(self, group_suffix: str):
        Consumer = getattr(importlib.import_module("confluent_kafka"), "Consumer")
        consumer = Consumer(
            {
                "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
                "group.id": f"ops-replay-diagnosis-{group_suffix}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([self.topic_diagnosed])
        return consumer

    def _write_evaluation_report(self, report: dict) -> Path:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.evaluations_dir / f"{stamp}-{report['job_id']}.json"
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return path

    def _infer_scenario(self, records) -> str | None:
        counts = ArchiveReader.extract_scenario_distribution(records)
        if not counts:
            return None
        return max(counts.items(), key=lambda x: x[1])[0]

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value)

    @staticmethod
    def _load_scenario_roots(path: str | Path = "config/scenarios.yaml") -> dict[str, str]:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        out: dict[str, str] = {}
        for scenario_name, spec in data.get("scenarios", {}).items():
            root = spec.get("root_fault", {}).get("service")
            if isinstance(root, str):
                out[scenario_name] = root
        return out


def main() -> None:
    configure_logging()
    worker = ReplayWorker()
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)
    worker.run()


if __name__ == "__main__":
    main()
