from __future__ import annotations

import logging
import os
import random
import signal
import time
from datetime import datetime
from hashlib import sha1

from apps.causal_engine.causal_graph import CausalGraphBuilder
from apps.causal_engine.dependency_graph import ServiceDependencyGraph
from apps.causal_engine.explainer import DiagnosisExplainer
from apps.causal_engine.ranker import RootCauseRanker, ScoringWeights
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
from libs.storage.repositories.evidence import EvidenceRepository
from libs.storage.repositories.incidents import IncidentRepository
from libs.storage.repositories.rankings import RankingRepository

logger = logging.getLogger("causal-engine")


class CausalEngine:
    def __init__(self) -> None:
        self.topic_candidates = os.getenv("TOPIC_INCIDENT_CANDIDATES", "incidents.candidates")
        self.topic_diagnosed = os.getenv("TOPIC_INCIDENT_DIAGNOSED", "incidents.diagnosed")
        self.group_id = os.getenv("KAFKA_GROUP_CAUSAL_ENGINE", "ops-causal-engine-v1")
        self.metrics_log_interval_seconds = int(os.getenv("METRICS_LOG_INTERVAL_SECONDS", "30"))
        self.chaos_drop_rate = float(os.getenv("CHAOS_CAUSAL_ENGINE_DROP_RATE", "0.0"))

        ensure_topics([self.topic_candidates, self.topic_diagnosed])
        init_db()

        dependency_graph = ServiceDependencyGraph.from_config()
        self.graph_builder = CausalGraphBuilder(dependency_graph)
        self.ranker = RootCauseRanker(dependency_graph, ScoringWeights.from_config())
        self.explainer = DiagnosisExplainer()

        self.incidents_repo = IncidentRepository()
        self.rankings_repo = RankingRepository()
        self.evidence_repo = EvidenceRepository()

        self.consumer = build_consumer(
            group_id=self.group_id,
            topics=[self.topic_candidates],
            client_suffix="causal-engine",
        )
        self.producer = build_producer("causal-engine")
        self.metrics = ServiceMetrics("causal-engine")
        self.running = True

    def stop(self, *_args) -> None:
        self.running = False

    def _incident_id(self, candidate: dict) -> str:
        stable = (
            f"{candidate['candidate_id']}|{candidate['window_start']}|"
            f"{candidate['window_end']}|{','.join(sorted(candidate['affected_services']))}"
        )
        return f"inc-{sha1(stable.encode('utf-8')).hexdigest()[:16]}"

    def _to_edges_for_storage(self, incident_id: str, diagnosis: dict) -> list[dict]:
        edges = diagnosis.get("causal_edges", [])
        result: list[dict] = []
        for edge in edges:
            result.append(
                {
                    "incident_id": incident_id,
                    "source_service": edge["source_service"],
                    "target_service": edge["target_service"],
                    "source_cluster": edge["source_cluster"],
                    "target_cluster": edge["target_cluster"],
                    "reason": edge.get("reason", "unknown"),
                    "weight": float(edge.get("weight", 1.0)),
                    "metadata": edge.get("metadata", {}),
                }
            )
        return result

    @staticmethod
    def _derive_incident_severity(diagnosis: dict, affected_service_count: int) -> str:
        rankings = diagnosis.get("rankings", [])
        if not rankings:
            return "low"

        top = rankings[0]
        score = float(top.get("score", 0.0))
        confidence = float(top.get("confidence", 0.0))
        critical = bool(top.get("explanation", {}).get("evidence", {}).get("critical_alert", False))
        edge_count = len(diagnosis.get("causal_edges", []))
        widespread = affected_service_count >= 3 or edge_count >= 2

        if (critical and widespread) or (score >= 0.80 and confidence >= 0.75 and widespread):
            return "critical"
        if score >= 0.65 and confidence >= 0.55 and (affected_service_count >= 2 or edge_count >= 1):
            return "high"
        if score >= 0.45:
            return "medium"
        return "low"

    def _process_candidate(self, candidate: dict) -> dict:
        incident_id = self._incident_id(candidate)
        graph = self.graph_builder.build(candidate, incident_id=incident_id)
        rankings = self.ranker.rank(graph)
        diagnosis = self.explainer.build_diagnosis(graph, rankings)

        top_service = diagnosis["summary"]["root_cause"] or "unknown"
        top_score = rankings[0]["score"] if rankings else 0.0
        top_confidence = diagnosis["summary"].get("confidence", 0.0)
        severity = self._derive_incident_severity(
            diagnosis=diagnosis,
            affected_service_count=len(candidate.get("affected_services", [])),
        )

        self.incidents_repo.upsert_incident(
            incident_id=incident_id,
            candidate_id=candidate["candidate_id"],
            window_start=datetime.fromisoformat(candidate["window_start"]),
            window_end=datetime.fromisoformat(candidate["window_end"]),
            affected_services=candidate["affected_services"],
            top_service=top_service,
            severity=severity,
            top_score=top_score,
            confidence=float(top_confidence),
            diagnosis_payload=diagnosis,
            status="triaging",
        )
        self.rankings_repo.replace_rankings(incident_id=incident_id, rankings=diagnosis["rankings"])
        self.evidence_repo.replace_edges(
            incident_id=incident_id,
            edges=self._to_edges_for_storage(incident_id=incident_id, diagnosis=diagnosis),
        )

        outgoing = {
            "incident_id": incident_id,
            "candidate_id": candidate["candidate_id"],
            "generated_at": diagnosis["generated_at"],
            "summary": diagnosis["summary"],
            "affected_services": diagnosis["affected_services"],
            "rankings": diagnosis["rankings"],
            "competing_hypotheses": diagnosis["competing_hypotheses"],
            "causal_edges": diagnosis["causal_edges"],
        }

        produce_json_with_retry(
            producer=self.producer,
            topic=self.topic_diagnosed,
            key=incident_id,
            value=outgoing,
            on_retry=lambda _attempt, _exc: self.metrics.record_retry(),
        )

        return outgoing

    def run(self) -> None:
        logger.info("Causal engine started group_id=%s", self.group_id)
        next_metrics_log = time.time() + self.metrics_log_interval_seconds
        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is None:
                if time.time() >= next_metrics_log:
                    logger.info(
                        "Causal engine metrics=%s",
                        self.metrics.snapshot(consumer_lag_snapshot(self.consumer)),
                    )
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
                    diagnosed = self._process_candidate(payload)
                    self.metrics.record_processed(watch.elapsed_ms())
                    logger.info(
                        "Diagnosed incident=%s root=%s confidence=%.3f",
                        diagnosed["incident_id"],
                        diagnosed["summary"].get("root_cause"),
                        diagnosed["summary"].get("confidence", 0.0),
                    )
            except Exception as exc:  # noqa: BLE001
                self.metrics.record_error(f"{type(exc).__name__}: {exc}")
                logger.exception("Causal engine failure: %s", exc)
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
                logger.info(
                    "Causal engine metrics=%s",
                    self.metrics.snapshot(consumer_lag_snapshot(self.consumer)),
                )
                next_metrics_log = time.time() + self.metrics_log_interval_seconds

        self.producer.flush(10)
        self.consumer.close()
        logger.info("Causal engine stopped")


def main() -> None:
    configure_logging()
    engine = CausalEngine()
    signal.signal(signal.SIGINT, engine.stop)
    signal.signal(signal.SIGTERM, engine.stop)
    engine.run()


if __name__ == "__main__":
    main()
