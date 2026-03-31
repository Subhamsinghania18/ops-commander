from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field
from sqlalchemy import select

from libs.common.kafka import build_producer, ensure_topics, serialize_json
from libs.storage.db import ReplayJob, session_scope
from libs.storage.repositories.evidence import EvidenceRepository
from libs.storage.repositories.incidents import IncidentRepository
from libs.storage.repositories.rankings import RankingRepository

logger = logging.getLogger(__name__)


class ReplayJobRequest(BaseModel):
    incident_id: str | None = None
    scenario_name: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    speed_factor: float = Field(default=1.0, ge=0.1, le=50.0)
    note: str | None = Field(default=None, max_length=512)


class IncidentService:
    def __init__(self) -> None:
        self.incidents = IncidentRepository()
        self.rankings = RankingRepository()
        self.evidence = EvidenceRepository()
        self.reports_topic = os.getenv("TOPIC_INCIDENT_REPORTS", "incidents.reports")
        self._producer = None
        try:
            ensure_topics([self.reports_topic])
            self._producer = build_producer("reporter-api")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Report topic producer unavailable: %s", exc)

    def list_incidents(
        self,
        page: int,
        page_size: int,
        service: str | None,
        severity: str | None,
        status: str | None,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> dict:
        incidents, total = self.incidents.query_incidents(
            page=page,
            page_size=page_size,
            service=service,
            severity=severity,
            status=status,
            start_time=start_time,
            end_time=end_time,
        )

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "items": [
                {
                    "incident_id": i.incident_id,
                    "candidate_id": i.candidate_id,
                    "status": i.status,
                    "severity": i.severity,
                    "window_start": i.window_start.isoformat(),
                    "window_end": i.window_end.isoformat(),
                    "affected_services": i.affected_services,
                    "top_service": i.top_service,
                    "top_score": i.top_score,
                    "confidence": i.confidence,
                    "updated_at": i.updated_at.isoformat(),
                }
                for i in incidents
            ],
        }

    def get_incident(self, incident_id: str) -> dict | None:
        incident = self.incidents.get_incident(incident_id)
        if incident is None:
            return None

        rankings = self.rankings.list_rankings(incident_id)
        edges = self.evidence.list_edges(incident_id)

        return {
            "incident_id": incident.incident_id,
            "candidate_id": incident.candidate_id,
            "status": incident.status,
            "severity": incident.severity,
            "window_start": incident.window_start.isoformat(),
            "window_end": incident.window_end.isoformat(),
            "affected_services": incident.affected_services,
            "top_service": incident.top_service,
            "top_score": incident.top_score,
            "confidence": incident.confidence,
            "created_at": incident.created_at.isoformat(),
            "updated_at": incident.updated_at.isoformat(),
            "rankings": [
                {
                    "rank": r.rank,
                    "service": r.service_name,
                    "score": r.score,
                    "confidence": r.confidence,
                    "component_scores": r.component_scores,
                    "explanation": r.explanation,
                }
                for r in rankings
            ],
            "evidence_edges": [
                {
                    "source_service": e.source_service,
                    "target_service": e.target_service,
                    "source_cluster": e.source_cluster,
                    "target_cluster": e.target_cluster,
                    "reason": e.reason,
                    "weight": e.weight,
                    "metadata": e.edge_metadata,
                }
                for e in edges
            ],
        }

    def get_incident_report(self, incident_id: str) -> dict | None:
        detailed = self.get_incident(incident_id)
        if detailed is None:
            return None

        rankings = detailed["rankings"]
        top = rankings[0] if rankings else None

        signal_coverage = sorted(
            {
                signal
                for item in rankings
                for signal in item.get("explanation", {}).get("evidence", {}).get("signal_types", [])
            }
        )

        edges = sorted(
            detailed["evidence_edges"],
            key=lambda e: (-float(e.get("weight", 0.0)), e.get("source_service", ""), e.get("target_service", "")),
        )

        propagation_chain = [
            {
                "from": edge["source_service"],
                "to": edge["target_service"],
                "path": f"{edge['source_service']} -> {edge['target_service']}",
                "reason": edge["reason"],
                "weight": edge["weight"],
                "lag_seconds": edge.get("metadata", {}).get("lag_seconds"),
            }
            for edge in edges
        ]

        report = {
            "report_id": f"report-{incident_id}",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "incident": {
                "incident_id": detailed["incident_id"],
                "status": detailed["status"],
                "severity": detailed["severity"],
                "window_start": detailed["window_start"],
                "window_end": detailed["window_end"],
                "affected_services": detailed["affected_services"],
            },
            "what_failed": {
                "primary_symptom_service": detailed["top_service"],
                "confidence": detailed["confidence"],
                "top_score": detailed["top_score"],
            },
            "why_it_likely_failed": {
                "root_cause": top["service"] if top else None,
                "evidence_summary": top["explanation"] if top else {},
                "competing_hypotheses": rankings[1:4],
            },
            "how_failure_propagated": propagation_chain,
            "diagnosis": {
                "rankings": rankings,
                "edge_count": len(propagation_chain),
                "signal_coverage": signal_coverage,
                "hypothesis_count": len(rankings),
            },
        }

        if self._producer is not None:
            try:
                self._producer.produce(
                    topic=self.reports_topic,
                    key=incident_id,
                    value=serialize_json(report),
                )
                self._producer.poll(0)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to emit report to topic for incident %s: %s", incident_id, exc)

        return report

    def create_replay_job(self, request: ReplayJobRequest, requested_by: str = "api") -> dict:
        job_id = f"replay-{uuid4()}"
        payload = request.model_dump(mode="json")
        now = datetime.now(timezone.utc)

        with session_scope() as session:
            session.add(
                ReplayJob(
                    job_id=job_id,
                    status="queued",
                    requested_by=requested_by,
                    request_payload=payload,
                    created_at=now,
                    updated_at=now,
                )
            )

        return {
            "job_id": job_id,
            "status": "queued",
            "requested_by": requested_by,
            "request": payload,
            "created_at": now.isoformat(),
        }

    def list_replay_jobs(self, limit: int = 50) -> list[dict]:
        with session_scope() as session:
            stmt = select(ReplayJob).order_by(ReplayJob.created_at.desc()).limit(limit)
            jobs = list(session.execute(stmt).scalars().all())

        return [
            {
                "job_id": j.job_id,
                "status": j.status,
                "requested_by": j.requested_by,
                "request": j.request_payload,
                "created_at": j.created_at.isoformat(),
                "updated_at": j.updated_at.isoformat(),
            }
            for j in jobs
        ]
