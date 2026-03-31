from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Select, and_, func, select

from libs.storage.db import Incident, IngestedEvent, session_scope


class IngestedEventsRepository:
    def insert_event(
        self,
        event_id: str,
        event_type: str,
        service_name: str,
        event_time,
        ingest_time,
        idempotency_key: str,
        late: bool,
        normalized_payload: dict,
    ) -> None:
        with session_scope() as session:
            session.add(
                IngestedEvent(
                    event_id=event_id,
                    event_type=event_type,
                    service_name=service_name,
                    event_time=event_time,
                    ingest_time=ingest_time,
                    idempotency_key=idempotency_key,
                    late=late,
                    normalized_payload=normalized_payload,
                )
            )

    def exists_by_idempotency_key(self, idempotency_key: str) -> bool:
        with session_scope() as session:
            stmt = select(IngestedEvent.id).where(IngestedEvent.idempotency_key == idempotency_key).limit(1)
            return session.execute(stmt).first() is not None


class IncidentRepository:
    def upsert_incident(
        self,
        incident_id: str,
        candidate_id: str,
        window_start,
        window_end,
        affected_services: list[str],
        top_service: str,
        severity: str,
        top_score: float,
        confidence: float,
        diagnosis_payload: dict,
        status: str = "triaging",
    ) -> None:
        with session_scope() as session:
            stmt = select(Incident).where(Incident.incident_id == incident_id).limit(1)
            existing = session.execute(stmt).scalar_one_or_none()
            now = datetime.now(timezone.utc)

            if existing is None:
                session.add(
                    Incident(
                        incident_id=incident_id,
                        candidate_id=candidate_id,
                        status=status,
                        window_start=window_start,
                        window_end=window_end,
                        affected_services=affected_services,
                        top_service=top_service,
                        severity=severity,
                        top_score=top_score,
                        confidence=confidence,
                        diagnosis_payload=diagnosis_payload,
                        created_at=now,
                        updated_at=now,
                    )
                )
                return

            existing.candidate_id = candidate_id
            existing.status = status
            existing.window_start = window_start
            existing.window_end = window_end
            existing.affected_services = affected_services
            existing.top_service = top_service
            existing.severity = severity
            existing.top_score = top_score
            existing.confidence = confidence
            existing.diagnosis_payload = diagnosis_payload
            existing.updated_at = now

    def list_recent_incidents(self, limit: int = 100) -> list[Incident]:
        with session_scope() as session:
            stmt = select(Incident).order_by(Incident.updated_at.desc()).limit(limit)
            return list(session.execute(stmt).scalars().all())

    def get_incident(self, incident_id: str) -> Incident | None:
        with session_scope() as session:
            stmt = select(Incident).where(Incident.incident_id == incident_id).limit(1)
            return session.execute(stmt).scalar_one_or_none()

    def query_incidents(
        self,
        page: int,
        page_size: int,
        service: str | None = None,
        severity: str | None = None,
        status: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> tuple[list[Incident], int]:
        offset = max(0, (page - 1) * page_size)

        predicates = []
        if service:
            predicates.append(Incident.affected_services.contains([service]))
        if severity:
            predicates.append(Incident.severity == severity)
        if status:
            predicates.append(Incident.status == status)
        if start_time:
            predicates.append(Incident.window_end >= start_time)
        if end_time:
            predicates.append(Incident.window_start <= end_time)

        where_clause = and_(*predicates) if predicates else None

        with session_scope() as session:
            base_stmt: Select = select(Incident)
            if where_clause is not None:
                base_stmt = base_stmt.where(where_clause)

            count_stmt = select(func.count()).select_from(base_stmt.subquery())
            total = int(session.execute(count_stmt).scalar_one())

            page_stmt = base_stmt.order_by(Incident.updated_at.desc()).offset(offset).limit(page_size)
            incidents = list(session.execute(page_stmt).scalars().all())

        return incidents, total
