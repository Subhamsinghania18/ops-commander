from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select

from libs.storage.db import ReplayJob, session_scope


class ReplayJobRepository:
    def get_queued_jobs(self, limit: int = 10) -> list[ReplayJob]:
        with session_scope() as session:
            stmt = (
                select(ReplayJob)
                .where(ReplayJob.status == "queued")
                .order_by(ReplayJob.created_at.asc())
                .limit(limit)
            )
            return list(session.execute(stmt).scalars().all())

    def update_status(self, job_id: str, status: str) -> ReplayJob | None:
        with session_scope() as session:
            stmt = select(ReplayJob).where(ReplayJob.job_id == job_id).limit(1)
            job = session.execute(stmt).scalar_one_or_none()
            if job is None:
                return None
            job.status = status
            job.updated_at = datetime.now(timezone.utc)
            return job

    def update_payload_and_status(self, job_id: str, payload: dict, status: str) -> ReplayJob | None:
        with session_scope() as session:
            stmt = select(ReplayJob).where(ReplayJob.job_id == job_id).limit(1)
            job = session.execute(stmt).scalar_one_or_none()
            if job is None:
                return None
            job.request_payload = payload
            job.status = status
            job.updated_at = datetime.now(timezone.utc)
            return job

    def status_counts(self) -> dict[str, int]:
        with session_scope() as session:
            stmt = select(ReplayJob.status, func.count()).group_by(ReplayJob.status)
            rows = session.execute(stmt).all()
        out: dict[str, int] = {}
        for status, count in rows:
            out[str(status)] = int(count)
        return out
