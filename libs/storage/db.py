from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from collections.abc import Iterator

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


def _db_url() -> str:
    user = os.getenv("DB_USER", "ops")
    password = os.getenv("DB_PASSWORD", "ops")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "15432")
    name = os.getenv("DB_NAME", "ops_commander")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


class Base(DeclarativeBase):
    pass


class QuarantineEvent(Base):
    __tablename__ = "quarantine_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    source_topic: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(String(512), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)


class ProcessedIdempotencyKey(Base):
    __tablename__ = "processed_idempotency_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )


class IngestedEvent(Base):
    __tablename__ = "ingested_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    service_name: Mapped[str] = mapped_column(String(128), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingest_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    late: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    normalized_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)


class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    candidate_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="triaging")
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    affected_services: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    top_service: Mapped[str] = mapped_column(String(128), nullable=False)
    severity: Mapped[str] = mapped_column(String(24), nullable=False, default="medium")
    top_score: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    diagnosis_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )


class RankingSnapshot(Base):
    __tablename__ = "rankings"
    __table_args__ = (
        UniqueConstraint("incident_id", "rank", name="uq_rankings_incident_rank"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(String(64), ForeignKey("incidents.incident_id"), index=True)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    service_name: Mapped[str] = mapped_column(String(128), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    component_scores: Mapped[dict] = mapped_column(JSONB, nullable=False)
    explanation: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )


class EvidenceEdge(Base):
    __tablename__ = "evidence_edges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(String(64), ForeignKey("incidents.incident_id"), index=True)
    source_service: Mapped[str] = mapped_column(String(128), nullable=False)
    target_service: Mapped[str] = mapped_column(String(128), nullable=False)
    source_cluster: Mapped[str] = mapped_column(String(64), nullable=False)
    target_cluster: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str] = mapped_column(String(256), nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    edge_metadata: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )


class ReplayJob(Base):
    __tablename__ = "replay_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="queued")
    requested_by: Mapped[str] = mapped_column(String(128), nullable=False, default="api")
    request_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )


engine = create_engine(_db_url(), pool_pre_ping=True, future=True, echo=os.getenv("DB_ECHO", "false").lower() == "true")
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
