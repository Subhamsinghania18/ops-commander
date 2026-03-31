from __future__ import annotations

from datetime import timedelta

from apps.ingestor.validators import RawEvent
from libs.common.models import EventEnvelope, EventType, QualityFlags, Severity, parse_payload
from libs.common.time import utc_now

LATE_EVENT_THRESHOLD_SECONDS = 20


def _event_type(signal_type: str) -> EventType:
    normalized = signal_type.lower().strip()
    if normalized not in {"log", "metric", "alert"}:
        raise ValueError(f"Unsupported signal_type: {signal_type}")
    return EventType(normalized)


def _severity(value: str) -> Severity:
    normalized = value.lower().strip()
    if normalized not in {"debug", "info", "warn", "error", "critical"}:
        return Severity.INFO
    return Severity(normalized)


def normalize_event(raw_event: RawEvent) -> EventEnvelope:
    event_type = _event_type(raw_event.signal_type)
    parsed_payload = parse_payload(event_type, raw_event.payload)
    ingest_time = utc_now()
    late = ingest_time - raw_event.event_time > timedelta(seconds=LATE_EVENT_THRESHOLD_SECONDS)
    out_of_order = raw_event.event_time > ingest_time + timedelta(seconds=5)

    return EventEnvelope(
        event_id=raw_event.event_id,
        event_type=event_type,
        event_time=raw_event.event_time,
        ingest_time=ingest_time,
        service_name=raw_event.service_name,
        instance_id=raw_event.instance_id,
        environment=raw_event.environment,
        region=raw_event.region,
        severity=_severity(raw_event.severity),
        correlation_keys=raw_event.correlation_keys,
        payload=parsed_payload.model_dump(),
        quality_flags=QualityFlags(late=late, out_of_order=out_of_order),
        schema_version="v1",
    )
