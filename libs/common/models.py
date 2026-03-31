from __future__ import annotations

from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventType(str, Enum):
    LOG = "log"
    METRIC = "metric"
    ALERT = "alert"


class Severity(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    CRITICAL = "critical"


class QualityFlags(BaseModel):
    model_config = ConfigDict(extra="forbid")

    late: bool = False
    duplicate: bool = False
    missing_fields: bool = False
    clock_skew_suspected: bool = False
    out_of_order: bool = False


class LogEventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(min_length=1)
    logger: str = Field(default="app")
    exception: str | None = None
    code: str | None = None


class MetricEventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric_name: str = Field(min_length=1)
    value: float
    unit: str = Field(default="count")
    tags: dict[str, str] = Field(default_factory=dict)


class AlertEventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alert_name: str = Field(min_length=1)
    status: Literal["firing", "resolved"] = "firing"
    threshold: float | None = None
    observed: float | None = None
    summary: str = Field(min_length=1)


PayloadType = LogEventPayload | MetricEventPayload | AlertEventPayload


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=8)
    event_type: EventType
    event_time: datetime
    ingest_time: datetime
    service_name: str = Field(min_length=1)
    instance_id: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    region: str = Field(min_length=1)
    severity: Severity
    correlation_keys: dict[str, str] = Field(default_factory=dict)
    payload: dict[str, Any]
    quality_flags: QualityFlags = Field(default_factory=QualityFlags)
    schema_version: str = Field(default="v1")

    @field_validator("ingest_time")
    @classmethod
    def ingest_not_before_event(cls, v: datetime, info):
        event_time = info.data.get("event_time")
        if event_time and v.year < 2000:
            raise ValueError("ingest_time appears invalid")
        return v

    def idempotency_key(self) -> str:
        stable = (
            f"{self.event_type}|{self.event_time.isoformat()}|{self.service_name}|"
            f"{self.instance_id}|{self.severity}|{self.payload}"
        )
        return sha256(stable.encode("utf-8")).hexdigest()


def parse_payload(event_type: EventType, payload: dict[str, Any]) -> PayloadType:
    if event_type == EventType.LOG:
        return LogEventPayload.model_validate(payload)
    if event_type == EventType.METRIC:
        return MetricEventPayload.model_validate(payload)
    if event_type == EventType.ALERT:
        return AlertEventPayload.model_validate(payload)
    raise ValueError(f"Unsupported event type: {event_type}")
