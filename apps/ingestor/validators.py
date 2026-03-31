from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class RawEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str = Field(min_length=8)
    signal_type: str = Field(min_length=3)
    event_time: datetime
    service_name: str = Field(min_length=1)
    instance_id: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    region: str = Field(min_length=1)
    severity: str = Field(min_length=1)
    correlation_keys: dict[str, str] = Field(default_factory=dict)
    payload: dict


def validate_raw_event(raw_payload: dict) -> RawEvent:
    return RawEvent.model_validate(raw_payload)
