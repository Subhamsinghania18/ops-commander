from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.reporter_api.dependencies import get_incident_service
from apps.reporter_api.services.incident_service import IncidentService

router = APIRouter(prefix="/incidents", tags=["incidents"])


@router.get("")
def list_incidents(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    service: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    status: str | None = Query(default=None),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
    incident_service: IncidentService = Depends(get_incident_service),
) -> dict:
    return incident_service.list_incidents(
        page=page,
        page_size=page_size,
        service=service,
        severity=severity,
        status=status,
        start_time=start_time,
        end_time=end_time,
    )


@router.get("/{incident_id}")
def get_incident(
    incident_id: str,
    incident_service: IncidentService = Depends(get_incident_service),
) -> dict:
    item = incident_service.get_incident(incident_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Incident not found: {incident_id}")
    return item
