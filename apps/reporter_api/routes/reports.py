from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.reporter_api.dependencies import get_incident_service
from apps.reporter_api.services.incident_service import IncidentService

router = APIRouter(prefix="/incidents", tags=["reports"])


@router.get("/{incident_id}/report")
def get_incident_report(
    incident_id: str,
    incident_service: IncidentService = Depends(get_incident_service),
) -> dict:
    report = incident_service.get_incident_report(incident_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"Incident not found: {incident_id}")
    return report
