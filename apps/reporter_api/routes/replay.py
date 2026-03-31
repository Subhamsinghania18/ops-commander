from __future__ import annotations

from fastapi import APIRouter, Depends, Header

from apps.reporter_api.dependencies import get_incident_service
from apps.reporter_api.services.incident_service import IncidentService, ReplayJobRequest

router = APIRouter(prefix="/replay", tags=["replay"])


@router.post("/jobs")
def create_replay_job(
    request: ReplayJobRequest,
    incident_service: IncidentService = Depends(get_incident_service),
    x_requested_by: str | None = Header(default=None),
) -> dict:
    requested_by = x_requested_by or "api"
    return incident_service.create_replay_job(request=request, requested_by=requested_by)


@router.get("/jobs")
def list_replay_jobs(
    limit: int = 50,
    incident_service: IncidentService = Depends(get_incident_service),
) -> dict:
    return {"items": incident_service.list_replay_jobs(limit=limit)}
