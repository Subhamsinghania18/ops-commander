from __future__ import annotations

from apps.reporter_api.services.incident_service import IncidentService

_service: IncidentService | None = None


def get_incident_service() -> IncidentService:
    global _service
    if _service is None:
        _service = IncidentService()
    return _service
