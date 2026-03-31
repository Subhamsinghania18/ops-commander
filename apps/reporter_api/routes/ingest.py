from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from apps.reporter_api.services.ingest_service import IngestService

router = APIRouter(prefix="/ingest", tags=["ingest"])


def get_ingest_service() -> IngestService:
    return IngestService()


@router.post("/alerts/alertmanager")
def ingest_alertmanager(
    payload: dict[str, Any],
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict:
    return ingest_service.ingest_alertmanager_payload(payload)
