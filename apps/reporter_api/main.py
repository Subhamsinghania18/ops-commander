from __future__ import annotations

import os
from time import perf_counter

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from apps.reporter_api.routes.incidents import router as incidents_router
from apps.reporter_api.routes.ingest import router as ingest_router
from apps.reporter_api.routes.replay import router as replay_router
from apps.reporter_api.routes.reports import router as reports_router
from libs.common.logging import configure_logging
from libs.common.observability import ServiceMetrics
from libs.storage.db import init_db
from libs.storage.repositories.replay_jobs import ReplayJobRepository


app = FastAPI(
    title="Ops Commander Reporter API",
    version="0.6.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.state.metrics = ServiceMetrics("reporter-api")
app.state.replay_jobs_repo = ReplayJobRepository()


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    started = perf_counter()
    try:
        response = await call_next(request)
        app.state.metrics.record_processed((perf_counter() - started) * 1000.0)
        return response
    except Exception as exc:  # noqa: BLE001
        app.state.metrics.record_error(f"{type(exc).__name__}: {exc}")
        return JSONResponse(status_code=500, content={"detail": "internal server error"})


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "reporter-api"}


@app.get("/ops/metrics")
def ops_metrics() -> dict:
    queue_counts = app.state.replay_jobs_repo.status_counts()
    return app.state.metrics.snapshot(extra={"replay_job_status_counts": queue_counts})


@app.get("/ops/readiness")
def readiness() -> dict:
    try:
        init_db()
        queue_counts = app.state.replay_jobs_repo.status_counts()
        return {
            "status": "ready",
            "service": "reporter-api",
            "checks": {
                "postgres": "ok",
                "replay_queue_visibility": "ok",
                "replay_job_status_counts": queue_counts,
            },
        }
    except Exception as exc:  # noqa: BLE001
        app.state.metrics.record_error(f"readiness:{type(exc).__name__}: {exc}")
        return {
            "status": "degraded",
            "service": "reporter-api",
            "checks": {
                "postgres": "error",
                "error": f"{type(exc).__name__}: {exc}",
            },
        }


@app.on_event("startup")
def startup() -> None:
    configure_logging()
    init_db()


app.include_router(incidents_router)
app.include_router(reports_router)
app.include_router(replay_router)
app.include_router(ingest_router)


def run() -> None:
    import uvicorn

    host = os.getenv("REPORTER_API_HOST", "0.0.0.0")
    port = int(os.getenv("REPORTER_API_PORT", "8000"))
    uvicorn.run("apps.reporter_api.main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run()
