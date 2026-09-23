"""
app/main.py
~~~~~~~~~~~
FastAPI application entry point — Phase 3.

Routers mounted:
  /api/v1/endpoints  — register and list webhook endpoints
  /api/v1/events     — dispatch events (async delivery via Celery)
  /api/v1/mock       — slow mock receiver for benchmarking
"""
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

# IMPORTANT: celery_app must be imported before any task module is imported.
# @shared_task binds to the "current" Celery app at import time.  Importing
# celery_app first ensures our Redis-backed app is current before
# app.tasks.delivery (imported transitively by events.py) is loaded.
from app.core.celery_app import celery_app  # noqa: F401

from app.api.v1.endpoints import router as endpoints_router
from app.api.v1.events import router as events_router
from app.api.v1.mock import router as mock_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan handler.

    Phase 3: the Celery worker is a separate process — no startup/shutdown
    hooks are required in the FastAPI process itself.  celery_app is imported
    at module level above to ensure the Redis broker is configured before any
    task .delay() calls are made.
    """
    yield


app = FastAPI(
    title="Webhook Engine",
    description=(
        "A scalable webhook delivery system. "
        "Phase 3: asynchronous delivery via Celery + Redis — "
        "POST /api/v1/events returns 202 Accepted immediately; "
        "a Celery worker handles fan-out in the background."
    ),
    version="0.3.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(endpoints_router, prefix="/api/v1")
app.include_router(events_router, prefix="/api/v1")
app.include_router(mock_router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/healthz", tags=["ops"], summary="Health check")
async def healthz() -> dict:
    """Returns 200 OK when the application process is alive."""
    return {"status": "ok"}
