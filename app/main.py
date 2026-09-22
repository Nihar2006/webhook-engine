"""
app/main.py
~~~~~~~~~~~
FastAPI application entry point — Phase 2.

Routers mounted:
  /api/v1/endpoints  — register and list webhook endpoints
  /api/v1/events     — dispatch events (synchronous delivery)
  /api/v1/mock       — slow mock receiver for benchmarking
"""
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from app.api.v1.endpoints import router as endpoints_router
from app.api.v1.events import router as events_router
from app.api.v1.mock import router as mock_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan handler.

    Phase 2: no startup/shutdown work needed beyond what FastAPI provides.
    Phase 3 will use this hook to initialise the Celery app and verify the
    Redis broker connection on startup.
    """
    yield


app = FastAPI(
    title="Webhook Engine",
    description=(
        "A scalable webhook delivery system. "
        "Phase 2: synchronous baseline demonstrating in-band delivery limits."
    ),
    version="0.2.0",
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
