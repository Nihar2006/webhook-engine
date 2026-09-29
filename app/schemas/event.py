import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from app.models.event import EventStatus


class EventCreate(BaseModel):
    """Request body for dispatching a new event."""

    event_type: str
    payload: dict
    idempotency_key: str


class EventRead(BaseModel):
    """Response schema for a persisted event."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_type: str
    payload: dict
    status: EventStatus
    idempotency_key: str
    created_at: datetime


class EventAccepted(BaseModel):
    """
    Phase 3 response schema for POST /api/v1/events.

    Returned immediately (HTTP 202) after the event is persisted and the
    Celery delivery task is enqueued.  Delivery happens asynchronously in
    the background — callers should not expect delivery to be complete when
    they receive this response.
    """

    event_id: uuid.UUID
    status: Literal["ACCEPTED"]
    message: str


# ---------------------------------------------------------------------------
# Phase 6 — DLQ & Replay schemas
# ---------------------------------------------------------------------------

class DLQEventItem(BaseModel):
    """One item in the paginated DLQ list — enriched with attempt metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_type: str
    idempotency_key: str
    status: EventStatus
    created_at: datetime
    total_attempts: int
    last_http_status: int | None
    last_error: str | None


class DLQListResponse(BaseModel):
    """Paginated response from GET /api/v1/dlq."""

    total: int
    page: int
    page_size: int
    items: list[DLQEventItem]


class ReplayAccepted(BaseModel):
    """HTTP 202 response from POST /api/v1/events/{event_id}/replay."""

    event_id: uuid.UUID
    status: Literal["REPLAY_QUEUED"]
    message: str
