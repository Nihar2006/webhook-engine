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
