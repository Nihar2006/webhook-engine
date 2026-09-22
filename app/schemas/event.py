import uuid
from datetime import datetime

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
