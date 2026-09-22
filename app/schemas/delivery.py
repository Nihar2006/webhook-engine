import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class DeliveryAttemptRead(BaseModel):
    """Response schema for a delivery attempt record."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    http_status: int | None
    response_body: str | None
    attempt_number: int
    created_at: datetime
