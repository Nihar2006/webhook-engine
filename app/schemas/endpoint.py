import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, HttpUrl


class WebhookEndpointCreate(BaseModel):
    """Request body for registering a new webhook endpoint."""

    target_url: HttpUrl
    secret: str | None = None


class WebhookEndpointRead(BaseModel):
    """Response schema for a registered webhook endpoint."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    target_url: str
    secret: str | None
    is_active: bool
    created_at: datetime
