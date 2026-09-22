"""
app/api/v1/endpoints.py
~~~~~~~~~~~~~~~~~~~~~~~
CRUD routes for WebhookEndpoint registration.
"""
from typing import Sequence

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.endpoint import WebhookEndpoint
from app.schemas.endpoint import WebhookEndpointCreate, WebhookEndpointRead

router = APIRouter(prefix="/endpoints", tags=["endpoints"])


@router.post(
    "",
    response_model=WebhookEndpointRead,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new webhook endpoint",
)
async def create_endpoint(
    body: WebhookEndpointCreate,
    db: AsyncSession = Depends(get_db),
) -> WebhookEndpoint:
    """Register a URL to receive webhook deliveries."""
    endpoint = WebhookEndpoint(
        target_url=str(body.target_url),
        secret=body.secret,
        is_active=True,
    )
    db.add(endpoint)
    await db.flush()
    return endpoint


@router.get(
    "",
    response_model=list[WebhookEndpointRead],
    summary="List active webhook endpoints",
)
async def list_endpoints(
    db: AsyncSession = Depends(get_db),
) -> Sequence[WebhookEndpoint]:
    """Return all currently active registered endpoints."""
    result = await db.execute(
        select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
    )
    return result.scalars().all()
