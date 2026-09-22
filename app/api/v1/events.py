"""
app/api/v1/events.py
~~~~~~~~~~~~~~~~~~~~
Event dispatch route — synchronous, in-band delivery.

Phase 2 deliberately keeps delivery synchronous (inside the HTTP request) so
the throughput ceiling can be measured and documented.  Phase 3 will move
delivery off the request path into a Celery task queue.
"""
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.delivery import DeliveryAttempt
from app.models.endpoint import WebhookEndpoint
from app.models.event import Event, EventStatus
from app.schemas.delivery import DeliveryAttemptRead
from app.schemas.event import EventCreate, EventRead

router = APIRouter(prefix="/events", tags=["events"])

# Timeout for outbound HTTP calls to registered endpoints.
_HTTP_TIMEOUT_S: float = 5.0


@router.post(
    "",
    status_code=status.HTTP_200_OK,
    summary="Dispatch an event — synchronous delivery",
)
async def dispatch_event(
    body: EventCreate,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Insert an event and deliver it synchronously to all active endpoints.

    **Flow**
    1. ``INSERT`` the Event with ``status=PENDING``.
       Duplicate ``idempotency_key`` → **409 Conflict**.
    2. ``SELECT`` all active ``WebhookEndpoint`` rows.
    3. Sequential loop — for each endpoint:
       a. POST the payload via ``httpx.AsyncClient`` (5 s timeout).
       b. Record latency, ``http_status``, and ``response_body``.
       c. ``INSERT`` a ``DeliveryAttempt`` row.
    4. Update ``Event.status`` → ``DELIVERED`` (all 2xx) or ``FAILED``.
    5. Commit — then return the summary.

    .. note::
       Because every HTTP call is ``await``-ed sequentially inside *this*
       request handler, the worker is blocked for
       ``n_endpoints × receiver_latency`` seconds.  With two endpoints each
       sleeping 1.5 s the handler takes ≥ 3 s per event.  This is the
       architectural pain Phase 3 solves.
    """
    # ------------------------------------------------------------------
    # 1. Persist the event
    # ------------------------------------------------------------------
    event = Event(
        event_type=body.event_type,
        payload=body.payload,
        status=EventStatus.PENDING,
        idempotency_key=body.idempotency_key,
    )
    db.add(event)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Event with idempotency_key={body.idempotency_key!r} already exists.",
        )

    # ------------------------------------------------------------------
    # 2. Fetch active endpoints
    # ------------------------------------------------------------------
    result = await db.execute(
        select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
    )
    active_endpoints = result.scalars().all()

    # ------------------------------------------------------------------
    # 3. Deliver sequentially — record each attempt
    # ------------------------------------------------------------------
    delivery_records: list[DeliveryAttempt] = []
    all_succeeded = True

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_S) as client:
        for attempt_number, endpoint in enumerate(active_endpoints, start=1):
            http_status: int | None = None
            response_body: str | None = None

            t0 = time.perf_counter()
            try:
                resp = await client.post(
                    str(endpoint.target_url),
                    json=body.payload,
                )
                http_status = resp.status_code
                response_body = resp.text[:4096]  # cap stored body size
                if not resp.is_success:
                    all_succeeded = False
            except httpx.TimeoutException:
                response_body = "TIMEOUT"
                all_succeeded = False
            except httpx.RequestError as exc:
                response_body = f"REQUEST_ERROR: {exc}"
                all_succeeded = False
            finally:
                elapsed_ms = (time.perf_counter() - t0) * 1000

            attempt = DeliveryAttempt(
                event_id=event.id,
                endpoint_id=endpoint.id,
                http_status=http_status,
                response_body=response_body,
                attempt_number=attempt_number,
            )
            db.add(attempt)
            delivery_records.append(attempt)

    # ------------------------------------------------------------------
    # 4. Update event status and commit
    # ------------------------------------------------------------------
    event.status = EventStatus.DELIVERED if all_succeeded else EventStatus.FAILED
    await db.flush()
    # get_db dependency commits on clean exit

    # ------------------------------------------------------------------
    # 5. Return summary (must serialise before session closes)
    # ------------------------------------------------------------------
    return {
        "event": EventRead.model_validate(event),
        "deliveries": [DeliveryAttemptRead.model_validate(d) for d in delivery_records],
    }
