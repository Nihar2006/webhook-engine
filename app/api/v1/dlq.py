"""
app/api/v1/dlq.py
~~~~~~~~~~~~~~~~~
Dead-Letter Queue inspection and manual replay API — Phase 6.

Routes
------
GET  /api/v1/dlq
    Paginated list of all events in ``DEAD_LETTER`` status, enriched with
    total delivery attempts and the last error reason.

POST /api/v1/events/{event_id}/replay
    Reset a ``DEAD_LETTER`` or ``FAILED`` event to ``PENDING`` and
    re-enqueue it for delivery.  Returns HTTP 202 Accepted.

Design notes
------------
* The replay endpoint accepts both ``DEAD_LETTER`` (retries exhausted) and
  ``FAILED`` (4xx non-retryable) so operators can recover from endpoint
  misconfiguration too (e.g. wrong auth key fixed → replay all FAILED).
* Only ``PENDING`` and ``DELIVERED`` events are blocked from replay:
  PENDING is already in-flight; DELIVERED already succeeded.
* The DLQ query joins ``delivery_attempt`` with a subquery to compute
  total_attempts and the most recent error per event efficiently.
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.delivery import DeliveryAttempt
from app.models.event import Event, EventStatus
from app.schemas.event import DLQEventItem, DLQListResponse, ReplayAccepted
from app.tasks.delivery import deliver_webhook_task

router = APIRouter(tags=["dlq"])

# Statuses that are eligible for replay.
_REPLAYABLE: frozenset[EventStatus] = frozenset(
    {EventStatus.DEAD_LETTER, EventStatus.FAILED}
)


# ---------------------------------------------------------------------------
# GET /api/v1/dlq — Paginated Dead-Letter Queue listing
# ---------------------------------------------------------------------------

@router.get(
    "/dlq",
    response_model=DLQListResponse,
    summary="List dead-lettered events (Phase 6)",
)
async def list_dlq(
    page: int = Query(default=1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(default=20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Return a paginated list of all events currently in ``DEAD_LETTER`` status.

    Each item includes:

    * ``total_attempts`` — total ``DeliveryAttempt`` rows for the event.
    * ``last_http_status`` — HTTP status from the most recent delivery attempt.
    * ``last_error`` — response body from the most recent delivery attempt
      (truncated to 500 chars).

    **Pagination**: zero-indexed offset ``(page - 1) * page_size``.
    """
    offset = (page - 1) * page_size

    # ------------------------------------------------------------------
    # 1. Count total DEAD_LETTER events (for pagination metadata).
    # ------------------------------------------------------------------
    total_result = await db.execute(
        select(func.count()).select_from(Event).where(
            Event.status == EventStatus.DEAD_LETTER
        )
    )
    total: int = total_result.scalar_one()

    # ------------------------------------------------------------------
    # 2. Fetch the page of DEAD_LETTER events, ordered newest-first.
    # ------------------------------------------------------------------
    events_result = await db.execute(
        select(Event)
        .where(Event.status == EventStatus.DEAD_LETTER)
        .order_by(Event.created_at.desc())
        .offset(offset)
        .limit(page_size)
    )
    events: list[Event] = list(events_result.scalars().all())

    # ------------------------------------------------------------------
    # 3. For each event, fetch attempt summary (total count + last entry).
    #    We do this in a single subquery per event to keep it readable;
    #    for high-volume DLQs a single JOIN with window functions is better.
    # ------------------------------------------------------------------
    items: list[DLQEventItem] = []
    for event in events:
        # Total attempt count
        count_result = await db.execute(
            select(func.count()).select_from(DeliveryAttempt).where(
                DeliveryAttempt.event_id == event.id
            )
        )
        total_attempts: int = count_result.scalar_one()

        # Last attempt (highest attempt_number)
        last_attempt_result = await db.execute(
            select(DeliveryAttempt)
            .where(DeliveryAttempt.event_id == event.id)
            .order_by(DeliveryAttempt.attempt_number.desc())
            .limit(1)
        )
        last_attempt: DeliveryAttempt | None = last_attempt_result.scalar_one_or_none()

        items.append(DLQEventItem(
            id               = event.id,
            event_type       = event.event_type,
            idempotency_key  = event.idempotency_key,
            status           = event.status,
            created_at       = event.created_at,
            total_attempts   = total_attempts,
            last_http_status = last_attempt.http_status if last_attempt else None,
            last_error       = (
                (last_attempt.response_body or "")[:500]
                if last_attempt else None
            ),
        ))

    return DLQListResponse(
        total     = total,
        page      = page,
        page_size = page_size,
        items     = items,
    )


# ---------------------------------------------------------------------------
# POST /api/v1/events/{event_id}/replay — Manual replay
# ---------------------------------------------------------------------------

@router.post(
    "/events/{event_id}/replay",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ReplayAccepted,
    summary="Manually replay a failed or dead-lettered event (Phase 6)",
)
async def replay_event(
    event_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Reset a ``DEAD_LETTER`` or ``FAILED`` event to ``PENDING`` and
    re-enqueue it for delivery.

    **Eligibility**

    | Current status | Replay allowed? | Reason |
    |---|---|---|
    | ``DEAD_LETTER`` | ✅ Yes | Retries exhausted — operator fix possible |
    | ``FAILED`` | ✅ Yes | 4xx failure — endpoint config may have been fixed |
    | ``PENDING`` | ❌ No (422) | Already in-flight |
    | ``DELIVERED`` | ❌ No (422) | Already succeeded — use idempotency_key to repost |

    **Flow**

    1. ``SELECT`` the event (404 if not found).
    2. Validate eligibility (422 if not replayable).
    3. ``UPDATE event.status = PENDING``, ``COMMIT``.
    4. ``deliver_webhook_task.delay(str(event_id))`` (non-blocking).
    5. Return ``202 Accepted``.

    The worker will fan out to all currently-active endpoints, giving it a
    fresh chance against endpoints that have since recovered or been fixed.
    """
    # ------------------------------------------------------------------
    # 1. Look up the event.
    # ------------------------------------------------------------------
    event: Event | None = await db.get(Event, event_id)
    if event is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event {event_id} not found.",
        )

    # ------------------------------------------------------------------
    # 2. Gate: only DEAD_LETTER and FAILED are replayable.
    # ------------------------------------------------------------------
    if event.status not in _REPLAYABLE:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Event {event_id} has status '{event.status.value}' and cannot be replayed. "
                f"Only {[s.value for s in _REPLAYABLE]} events are eligible."
            ),
        )

    # ------------------------------------------------------------------
    # 3. Reset to PENDING and commit — must be visible before .delay().
    # ------------------------------------------------------------------
    previous_status = event.status.value
    event.status = EventStatus.PENDING
    await db.commit()

    # ------------------------------------------------------------------
    # 4. Enqueue delivery task (non-blocking Redis push).
    # ------------------------------------------------------------------
    deliver_webhook_task.delay(str(event_id))

    import logging
    logging.getLogger(__name__).info(
        "[replay] event=%s replayed from %s → PENDING, task enqueued.",
        event_id, previous_status,
    )

    # ------------------------------------------------------------------
    # 5. Return 202 Accepted.
    # ------------------------------------------------------------------
    return ReplayAccepted(
        event_id=event_id,
        status="REPLAY_QUEUED",
        message=(
            f"Event {event_id} (was {previous_status}) reset to PENDING "
            "and queued for redelivery."
        ),
    )
