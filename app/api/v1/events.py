"""
app/api/v1/events.py
~~~~~~~~~~~~~~~~~~~~
Event dispatch route — Phase 5: true idempotent ingestion.

Phase 5 change summary
----------------------
* The route is now **truly idempotent** on ``idempotency_key``:

  - **First POST** (key unseen): ``INSERT … ON CONFLICT DO NOTHING`` succeeds
    → HTTP **202 Accepted**, Celery task enqueued.

  - **Subsequent POST** (key already exists): the existing ``Event`` row is
    fetched and returned as HTTP **200 OK** with the header::

        X-Idempotent-Replay: true

    No Celery task is enqueued on replay — the delivery already happened (or
    is in progress) from the first call.

  - **Concurrent race** (two requests arrive simultaneously with the same key):
    ``ON CONFLICT DO NOTHING`` ensures exactly one INSERT lands.  The losing
    request receives 0 rows inserted, falls through to the SELECT fallback, and
    returns HTTP 200 replay — never re-enqueuing a second task.

Phase 4 behaviour (raw 409 Conflict) is superseded.  The 409 path is gone;
callers that sent the same key expecting a 409 should now handle 200 gracefully.

Phase 3 change summary
----------------------
* The route returns **HTTP 202 Accepted** immediately after persisting the
  event and enqueuing the Celery delivery task.
* Actual HTTP fan-out, DeliveryAttempt recording, and Event.status updates
  happen inside ``deliver_webhook_task`` running in a Celery worker process.
* End-to-end API latency drops from ~3 s (Phase 2) to < 20 ms (Phase 3).

Phase 2 behaviour is preserved in git history and documented in
docs/PHASE_2_SYNC_BASELINE.md.
"""
from typing import Any

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.event import Event, EventStatus
from app.schemas.event import EventAccepted, EventCreate
from app.tasks.delivery import deliver_webhook_task

router = APIRouter(prefix="/events", tags=["events"])


@router.post(
    "",
    summary="Dispatch an event — idempotent ingestion (Phase 5)",
)
async def dispatch_event(
    body: EventCreate,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Persist an event and enqueue it for background delivery — idempotent.

    **Idempotency semantics (Phase 5)**

    The ``idempotency_key`` field is enforced as UNIQUE in Postgres.
    Two callers sending the same key will never produce two deliveries:

    1. **SELECT** — check whether the key already exists (fast indexed lookup).
    2. If **found** → return **HTTP 200** with the original event payload and
       the header ``X-Idempotent-Replay: true``.  No Celery task is enqueued.
    3. If **not found** → ``INSERT … ON CONFLICT DO NOTHING`` (race-safe).
    4. If the INSERT produced a row (``rowcount == 1``) → commit, enqueue
       ``deliver_webhook_task``, return **HTTP 202 Accepted**.
    5. If the INSERT was a no-op (concurrent duplicate won the race) → SELECT
       fallback → return **HTTP 200 replay**.

    **Flow diagram**::

        POST /api/v1/events
          ├─ SELECT event WHERE idempotency_key = ?
          │    ├─ EXISTS  → 200 + X-Idempotent-Replay: true  (no re-enqueue)
          │    └─ NOT EXISTS → INSERT ON CONFLICT DO NOTHING
          │         ├─ inserted → commit → enqueue task → 202 Accepted
          │         └─ no-op   → SELECT fallback → 200 + X-Idempotent-Replay: true

    **Celery worker (unchanged from Phase 3/4)**

    * Queries all active ``WebhookEndpoint`` rows.
    * POSTs the payload to each endpoint (5 s timeout).
    * Records a ``DeliveryAttempt`` row per endpoint.
    * Updates ``Event.status`` → ``DELIVERED`` or ``FAILED``.
    """
    # ------------------------------------------------------------------
    # 1. Fast-path: check whether this key was already processed.
    #
    #    An indexed SELECT is cheaper than a failed INSERT + rollback and
    #    avoids unnecessary transaction noise on the hot path (the vast
    #    majority of calls will be first-time keys in production).
    # ------------------------------------------------------------------
    existing_result = await db.execute(
        select(Event).where(Event.idempotency_key == body.idempotency_key)
    )
    existing_event: Event | None = existing_result.scalar_one_or_none()

    if existing_event is not None:
        return _replay_response(existing_event)

    # ------------------------------------------------------------------
    # 2. Race-safe INSERT using ON CONFLICT DO NOTHING.
    #
    #    If two concurrent requests slip through the SELECT above with the
    #    same key, exactly one INSERT will succeed; the other will be
    #    silently discarded by Postgres.  We detect the no-op via
    #    ``result.inserted_primary_key`` being None (psycopg2 / asyncpg
    #    return None for suppressed inserts).
    # ------------------------------------------------------------------
    new_id = uuid.uuid4()
    stmt = (
        pg_insert(Event)
        .values(
            id=new_id,
            event_type=body.event_type,
            payload=body.payload,
            status=EventStatus.PENDING,
            idempotency_key=body.idempotency_key,
        )
        .on_conflict_do_nothing(index_elements=["idempotency_key"])
        .returning(Event.id)
    )
    result = await db.execute(stmt)
    inserted_id = result.scalar_one_or_none()

    if inserted_id is None:
        # Another request won the race — fetch the winning row and replay.
        await db.rollback()
        fallback_result = await db.execute(
            select(Event).where(Event.idempotency_key == body.idempotency_key)
        )
        winner: Event = fallback_result.scalar_one()
        return _replay_response(winner)

    # ------------------------------------------------------------------
    # 3. Commit BEFORE enqueuing — critical for correctness.
    #
    #    The Celery worker uses a separate psycopg2 connection.  If we call
    #    .delay() before committing, the worker can query Postgres before the
    #    transaction is visible and find no row, silently skipping delivery.
    #    Committing first makes the row durable and visible. (~1 ms overhead)
    # ------------------------------------------------------------------
    await db.commit()

    # ------------------------------------------------------------------
    # 4. Enqueue the Celery delivery task (non-blocking Redis push).
    # ------------------------------------------------------------------
    deliver_webhook_task.delay(str(inserted_id))

    # ------------------------------------------------------------------
    # 5. Return 202 Accepted — delivery is in progress in the background.
    # ------------------------------------------------------------------
    return JSONResponse(
        status_code=202,
        content=EventAccepted(
            event_id=inserted_id,
            status="ACCEPTED",
            message="Event queued for background delivery",
        ).model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _replay_response(event: Event) -> JSONResponse:
    """
    Build a 200 OK replay response for an already-processed idempotency key.

    Includes the header ``X-Idempotent-Replay: true`` so callers can
    distinguish a genuine 200 from a deduplicated replay without parsing
    the body.
    """
    return JSONResponse(
        status_code=200,
        content={
            "event_id": str(event.id),
            "event_type": event.event_type,
            "payload": event.payload,
            "status": event.status.value,
            "idempotency_key": event.idempotency_key,
            "created_at": (
                event.created_at.isoformat()
                if event.created_at is not None
                else None
            ),
            "message": "Duplicate idempotency_key — returning existing event (no re-delivery).",
        },
        headers={"X-Idempotent-Replay": "true"},
    )
