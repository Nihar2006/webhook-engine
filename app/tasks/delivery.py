"""
app/tasks/delivery.py
~~~~~~~~~~~~~~~~~~~~~
Celery task: deliver a webhook event to all active registered endpoints.

Why a *synchronous* SQLAlchemy engine here?
-------------------------------------------
Celery workers run tasks inside a plain OS thread pool (or the solo pool on
Windows), **not** inside an asyncio event loop.  The application's primary
database module (``app.core.database``) uses ``asyncpg`` + ``AsyncSession``,
which *requires* a running event loop.  Calling ``asyncio.run()`` inside a
Celery task is technically possible but creates a new event loop per task
invocation — expensive and error-prone.

The idiomatic solution is to maintain a separate **synchronous** engine whose
URL is derived from ``settings.DATABASE_URL`` by replacing the async driver
specifier (``+asyncpg``) with the sync one (``+psycopg2``).  Both engines
point at the same Postgres database; they just use different DBAPI adapters.

Session lifecycle
-----------------
Each task invocation opens its own ``Session``, does all its work in a single
transaction, commits (or rolls back on error), and closes.  There is no shared
state between tasks.
"""
from __future__ import annotations

import logging
import time
import uuid

import httpx
from celery import shared_task
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.delivery import DeliveryAttempt
from app.models.endpoint import WebhookEndpoint
from app.models.event import Event, EventStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Synchronous DB engine — used exclusively by Celery tasks.
# Derived from the async URL by swapping the driver specifier.
# e.g. "postgresql+asyncpg://..." → "postgresql+psycopg2://..."
# ---------------------------------------------------------------------------
_SYNC_DB_URL: str = settings.DATABASE_URL.replace(
    "postgresql+asyncpg://",
    "postgresql+psycopg2://",
).replace(
    # Fallback: handle URLs that start with just "postgresql+asyncpg" without "://"
    "+asyncpg",
    "+psycopg2",
)

_sync_engine = create_engine(
    _SYNC_DB_URL,
    pool_pre_ping=True,   # Validate connections before use
    pool_size=5,          # Reasonable pool for a Celery worker
    max_overflow=10,
)

# HTTP timeout for outbound delivery calls (seconds).
_HTTP_TIMEOUT_S: float = 5.0


@shared_task(
    name="app.tasks.delivery.deliver_webhook_task",
    bind=True,
    max_retries=3,
    default_retry_delay=30,
)
def deliver_webhook_task(self, event_id: str) -> dict:  # type: ignore[override]
    """
    Celery task: deliver *event_id* to every active WebhookEndpoint.

    Session lifecycle design
    ------------------------
    We open TWO short-lived DB sessions instead of one long-lived one:

    Phase A  (read, fast):  fetch Event + WebhookEndpoint rows, close session.
    Phase B  (HTTP, slow):  fire all HTTP POSTs with the session CLOSED.
                            Holding a DB connection open during slow HTTP calls
                            causes Windows TCP connection aborts (10053) when
                            the idle connection is reaped by the OS or Postgres.
    Phase C  (write, fast): open a fresh session, insert DeliveryAttempts,
                            update Event.status, commit, close session.

    Parameters
    ----------
    event_id:
        String representation of the ``Event.id`` UUID.
    """
    event_uuid = uuid.UUID(event_id)
    logger.info("[deliver_webhook_task] Starting delivery for event_id=%s", event_id)

    # ------------------------------------------------------------------
    # Phase A: Read — fetch event + endpoints, then CLOSE the session.
    # ------------------------------------------------------------------
    with Session(_sync_engine) as session:
        event: Event | None = session.get(Event, event_uuid)
        if event is None:
            logger.error(
                "[deliver_webhook_task] Event %s not found — skipping.", event_id
            )
            return {"event_id": event_id, "error": "Event not found"}

        active_endpoints: list[WebhookEndpoint] = session.execute(
            select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
        ).scalars().all()

        if not active_endpoints:
            event.status = EventStatus.DELIVERED
            session.commit()
            return {"event_id": event_id, "delivered": 0, "failed": 0}

        # Snapshot the data we need — session will close after this block.
        event_payload = dict(event.payload)
        endpoint_data = [
            {"id": ep.id, "target_url": str(ep.target_url)}
            for ep in active_endpoints
        ]
    # Session is now CLOSED. No DB connection held during HTTP calls.

    # ------------------------------------------------------------------
    # Phase B: HTTP delivery — no DB connection open.
    # ------------------------------------------------------------------
    AttemptResult = dict  # typing alias for clarity
    results: list[AttemptResult] = []
    succeeded = 0
    failed = 0

    with httpx.Client(timeout=_HTTP_TIMEOUT_S) as http_client:
        for attempt_number, ep in enumerate(endpoint_data, start=1):
            http_status: int | None = None
            response_body: str | None = None

            t0 = time.perf_counter()
            try:
                resp = http_client.post(ep["target_url"], json=event_payload)
                http_status = resp.status_code
                response_body = resp.text[:4096]
                if resp.is_success:
                    succeeded += 1
                    logger.info(
                        "[deliver_webhook_task] event=%s endpoint=%s -> HTTP %s",
                        event_id, ep["id"], http_status,
                    )
                else:
                    failed += 1
                    logger.warning(
                        "[deliver_webhook_task] event=%s endpoint=%s -> HTTP %s (non-2xx)",
                        event_id, ep["id"], http_status,
                    )
            except httpx.TimeoutException:
                response_body = "TIMEOUT"
                failed += 1
                logger.warning(
                    "[deliver_webhook_task] event=%s endpoint=%s -> TIMEOUT",
                    event_id, ep["id"],
                )
            except httpx.RequestError as exc:
                response_body = f"REQUEST_ERROR: {exc}"
                failed += 1
                logger.error(
                    "[deliver_webhook_task] event=%s endpoint=%s -> %s",
                    event_id, ep["id"], exc,
                )
            finally:
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.debug(
                    "[deliver_webhook_task] endpoint=%s elapsed=%.1f ms",
                    ep["id"], elapsed_ms,
                )

            results.append({
                "endpoint_id": ep["id"],
                "http_status": http_status,
                "response_body": response_body,
                "attempt_number": attempt_number,
            })

    # ------------------------------------------------------------------
    # Phase C: Write — open a fresh session just for inserts + update.
    # ------------------------------------------------------------------
    final_status = EventStatus.DELIVERED if succeeded > 0 else EventStatus.FAILED

    try:
        with Session(_sync_engine) as session:
            for r in results:
                session.add(DeliveryAttempt(
                    event_id=event_uuid,
                    endpoint_id=r["endpoint_id"],
                    http_status=r["http_status"],
                    response_body=r["response_body"],
                    attempt_number=r["attempt_number"],
                ))
            # Re-fetch event to update status in this fresh session
            evt = session.get(Event, event_uuid)
            if evt is not None:
                evt.status = final_status
            session.commit()
    except Exception as exc:
        logger.exception(
            "[deliver_webhook_task] DB write failed for event %s: %s", event_id, exc
        )
        raise

    summary = {
        "event_id": event_id,
        "delivered": succeeded,
        "failed": failed,
        "status": final_status.value,
    }
    logger.info("[deliver_webhook_task] Completed: %s", summary)
    return summary
