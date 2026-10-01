"""
app/tasks/delivery.py
~~~~~~~~~~~~~~~~~~~~~
Celery tasks: dispatch and deliver webhook events — Phase 4 / 5 / 6 / 8.

Architecture
------------
Phase 4 splits the monolithic ``deliver_webhook_task`` into two tasks:

1. **deliver_webhook_task(event_id)**  — *dispatcher*, no retries.
   Reads all active endpoints and enqueues one ``deliver_to_endpoint_task``
   per endpoint.  Returns immediately after fan-out.

2. **deliver_to_endpoint_task(event_id, endpoint_id)**  — *retryable worker*.
   Handles delivery to exactly ONE endpoint with full retry logic:

   =========  ============  ==============================================
   Response   Outcome       Action
   =========  ============  ==============================================
   THROTTLED  RATE LIMIT    Defer task via self.retry(countdown=window).
                            No DeliveryAttempt written; does NOT count
                            against the network-failure retry budget.
   2xx        SUCCESS       Record attempt, mark Event DELIVERED, done.
   4xx        CLIENT_ERROR  Record attempt, mark Event FAILED, no retry.
                            (Bad request / endpoint gone — retrying won't
                            help; the caller must fix the payload/config.)
   5xx / err  TRANSIENT     Record attempt, jitter-backoff retry up to
                            ``max_retries`` times.  If exhausted, mark
                            Event DEAD_LETTER (Phase 6) and emit a
                            structured [DLQ] log line.
   =========  ============  ==============================================

   attempt_number = self.request.retries + 1  (1-indexed, increments per retry)

Phase 8: Atomic per-endpoint rate limiting
------------------------------------------
Before every HTTP dispatch, the worker atomically increments a Redis counter
for the target endpoint using a Lua script (INCR + conditional EXPIRE).  If
the counter exceeds ``RATE_LIMIT_MAX`` within ``RATE_LIMIT_WINDOW_S`` seconds,
the task is deferred via ``self.retry(countdown=RATE_LIMIT_WINDOW_S,
max_retries=None)`` — an unlimited deferral that bypasses the 4-retry
network-failure budget entirely.  No ``DeliveryAttempt`` is written for
throttled deferrals, and the event never transitions to FAILED or DEAD_LETTER
due to rate limiting.

Why per-endpoint tasks?
-----------------------
With a single task that fans out to N endpoints, a 5xx from endpoint B
would force a retry that re-delivers to endpoint A (which already succeeded).
Splitting into per-endpoint tasks means each endpoint's retry lifecycle is
independent: a flaky endpoint never delays or duplicates delivery to healthy
endpoints.

Why a *synchronous* SQLAlchemy engine?
----------------------------------------
See Phase 3 notes: Celery workers run in OS threads, not an asyncio loop.
The synchronous ``psycopg2`` engine is the idiomatic solution.

Session lifecycle (three short sessions per attempt)
----------------------------------------------------
Phase A   read    : fetch event + endpoint data (incl. idempotency_key),
                    pre-generate delivery_attempt UUID, close session.
Phase A.5 RL gate : atomically check rate limit via Redis Lua — if
                    exceeded, defer with self.retry() and exit early.
Phase B   HTTP    : fire the POST with no DB connection held.
                    Four structured headers are injected (Phase 5):
                        X-Webhook-Event-Id, X-Webhook-Delivery-Id,
                        X-Webhook-Idempotency-Key, X-Webhook-Timestamp.
Phase C   write   : open fresh session, insert DeliveryAttempt (using the
                    pre-generated UUID from Phase A), update Event.status
                    if this is a terminal attempt, commit.

Phase 5 header semantics
------------------------
``X-Webhook-Event-Id`` and ``X-Webhook-Idempotency-Key`` are **stable**
across all retry attempts for the same event — the destination server can
use either as a deduplication key.  ``X-Webhook-Delivery-Id`` is freshly
generated per attempt, letting subscribers distinguish individual deliveries
in their own audit log.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone

import httpx
import redis as redis_lib
from celery import shared_task
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.rate_limiter import RateLimitExceeded, RateLimiter
from app.core.retry import calculate_full_jitter_backoff
from app.core.security import generate_webhook_signature
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
    # Fallback: handle URLs that omit the "://" after +asyncpg
    "+asyncpg",
    "+psycopg2",
)

_sync_engine = create_engine(
    _SYNC_DB_URL,
    pool_pre_ping=True,   # Validate connections before use
    pool_size=5,
    max_overflow=10,
)

# HTTP timeout for outbound delivery calls (seconds).
_HTTP_TIMEOUT_S: float = 5.0

# ---------------------------------------------------------------------------
# Phase 8: Per-endpoint rate limiting constants
# ---------------------------------------------------------------------------
# Maximum number of HTTP delivery requests allowed per endpoint per window.
# Configurable here; a future phase can promote these to per-endpoint DB fields.
RATE_LIMIT_MAX: int = 10

# Duration of the fixed rate-limit window in seconds.
RATE_LIMIT_WINDOW_S: int = 1

# ---------------------------------------------------------------------------
# Phase 8: Module-level Redis client + RateLimiter singleton
# ---------------------------------------------------------------------------
# We use a dedicated redis-py client (not the Celery broker connection) so that
# rate-limit counters are independent of task queue traffic.  The singleton is
# created lazily on first use to avoid connection setup at import time.
_redis_client: redis_lib.Redis | None = None  # type: ignore[type-arg]
_rate_limiter: RateLimiter | None = None


def _get_rate_limiter() -> RateLimiter:
    """Return the module-level :class:`RateLimiter` singleton.

    Creates the Redis client and registers the Lua script on first call.
    Thread-safe for Celery's solo/prefork pool because each forked worker
    process owns its own module globals.
    """
    global _redis_client, _rate_limiter
    if _rate_limiter is None:
        _redis_client = redis_lib.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=False,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        _rate_limiter = RateLimiter(_redis_client)
        logger.debug("[delivery] RateLimiter singleton initialised.")
    return _rate_limiter


# Outcome sentinels — avoid bare strings scattered across the code.
_SUCCESS        = "success"
_CLIENT_ERROR   = "client_error"   # 4xx — non-retryable
_TRANSIENT_ERROR = "transient_error"  # 5xx / timeout — retryable


# ---------------------------------------------------------------------------
# Task 1: Dispatcher — fans out to per-endpoint delivery tasks
# ---------------------------------------------------------------------------

@shared_task(
    name="app.tasks.delivery.deliver_webhook_task",
    bind=True,
    max_retries=0,          # dispatcher itself never retries
    ignore_result=False,
)
def deliver_webhook_task(self, event_id: str) -> dict:  # type: ignore[override]
    """
    Dispatcher task: read active endpoints and enqueue one
    ``deliver_to_endpoint_task`` per endpoint.

    This task is enqueued by the FastAPI route immediately after the Event
    is committed to Postgres.  It is deliberately lightweight — it does one
    DB read, fans out, and exits.  All retry logic lives in
    ``deliver_to_endpoint_task``.

    Parameters
    ----------
    event_id:
        String UUID of the ``Event`` to deliver.
    """
    event_uuid = uuid.UUID(event_id)
    logger.info("[deliver_webhook_task] Dispatching event_id=%s", event_id)

    # ------------------------------------------------------------------
    # Read event + active endpoints, then close session immediately.
    # ------------------------------------------------------------------
    with Session(_sync_engine) as session:
        event: Event | None = session.get(Event, event_uuid)
        if event is None:
            logger.error(
                "[deliver_webhook_task] Event %s not found — skipping.", event_id
            )
            return {"event_id": event_id, "error": "Event not found"}

        active_endpoints = session.execute(
            select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
        ).scalars().all()

        if not active_endpoints:
            # No registered endpoints → mark DELIVERED immediately (nothing to do).
            evt = session.get(Event, event_uuid)
            if evt is not None:
                evt.status = EventStatus.DELIVERED
            session.commit()
            logger.info(
                "[deliver_webhook_task] No active endpoints for event=%s — DELIVERED.", event_id
            )
            return {"event_id": event_id, "dispatched": 0}

        endpoint_ids: list[str] = [str(ep.id) for ep in active_endpoints]
    # DB session closed.

    # ------------------------------------------------------------------
    # Fan out: one Celery task per endpoint.
    # ------------------------------------------------------------------
    for endpoint_id in endpoint_ids:
        deliver_to_endpoint_task.delay(event_id, endpoint_id)
        logger.debug(
            "[deliver_webhook_task] Enqueued delivery event=%s endpoint=%s",
            event_id, endpoint_id,
        )

    logger.info(
        "[deliver_webhook_task] Dispatched %d delivery task(s) for event=%s",
        len(endpoint_ids), event_id,
    )
    return {"event_id": event_id, "dispatched": len(endpoint_ids)}


# ---------------------------------------------------------------------------
# Task 2: Per-endpoint delivery worker — retryable with Full-Jitter backoff
# ---------------------------------------------------------------------------

@shared_task(
    name="app.tasks.delivery.deliver_to_endpoint_task",
    bind=True,
    max_retries=4,          # 1 initial attempt + 4 retries = 5 total attempts
    default_retry_delay=1,  # overridden by calculate_full_jitter_backoff()
)
def deliver_to_endpoint_task(  # type: ignore[override]
    self,
    event_id: str,
    endpoint_id: str,
) -> dict:
    """
    Deliver ``event_id`` to a single ``endpoint_id`` with automatic retry.

    Retry policy
    ------------
    * **2xx** → success; record attempt, mark Event DELIVERED, return.
    * **4xx** → non-retryable client error; record attempt, mark Event
      FAILED, return.  Retrying a bad request / gone endpoint wastes
      resources and will never succeed.
    * **5xx / TimeoutException / RequestError** → transient; record attempt,
      schedule retry with Full-Jitter backoff countdown.  If
      ``max_retries`` exhausted, mark Event FAILED.

    attempt_number tracking
    -----------------------
    ``self.request.retries`` is 0 on the first call, 1 on the first retry,
    etc.  We store ``attempt_number = self.request.retries + 1`` (1-indexed)
    so the audit trail in ``delivery_attempt`` is human-readable.

    Parameters
    ----------
    event_id:
        String UUID of the ``Event``.
    endpoint_id:
        String UUID of the ``WebhookEndpoint`` to deliver to.
    """
    event_uuid    = uuid.UUID(event_id)
    endpoint_uuid = uuid.UUID(endpoint_id)

    # attempt_number is 1-indexed for the audit trail.
    attempt_number: int = self.request.retries + 1

    logger.info(
        "[deliver_to_endpoint_task] event=%s endpoint=%s attempt=%d/%d",
        event_id, endpoint_id, attempt_number, self.max_retries + 1,
    )

    # ------------------------------------------------------------------
    # Phase A: Read — fetch event payload + endpoint URL, close session.
    # ------------------------------------------------------------------
    with Session(_sync_engine) as session:
        event: Event | None = session.get(Event, event_uuid)
        if event is None:
            logger.error(
                "[deliver_to_endpoint_task] Event %s not found — skipping.", event_id
            )
            return {"event_id": event_id, "error": "Event not found"}

        endpoint: WebhookEndpoint | None = session.get(WebhookEndpoint, endpoint_uuid)
        if endpoint is None:
            logger.error(
                "[deliver_to_endpoint_task] Endpoint %s not found — skipping.", endpoint_id
            )
            return {"endpoint_id": endpoint_id, "error": "Endpoint not found"}

        event_payload: dict       = dict(event.payload)
        target_url: str            = str(endpoint.target_url)
        event_idempotency_key: str = str(event.idempotency_key)
        event_id_str: str          = str(event.id)
        endpoint_secret: str | None = endpoint.secret   # None = unsigned endpoint
    # Session closed -- no DB connection held during HTTP call.

    # ------------------------------------------------------------------
    # Phase A.5: Rate-limit gate — atomic Redis Lua check.
    #
    # This runs AFTER the DB session is closed (no connection held) and
    # BEFORE the HTTP call (no wasted network round-trip on throttled tasks).
    #
    # If throttled:
    #   • Log [THROTTLED] at WARNING level.
    #   • Re-enqueue via self.retry(countdown=RATE_LIMIT_WINDOW_S).
    #   • max_retries=None: throttle deferrals never exhaust the retry budget.
    #   • No DeliveryAttempt is written (this is a deferral, not a real attempt).
    #   • The event stays PENDING — it will be retried after the window expires.
    # ------------------------------------------------------------------
    limiter = _get_rate_limiter()
    is_limited, retry_after = limiter.is_rate_limited(
        endpoint_id,
        limit=RATE_LIMIT_MAX,
        window_seconds=RATE_LIMIT_WINDOW_S,
    )
    if is_limited:
        logger.warning(
            "[THROTTLED] endpoint=%s exceeded rate limit (%d req/%ds) — "
            "deferring task for %ds (attempt=%d)",
            endpoint_id, RATE_LIMIT_MAX, RATE_LIMIT_WINDOW_S,
            retry_after, attempt_number,
        )
        # Raise self.retry with max_retries=None so Celery treats this as an
        # unconditional deferral — it will NOT count against self.max_retries
        # (the network-failure budget) and will NOT mark the task as failed.
        raise self.retry(
            exc=RateLimitExceeded(
                f"endpoint {endpoint_id} exceeded rate limit "
                f"({RATE_LIMIT_MAX} req/{RATE_LIMIT_WINDOW_S}s)"
            ),
            countdown=retry_after,
            max_retries=None,
        )

    # Pre-generate the DeliveryAttempt UUID so we can embed it in the
    # request headers *and* use the same value in the Phase C DB write.
    delivery_attempt_id: uuid.UUID = uuid.uuid4()
    dispatch_timestamp: str = datetime.now(tz=timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Serialise payload exactly once -- signed bytes must equal sent bytes.
    #
    # Using httpx's json= kwarg lets httpx re-serialise the dict internally;
    # the resulting bytes might differ from what we signed (key ordering,
    # spacing). Instead we serialise once with deterministic settings
    # (sort_keys=True, compact separators) and POST raw content=.
    # ------------------------------------------------------------------
    unix_ts: int = int(time.time())
    payload_str: str = json.dumps(event_payload, separators=(",", ":"), sort_keys=True)

    # Build X-Webhook-Signature only when the endpoint has a secret.
    sig_header: str | None = None
    if endpoint_secret:
        sig_header = generate_webhook_signature(endpoint_secret, unix_ts, payload_str)
        logger.debug(
            "[deliver_to_endpoint_task] Signing event=%s with HMAC-SHA256 (ts=%d)",
            event_id, unix_ts,
        )

    # ------------------------------------------------------------------
    # Phase B: HTTP delivery — classify outcome.
    # ------------------------------------------------------------------
    http_status:   int | None = None
    response_body: str | None = None
    outcome:       str        = _TRANSIENT_ERROR   # assume worst case
    exc_for_retry: Exception | None = None

    t0 = time.perf_counter()
    try:
        # Structured idempotency + tracing headers injected on every attempt.
        # The destination server can use X-Webhook-Event-Id and
        # X-Webhook-Idempotency-Key (which are stable across retries) to
        # deduplicate on its side.  X-Webhook-Delivery-Id changes per attempt.
        delivery_headers = {
            "X-Webhook-Event-Id":        event_id_str,
            "X-Webhook-Delivery-Id":     str(delivery_attempt_id),
            "X-Webhook-Idempotency-Key": event_idempotency_key,
            "X-Webhook-Timestamp":       dispatch_timestamp,
        }
        if sig_header:
            delivery_headers["X-Webhook-Signature"] = sig_header

        with httpx.Client(timeout=_HTTP_TIMEOUT_S) as http_client:
            resp = http_client.post(
                target_url,
                content=payload_str.encode("utf-8"),
                headers={"Content-Type": "application/json", **delivery_headers},
            )

        http_status   = resp.status_code
        response_body = resp.text[:4096]

        if resp.is_success:               # 2xx
            outcome = _SUCCESS
            logger.info(
                "[deliver_to_endpoint_task] event=%s endpoint=%s attempt=%d -> HTTP %s SUCCESS",
                event_id, endpoint_id, attempt_number, http_status,
            )

        elif 400 <= http_status < 500:    # 4xx — non-retryable
            outcome = _CLIENT_ERROR
            logger.warning(
                "[deliver_to_endpoint_task] event=%s endpoint=%s attempt=%d -> HTTP %s "
                "CLIENT ERROR (non-retryable — bad request or endpoint gone)",
                event_id, endpoint_id, attempt_number, http_status,
            )

        else:                             # 5xx — retryable
            outcome = _TRANSIENT_ERROR
            exc_for_retry = Exception(f"HTTP {http_status} from {target_url}")
            logger.warning(
                "[deliver_to_endpoint_task] event=%s endpoint=%s attempt=%d -> HTTP %s "
                "TRANSIENT ERROR (will retry)",
                event_id, endpoint_id, attempt_number, http_status,
            )

    except httpx.TimeoutException as exc:
        response_body = "TIMEOUT"
        outcome       = _TRANSIENT_ERROR
        exc_for_retry = exc
        logger.warning(
            "[deliver_to_endpoint_task] event=%s endpoint=%s attempt=%d -> TIMEOUT",
            event_id, endpoint_id, attempt_number,
        )

    except httpx.RequestError as exc:
        response_body = f"REQUEST_ERROR: {exc}"
        outcome       = _TRANSIENT_ERROR
        exc_for_retry = exc
        logger.error(
            "[deliver_to_endpoint_task] event=%s endpoint=%s attempt=%d -> %s",
            event_id, endpoint_id, attempt_number, exc,
        )

    finally:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.debug(
            "[deliver_to_endpoint_task] endpoint=%s attempt=%d elapsed=%.1f ms",
            endpoint_id, attempt_number, elapsed_ms,
        )

    # ------------------------------------------------------------------
    # Phase C: Write — record DeliveryAttempt, update Event.status if terminal.
    #
    # We write the DeliveryAttempt row BEFORE calling self.retry() because
    # retry() raises celery.exceptions.Retry (a subclass of Exception) which
    # unwinds the call stack — code after raise is never reached.
    # ------------------------------------------------------------------

    # Is this attempt the last one? (success, client error, or retries exhausted)
    retries_exhausted: bool = (
        outcome == _TRANSIENT_ERROR and self.request.retries >= self.max_retries
    )
    is_terminal: bool = outcome in (_SUCCESS, _CLIENT_ERROR) or retries_exhausted

    final_event_status: EventStatus | None = None
    if is_terminal:
        if outcome == _SUCCESS:
            final_event_status = EventStatus.DELIVERED
        elif outcome == _CLIENT_ERROR:
            # 4xx: non-retryable, bad payload or endpoint config.
            final_event_status = EventStatus.FAILED
        else:
            # retries_exhausted on transient error — move to Dead-Letter Queue.
            final_event_status = EventStatus.DEAD_LETTER

    try:
        with Session(_sync_engine) as session:
            session.add(DeliveryAttempt(
                id            = delivery_attempt_id,
                event_id      = event_uuid,
                endpoint_id   = endpoint_uuid,
                http_status   = http_status,
                response_body = response_body,
                attempt_number = attempt_number,
            ))

            if final_event_status is not None:
                evt = session.get(Event, event_uuid)
                if evt is not None:
                    evt.status = final_event_status
                    logger.info(
                        "[deliver_to_endpoint_task] event=%s -> status=%s (attempt=%d)",
                        event_id, final_event_status.value, attempt_number,
                    )

            session.commit()
    except Exception as db_exc:
        logger.exception(
            "[deliver_to_endpoint_task] DB write failed event=%s attempt=%d: %s",
            event_id, attempt_number, db_exc,
        )
        raise

    # ------------------------------------------------------------------
    # Retry if transient and not yet exhausted.
    # ------------------------------------------------------------------
    if outcome == _TRANSIENT_ERROR and not retries_exhausted:
        # countdown = Full-Jitter backoff for the *current* retry count.
        # self.request.retries is still the CURRENT attempt index (0-based),
        # so passing it here gives the backoff for the upcoming (retries+1)-th attempt.
        countdown = calculate_full_jitter_backoff(attempt=self.request.retries)
        logger.info(
            "[deliver_to_endpoint_task] Scheduling retry %d/%d for event=%s in %.2f s",
            self.request.retries + 1, self.max_retries, event_id, countdown,
        )
        raise self.retry(
            exc=exc_for_retry or Exception("Transient error"),
            countdown=countdown,
        )

    if retries_exhausted:
        logger.error(
            "[DLQ] event=%s endpoint=%s moved to DEAD_LETTER after %d attempt(s). "
            "Last HTTP status: %s. Use POST /api/v1/events/%s/replay to redeliver.",
            event_id, endpoint_id, attempt_number, http_status, event_id,
        )

    return {
        "event_id":      event_id,
        "endpoint_id":   endpoint_id,
        "attempt_number": attempt_number,
        "outcome":       outcome,
        "http_status":   http_status,
    }
