"""
scripts/verify_phase3.py
~~~~~~~~~~~~~~~~~~~~~~~~
Phase 3 end-to-end verification script.

Tests:
  1. POST /api/v1/events returns HTTP 202 Accepted in < 20 ms.
  2. Response body contains event_id and status == "ACCEPTED".
  3. After Celery processes the task, a DeliveryAttempt row exists in
     Postgres for the event.
  4. The Event.status is updated to DELIVERED.

Prerequisites (all must be running):
  - Postgres          (postgresql://webhook:webhook@localhost:5432/webhookdb)
  - Redis             (redis://localhost:6379/0)
  - FastAPI server    (uvicorn app.main:app --port 8000)
  - Celery worker     (celery -A app.core.celery_app worker ...)
  - At least one active WebhookEndpoint row in the DB, OR the mock
    receiver is registered (the task marks DELIVERED even with 0
    endpoints -- see delivery.py Phase A early-return path).

Run from the project root:
    python scripts/verify_phase3.py
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid

import httpx
from sqlalchemy import select

# Make sure the project root is on sys.path when running as a script.
sys.path.insert(0, ".")

from app.core.database import AsyncSessionLocal          # noqa: E402
from app.models.delivery import DeliveryAttempt         # noqa: E402
from app.models.event import Event, EventStatus          # noqa: E402
from app.models.endpoint import WebhookEndpoint          # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE_URL: str = "http://127.0.0.1:8000"
EVENTS_ENDPOINT: str = f"{API_BASE_URL}/api/v1/events"

# Maximum acceptable round-trip latency for the 202 response (milliseconds).
MAX_LATENCY_MS: float = 20.0

# How long to wait (seconds) for Celery to process the task before giving up.
# The mock receiver sleeps 1.5 s per call; allow generous headroom.
CELERY_TIMEOUT_S: float = 30.0

# How often to poll Postgres while waiting for Celery (seconds).
POLL_INTERVAL_S: float = 0.5

# ---------------------------------------------------------------------------
# ANSI colours for terminal output
# ---------------------------------------------------------------------------
GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
RESET = "\033[0m"

SEP = "=" * 62


def ok(msg: str) -> None:
    print(f"  {GREEN}[OK]{RESET}   {msg}")


def fail(msg: str) -> None:
    print(f"  {RED}[FAIL]{RESET} {msg}")


def info(msg: str) -> None:
    print(f"  {CYAN}[INFO]{RESET} {msg}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def ensure_active_endpoint(session) -> WebhookEndpoint:
    """
    Ensure EXACTLY ONE active WebhookEndpoint exists — the local mock receiver.

    We deactivate every other active endpoint so that the Celery fan-out
    only hits a single target.  This prevents stale endpoints from previous
    runs from inflating delivery time (each mock call sleeps 1.5 s) and
    causing the CELERY_TIMEOUT_S poll to expire before the task finishes.
    """
    mock_url = f"{API_BASE_URL}/api/v1/mock/receiver"

    # Fetch ALL currently active endpoints.
    all_active = (
        await session.execute(
            select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
        )
    ).scalars().all()

    keep: WebhookEndpoint | None = None
    deactivated = 0

    for ep in all_active:
        if str(ep.target_url) == mock_url and keep is None:
            keep = ep  # keep the first correct one
        else:
            ep.is_active = False
            deactivated += 1

    if deactivated:
        info(f"Deactivated {deactivated} stale endpoint(s) to isolate the test.")
        await session.commit()

    if keep:
        info(f"Using mock receiver endpoint: {keep.target_url}  id={keep.id}")
        return keep

    # None existed with the right URL — create one.
    info(f"Registering mock receiver at {mock_url}")
    ep = WebhookEndpoint(target_url=mock_url, is_active=True)
    session.add(ep)
    await session.flush()
    await session.commit()
    info(f"Registered new endpoint id={ep.id}")
    return ep


async def poll_for_delivery(event_id: uuid.UUID) -> tuple[bool, str]:
    """
    Poll Postgres until a DeliveryAttempt exists for *event_id* AND
    Event.status != PENDING, or until CELERY_TIMEOUT_S elapses.

    Returns (success: bool, reason: str).
    """
    deadline = time.monotonic() + CELERY_TIMEOUT_S
    while time.monotonic() < deadline:
        async with AsyncSessionLocal() as session:
            attempt = (
                await session.execute(
                    select(DeliveryAttempt).where(
                        DeliveryAttempt.event_id == event_id
                    ).limit(1)
                )
            ).scalar_one_or_none()

            event = await session.get(Event, event_id)

        if event is None:
            return False, "Event row disappeared from DB"

        if attempt is not None and event.status != EventStatus.PENDING:
            return True, event.status.value

        await asyncio.sleep(POLL_INTERVAL_S)

    return False, f"Timed out after {CELERY_TIMEOUT_S}s -- Celery may not be running"


# ---------------------------------------------------------------------------
# Main verification routine
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"\n{SEP}")
    print("  Webhook Engine -- Phase 3 End-to-End Verification")
    print(SEP)

    all_passed = True
    idempotency_key = f"verify-phase3-{uuid.uuid4()}"

    # ------------------------------------------------------------------
    # Step 0: Ensure at least one active WebhookEndpoint exists so the
    # Celery task records a DeliveryAttempt (rather than short-circuiting).
    # ------------------------------------------------------------------
    print("\n[Step 0] Ensuring an active WebhookEndpoint exists ...")
    async with AsyncSessionLocal() as session:
        await ensure_active_endpoint(session)

    # ------------------------------------------------------------------
    # Step 1: POST /api/v1/events and measure latency.
    # ------------------------------------------------------------------
    print("\n[Step 1] POST /api/v1/events ...")
    event_payload = {
        "event_type": "order.created",
        "payload": {"order_id": str(uuid.uuid4()), "source": "verify_phase3"},
        "idempotency_key": idempotency_key,
    }

    # Reuse one AsyncClient for warmup + timed request.
    # Opening a new client per call pays a fresh TCP handshake (~150 ms)
    # that is NOT representative of server latency — that is OS overhead.
    async with httpx.AsyncClient(timeout=5.0) as client:

        # Warm up: establish TCP connection and exercise the server's
        # asyncpg pool so the timed request hits steady-state paths only.
        try:
            await client.get(f"{API_BASE_URL}/healthz")
        except httpx.ConnectError:
            fail(
                f"Could not connect to {API_BASE_URL}/healthz. "
                "Is the FastAPI server running?  "
                "(uvicorn app.main:app --port 8000)"
            )
            print(f"\n{SEP}")
            print(f"  {RED}FAIL{RESET}")
            print(SEP)
            sys.exit(1)

        # Timed request — connection is already open from warmup.
        t_start = time.perf_counter()
        response = await client.post(EVENTS_ENDPOINT, json=event_payload)
        elapsed_ms = (time.perf_counter() - t_start) * 1000

    # Check HTTP status code.
    if response.status_code == 202:
        ok(f"HTTP status: {response.status_code} Accepted")
    else:
        fail(f"Expected 202, got {response.status_code}: {response.text[:200]}")
        all_passed = False

    # Check latency.
    latency_label = f"{elapsed_ms:.2f} ms"
    if elapsed_ms < MAX_LATENCY_MS:
        ok(f"Latency: {latency_label}  (< {MAX_LATENCY_MS} ms threshold)")
    else:
        fail(
            f"Latency: {latency_label}  exceeds {MAX_LATENCY_MS} ms threshold -- "
            "Phase 3 async delivery should be faster than this."
        )
        all_passed = False

    # Parse response body.
    try:
        body = response.json()
    except Exception:
        fail(f"Response body is not valid JSON: {response.text[:200]}")
        print(f"\n{SEP}")
        print(f"  {RED}FAIL{RESET}")
        print(SEP)
        sys.exit(1)

    info(f"Response body: {body}")

    if body.get("status") == "ACCEPTED":
        ok("Response status field == 'ACCEPTED'")
    else:
        fail(f"Expected status='ACCEPTED', got {body.get('status')!r}")
        all_passed = False

    event_id_str: str | None = body.get("event_id")
    if event_id_str:
        ok(f"event_id present: {event_id_str}")
    else:
        fail("Response missing 'event_id' field")
        all_passed = False

    try:
        event_id = uuid.UUID(str(event_id_str))
    except (ValueError, TypeError):
        fail(f"'event_id' is not a valid UUID: {event_id_str!r}")
        print(f"\n{SEP}")
        print(f"  {RED}FAIL{RESET}")
        print(SEP)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 2: Wait for Celery to process the task.
    # ------------------------------------------------------------------
    print(
        f"\n[Step 2] Waiting up to {CELERY_TIMEOUT_S}s for Celery to process "
        f"event {event_id} ..."
    )
    success, reason = await poll_for_delivery(event_id)

    if success:
        ok(f"Celery processed the task -- Event.status = {reason}")
    else:
        fail(f"Celery did not deliver in time: {reason}")
        all_passed = False

    # ------------------------------------------------------------------
    # Step 3: Confirm DeliveryAttempt row in Postgres.
    # ------------------------------------------------------------------
    print("\n[Step 3] Confirming DeliveryAttempt row in Postgres ...")
    async with AsyncSessionLocal() as session:
        attempts = (
            await session.execute(
                select(DeliveryAttempt).where(
                    DeliveryAttempt.event_id == event_id
                )
            )
        ).scalars().all()

        final_event = await session.get(Event, event_id)

    if attempts:
        ok(f"Found {len(attempts)} DeliveryAttempt row(s) for event_id={event_id}")
        for i, a in enumerate(attempts, 1):
            info(
                f"  Attempt #{i}: endpoint_id={a.endpoint_id} "
                f"http_status={a.http_status} "
                f"attempt_number={a.attempt_number}"
            )
    else:
        fail("No DeliveryAttempt rows found in Postgres -- Celery may not be running")
        all_passed = False

    # Check final event status.
    if final_event is not None and final_event.status == EventStatus.DELIVERED:
        ok("Event.status = DELIVERED")
    elif final_event is not None:
        fail(
            f"Event.status = {final_event.status.value} "
            "(expected DELIVERED -- check if all endpoints returned 2xx)"
        )
        all_passed = False
    else:
        fail("Event row not found when re-fetching from Postgres")
        all_passed = False

    # ------------------------------------------------------------------
    # Final verdict
    # ------------------------------------------------------------------
    print(f"\n{SEP}")
    if all_passed:
        print(f"  {GREEN}PASS{RESET}  -- Phase 3 end-to-end verification succeeded.")
    else:
        print(f"  {RED}FAIL{RESET}  -- One or more checks did not pass (see above).")
    print(SEP)
    print()

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
