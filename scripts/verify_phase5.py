"""
scripts/verify_phase5.py
~~~~~~~~~~~~~~~~~~~~~~~~
Phase 5 end-to-end verification script.

Tests two scenarios:

  Suite A — Ingestion Deduplication:
    POST event with key ``test-idem-001``.  Assert HTTP 202 Accepted.
    POST the exact same payload immediately.  Assert HTTP 200,
    matching event ID, and ``X-Idempotent-Replay: true`` header.
    DB check: exactly 1 Event row for the key; no extra delivery task.

  Suite B — Header Injection:
    Register the /api/v1/mock/echo endpoint.
    POST a fresh event and wait for delivery.
    Parse the DeliveryAttempt.response_body (JSON from /mock/echo).
    Assert all four idempotency headers are present and non-empty:
        X-Webhook-Event-Id, X-Webhook-Delivery-Id,
        X-Webhook-Idempotency-Key, X-Webhook-Timestamp.

Prerequisites:
  - Postgres + Redis running
  - uvicorn app.main:app --port 8000
  - celery -A app.core.celery_app worker --loglevel=warning --pool=solo

Run from project root:
    python scripts/verify_phase5.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy import func, select

sys.path.insert(0, ".")

from app.core.database import AsyncSessionLocal          # noqa: E402
from app.models.delivery import DeliveryAttempt         # noqa: E402
from app.models.endpoint import WebhookEndpoint          # noqa: E402
from app.models.event import Event, EventStatus          # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE_URL = "http://127.0.0.1:8000"
EVENTS_URL   = f"{API_BASE_URL}/api/v1/events"
MOCK_ECHO_URL = f"{API_BASE_URL}/api/v1/mock/echo"

POLL_TIMEOUT_S = 30.0   # seconds to wait for Celery delivery
POLL_INTERVAL_S = 0.5

# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
SEP    = "=" * 68


def ok(msg: str)   -> None: print(f"  {GREEN}[OK]{RESET}   {msg}")
def fail(msg: str) -> None: print(f"  {RED}[FAIL]{RESET} {msg}")
def info(msg: str) -> None: print(f"  {CYAN}[INFO]{RESET} {msg}")
def warn(msg: str) -> None: print(f"  {YELLOW}[WARN]{RESET} {msg}")
def hdr(title: str) -> None:
    print(f"\n{'─'*68}")
    print(f"  {title}")
    print(f"{'─'*68}")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def deactivate_all_endpoints(session) -> int:
    """Deactivate every active endpoint for a clean-slate test."""
    all_active = (
        await session.execute(
            select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
        )
    ).scalars().all()
    for ep in all_active:
        ep.is_active = False
    if all_active:
        await session.commit()
    return len(all_active)


async def register_endpoint(session, target_url: str) -> WebhookEndpoint:
    """Create and commit a new active WebhookEndpoint."""
    ep = WebhookEndpoint(target_url=target_url, is_active=True)
    session.add(ep)
    await session.flush()
    await session.commit()
    return ep


async def count_events_for_key(idempotency_key: str) -> int:
    """Return the number of Event rows matching the given idempotency_key."""
    async with AsyncSessionLocal() as s:
        result = await s.execute(
            select(func.count()).select_from(Event).where(
                Event.idempotency_key == idempotency_key
            )
        )
        return result.scalar_one()


async def get_delivery_attempts(event_id: uuid.UUID) -> list[DeliveryAttempt]:
    """Return all DeliveryAttempt rows for an event, ordered by attempt_number."""
    async with AsyncSessionLocal() as s:
        result = await s.execute(
            select(DeliveryAttempt)
            .where(DeliveryAttempt.event_id == event_id)
            .order_by(DeliveryAttempt.attempt_number)
        )
        return list(result.scalars().all())


async def poll_for_delivery(
    event_id: uuid.UUID,
    timeout_s: float = POLL_TIMEOUT_S,
    min_attempts: int = 1,
) -> tuple[Event | None, list[DeliveryAttempt]]:
    """
    Poll until Event.status != PENDING and at least min_attempts exist,
    or until timeout_s elapses.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        async with AsyncSessionLocal() as s:
            event = await s.get(Event, event_id)
            attempts = (
                await s.execute(
                    select(DeliveryAttempt)
                    .where(DeliveryAttempt.event_id == event_id)
                    .order_by(DeliveryAttempt.attempt_number)
                )
            ).scalars().all()

        if (
            event is not None
            and event.status != EventStatus.PENDING
            and len(attempts) >= min_attempts
        ):
            return event, list(attempts)

        await asyncio.sleep(POLL_INTERVAL_S)

    # Return whatever we have on timeout
    async with AsyncSessionLocal() as s:
        event = await s.get(Event, event_id)
        attempts = (
            await s.execute(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.event_id == event_id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        ).scalars().all()
    return event, list(attempts)


# ---------------------------------------------------------------------------
# Suite A: Ingestion Deduplication
# ---------------------------------------------------------------------------

async def suite_a_deduplication(client: httpx.AsyncClient) -> bool:
    """
    Verify that posting the same idempotency_key twice:
      1. First POST  → HTTP 202, new event created, task enqueued.
      2. Second POST → HTTP 200, same event_id returned, X-Idempotent-Replay: true.
      3. DB has exactly 1 Event row for the key.
      4. Only 1 delivery task executes (no second fan-out).
    """
    hdr("Suite A — Ingestion Deduplication")
    passed = True

    idempotency_key = f"test-idem-{uuid.uuid4()}"
    payload = {
        "event_type": "order.created",
        "payload": {"order_id": str(uuid.uuid4()), "source": "verify_phase5_suite_a"},
        "idempotency_key": idempotency_key,
    }

    # -----------------------------------------------------------------------
    # Step 1: First POST → 202 Accepted
    # -----------------------------------------------------------------------
    info(f"Posting first request  (key={idempotency_key!r})")
    r1 = await client.post(EVENTS_URL, json=payload)
    info(f"First response: HTTP {r1.status_code}  body={r1.text[:120]}")

    if r1.status_code == 202:
        ok("First POST → HTTP 202 Accepted")
    else:
        fail(f"First POST → HTTP {r1.status_code} (expected 202)")
        passed = False

    first_body = r1.json()
    event_id_str: str = first_body.get("event_id", "")
    if not event_id_str:
        fail("No 'event_id' in first response body")
        return False  # Cannot continue without an event_id

    event_id = uuid.UUID(event_id_str)
    ok(f"event_id = {event_id}")

    if r1.headers.get("X-Idempotent-Replay", "").lower() == "true":
        warn("First POST already returned X-Idempotent-Replay: true — key may be stale")

    # -----------------------------------------------------------------------
    # Step 2: Second POST (same payload + key) → 200 + replay header
    # -----------------------------------------------------------------------
    info("Posting second request (same idempotency_key — should replay)")
    r2 = await client.post(EVENTS_URL, json=payload)
    info(f"Second response: HTTP {r2.status_code}  headers={dict(r2.headers)}")

    if r2.status_code == 200:
        ok("Second POST → HTTP 200 OK (replay)")
    else:
        fail(f"Second POST → HTTP {r2.status_code} (expected 200)")
        passed = False

    replay_header = r2.headers.get("X-Idempotent-Replay", "")
    if replay_header.lower() == "true":
        ok("X-Idempotent-Replay: true header present")
    else:
        fail(f"X-Idempotent-Replay header missing or wrong value: {replay_header!r}")
        passed = False

    second_body = r2.json()
    replayed_id = second_body.get("event_id", "")
    if str(replayed_id) == str(event_id):
        ok(f"Replayed event_id matches original: {replayed_id}")
    else:
        fail(f"Replayed event_id {replayed_id!r} != original {event_id!r}")
        passed = False

    # -----------------------------------------------------------------------
    # Step 3: DB check — exactly 1 Event row for the key
    # -----------------------------------------------------------------------
    db_count = await count_events_for_key(idempotency_key)
    if db_count == 1:
        ok(f"DB: exactly 1 Event row for idempotency_key={idempotency_key!r}")
    else:
        fail(f"DB: found {db_count} Event row(s) for key (expected 1)")
        passed = False

    # -----------------------------------------------------------------------
    # Step 4: Wait briefly — confirm only 1 set of delivery attempts exists
    #         (the second POST must NOT have enqueued a second task)
    # -----------------------------------------------------------------------
    info("Waiting up to 20 s for Celery delivery to complete ...")
    async with AsyncSessionLocal() as s:
        ep_count_result = await s.execute(
            select(func.count()).select_from(WebhookEndpoint)
            .where(WebhookEndpoint.is_active.is_(True))
        )
    active_endpoints = ep_count_result.scalar_one()
    info(f"Active endpoints at time of delivery: {active_endpoints}")

    _, attempts = await poll_for_delivery(event_id, timeout_s=20.0, min_attempts=max(active_endpoints, 0))

    if active_endpoints == 0:
        info("No active endpoints registered — DeliveryAttempt count check skipped")
    else:
        # Expected: exactly `active_endpoints` attempts (1 per endpoint, from 1 delivery task)
        # An extra task would produce 2× attempts.
        if len(attempts) == active_endpoints:
            ok(f"DeliveryAttempt count = {len(attempts)} (1 per active endpoint — no double-delivery)")
        elif len(attempts) < active_endpoints:
            warn(f"DeliveryAttempt count = {len(attempts)} < {active_endpoints} (Celery may still be processing)")
        else:
            fail(f"DeliveryAttempt count = {len(attempts)} > {active_endpoints} — possible duplicate task!")
            passed = False

    print()
    return passed


# ---------------------------------------------------------------------------
# Suite B: Header Injection
# ---------------------------------------------------------------------------

async def suite_b_header_injection(client: httpx.AsyncClient) -> bool:
    """
    Verify that the four webhook delivery headers are injected and received:
      X-Webhook-Event-Id, X-Webhook-Delivery-Id,
      X-Webhook-Idempotency-Key, X-Webhook-Timestamp.

    Strategy: register /mock/echo as an endpoint, post an event, wait for
    delivery, then read DeliveryAttempt.response_body (which contains the
    JSON echo of all headers received by /mock/echo).
    """
    hdr("Suite B — Delivery Header Injection")
    passed = True

    # Setup: deactivate all existing endpoints, register /mock/echo
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} pre-existing endpoint(s).")

    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, MOCK_ECHO_URL)
    info(f"Registered echo endpoint: {MOCK_ECHO_URL}  id={ep.id}")

    # Post a fresh event
    idempotency_key = f"test-headers-{uuid.uuid4()}"
    payload = {
        "event_type": "header.check",
        "payload": {"source": "verify_phase5_suite_b"},
        "idempotency_key": idempotency_key,
    }
    info(f"Posting event (key={idempotency_key!r})")
    r = await client.post(EVENTS_URL, json=payload)
    if r.status_code != 202:
        fail(f"POST event returned HTTP {r.status_code} (expected 202)")
        return False

    event_id = uuid.UUID(r.json()["event_id"])
    ok(f"Event accepted: event_id={event_id}")

    # Poll for delivery
    info(f"Polling up to {POLL_TIMEOUT_S}s for Celery delivery ...")
    event, attempts = await poll_for_delivery(event_id, timeout_s=POLL_TIMEOUT_S, min_attempts=1)

    if not attempts:
        fail("No DeliveryAttempt rows found after polling — Celery worker may not be running")
        return False

    attempt = attempts[0]
    ok(f"DeliveryAttempt found: id={attempt.id}  http_status={attempt.http_status}")

    if attempt.http_status != 200:
        fail(f"Echo endpoint returned HTTP {attempt.http_status} (expected 200)")
        passed = False

    # Parse the echo body
    if not attempt.response_body:
        fail("DeliveryAttempt.response_body is empty — cannot inspect headers")
        return False

    try:
        echo_data = json.loads(attempt.response_body)
    except json.JSONDecodeError as exc:
        fail(f"Could not parse response_body as JSON: {exc}")
        fail(f"Raw body: {attempt.response_body[:300]}")
        return False

    received_headers: dict = echo_data.get("received_headers", {})
    # httpx normalises header names to lowercase
    received_lower = {k.lower(): v for k, v in received_headers.items()}

    print()
    print(f"  {'─'*64}")
    print(f"  Received webhook headers (as echoed by /mock/echo)")
    print(f"  {'─'*64}")

    required_headers = [
        "x-webhook-event-id",
        "x-webhook-delivery-id",
        "x-webhook-idempotency-key",
        "x-webhook-timestamp",
    ]
    for hdr_name in required_headers:
        value = received_lower.get(hdr_name, "")
        if value:
            ok(f"{hdr_name}: {value}")
        else:
            fail(f"{hdr_name}: MISSING")
            passed = False

    print(f"  {'─'*64}\n")

    # Semantic assertions
    event_id_hdr = received_lower.get("x-webhook-event-id", "")
    if event_id_hdr == str(event_id):
        ok(f"X-Webhook-Event-Id matches event_id: {event_id_hdr}")
    else:
        fail(f"X-Webhook-Event-Id={event_id_hdr!r} != event_id={event_id!r}")
        passed = False

    idem_key_hdr = received_lower.get("x-webhook-idempotency-key", "")
    if idem_key_hdr == idempotency_key:
        ok(f"X-Webhook-Idempotency-Key matches submitted key: {idem_key_hdr}")
    else:
        fail(f"X-Webhook-Idempotency-Key={idem_key_hdr!r} != {idempotency_key!r}")
        passed = False

    delivery_id_hdr = received_lower.get("x-webhook-delivery-id", "")
    try:
        uuid.UUID(delivery_id_hdr)
        ok(f"X-Webhook-Delivery-Id is a valid UUID: {delivery_id_hdr}")
    except ValueError:
        fail(f"X-Webhook-Delivery-Id is not a valid UUID: {delivery_id_hdr!r}")
        passed = False

    ts_hdr = received_lower.get("x-webhook-timestamp", "")
    try:
        datetime.fromisoformat(ts_hdr)
        ok(f"X-Webhook-Timestamp is a valid ISO-8601 timestamp: {ts_hdr}")
    except ValueError:
        fail(f"X-Webhook-Timestamp is not a valid ISO-8601 string: {ts_hdr!r}")
        passed = False

    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"\n{SEP}")
    print("  Webhook Engine — Phase 5 Idempotency & Header Injection Verification")
    print(SEP)
    print(f"  Run time: {datetime.now(tz=timezone.utc).isoformat()}")
    print(SEP)

    # Connectivity check
    try:
        async with httpx.AsyncClient(timeout=3.0) as c:
            r = await c.get(f"{API_BASE_URL}/healthz")
            if r.status_code != 200:
                raise RuntimeError(f"healthz returned {r.status_code}")
    except Exception as exc:
        fail(f"FastAPI server not reachable: {exc}")
        print(f"\n{SEP}\n  {RED}FAIL{RESET}\n{SEP}\n")
        sys.exit(1)
    info("FastAPI server reachable [ok]")

    results: list[bool] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        passed_a = await suite_a_deduplication(client)
        results.append(passed_a)

        passed_b = await suite_b_header_injection(client)
        results.append(passed_b)

    # Final verdict
    all_passed = all(results)
    print(f"\n{SEP}")
    suite_results = [
        f"Suite A (Deduplication): {'PASS' if results[0] else 'FAIL'}",
        f"Suite B (Header Injection): {'PASS' if results[1] else 'FAIL'}",
    ]
    for line in suite_results:
        color = GREEN if "PASS" in line else RED
        print(f"  {color}{line}{RESET}")
    print()
    if all_passed:
        print(f"  {GREEN}PASS{RESET}  — Phase 5 idempotency verification succeeded.")
    else:
        print(f"  {RED}FAIL{RESET}  — One or more suites did not pass (see above).")
    print(SEP)
    print()

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
