"""
scripts/verify_phase4.py
~~~~~~~~~~~~~~~~~~~~~~~~
Phase 4 end-to-end verification script.

Tests two retry scenarios:

  Scenario A — Flaky endpoint (/mock/flaky):
    POST an event to an endpoint that returns 503 twice then 200.
    Assert Event becomes DELIVERED with 3 DeliveryAttempt rows
    (attempt_number 1→503, 2→503, 3→200).

  Scenario B — Failing endpoint (/mock/failing):
    POST an event to an endpoint that always returns 500.
    Assert Event becomes FAILED after max_retries=4 exhausted,
    with 5 DeliveryAttempt rows (all 500).

Prints a full audit trail (attempt_number, http_status, timestamp,
backoff delta between attempts) then PASS or FAIL.

Prerequisites:
  - Postgres + Redis running
  - uvicorn app.main:app --port 8000
  - celery -A app.core.celery_app worker --loglevel=warning --pool=solo

Run from project root:
    python scripts/verify_phase4.py
"""
from __future__ import annotations

import asyncio
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

sys.path.insert(0, ".")

from app.core.database import AsyncSessionLocal          # noqa: E402
from app.models.delivery import DeliveryAttempt         # noqa: E402
from app.models.endpoint import WebhookEndpoint          # noqa: E402
from app.models.event import Event, EventStatus          # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE_URL    = "http://127.0.0.1:8000"
EVENTS_URL      = f"{API_BASE_URL}/api/v1/events"
MOCK_FLAKY_URL  = f"{API_BASE_URL}/api/v1/mock/flaky"
MOCK_FAILING_URL = f"{API_BASE_URL}/api/v1/mock/failing"

# Celery retries with jitter — worst-case for failing scenario:
# 5 attempts, max jitter cap per attempt: 0+2+4+8+16 = 30 s worst case.
POLL_TIMEOUT_FLAKY_S   = 40.0   # 3 attempts, low jitter
POLL_TIMEOUT_FAILING_S = 90.0   # 5 attempts, max_delay=60 possible
POLL_INTERVAL_S        = 0.75

# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
SEP    = "=" * 66


def ok(msg: str)   -> None: print(f"  {GREEN}[OK]{RESET}   {msg}")
def fail(msg: str) -> None: print(f"  {RED}[FAIL]{RESET} {msg}")
def info(msg: str) -> None: print(f"  {CYAN}[INFO]{RESET} {msg}")
def warn(msg: str) -> None: print(f"  {YELLOW}[WARN]{RESET} {msg}")


# ---------------------------------------------------------------------------
# Helpers
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


async def post_event(client: httpx.AsyncClient, idempotency_key: str) -> dict:
    """POST a test event and return the parsed response body."""
    resp = await client.post(EVENTS_URL, json={
        "event_type": "order.created",
        "payload": {"order_id": str(uuid.uuid4()), "source": "verify_phase4"},
        "idempotency_key": idempotency_key,
    })
    resp.raise_for_status()
    return resp.json()


async def poll_event(
    event_id: uuid.UUID,
    timeout_s: float,
    expected_attempts: int,
) -> tuple[Event | None, list[DeliveryAttempt]]:
    """
    Poll until Event.status != PENDING AND len(attempts) == expected_attempts,
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
            and len(attempts) >= expected_attempts
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


def print_audit_trail(attempts: list[DeliveryAttempt], scenario: str) -> None:
    """Print a formatted audit trail with backoff deltas between attempts."""
    print(f"\n  {'─'*62}")
    print(f"  Audit trail — {scenario}")
    print(f"  {'─'*62}")
    print(f"  {'#':>3}  {'http_status':>11}  {'created_at (UTC)':>26}  {'Δ since prev':>14}")
    print(f"  {'─'*62}")
    prev_ts: datetime | None = None
    for a in attempts:
        ts = a.created_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta_str = ""
        if prev_ts is not None:
            delta_s = (ts - prev_ts).total_seconds()
            delta_str = f"{delta_s:+.2f} s"
        status_str = str(a.http_status) if a.http_status else "N/A"
        print(f"  {a.attempt_number:>3}  {status_str:>11}  {ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC':>26}  {delta_str:>14}")
        prev_ts = ts
    print(f"  {'─'*62}")


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

async def run_scenario(
    client: httpx.AsyncClient,
    name: str,
    target_url: str,
    expected_status: EventStatus,
    expected_attempts: int,
    expected_http_statuses: list[int],
    poll_timeout: float,
) -> bool:
    """
    Register endpoint → POST event → poll → assert → print audit trail.
    Returns True on PASS, False on FAIL.
    """
    print(f"\n{'─'*66}")
    print(f"  Scenario: {name}")
    print(f"{'─'*66}")

    passed = True
    idempotency_key = f"verify-phase4-{name.lower().replace(' ', '-')}-{uuid.uuid4()}"

    # --- Setup ---
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} pre-existing endpoint(s).")

    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, target_url)
    info(f"Registered endpoint: {target_url}  id={ep.id}")

    # --- Post event ---
    body = await post_event(client, idempotency_key)
    event_id = uuid.UUID(body["event_id"])
    info(f"Posted event_id={event_id}  status={body['status']}")
    if body.get("status") != "ACCEPTED":
        fail("Expected status='ACCEPTED' in POST response")
        passed = False

    # --- Poll ---
    info(
        f"Polling up to {poll_timeout}s for {expected_attempts} attempt(s) "
        f"and Event.status != PENDING ..."
    )
    event, attempts = await poll_event(event_id, poll_timeout, expected_attempts)

    # --- Assert Event status ---
    if event is None:
        fail("Event row not found in Postgres after polling")
        passed = False
    elif event.status == expected_status:
        ok(f"Event.status = {event.status.value}")
    else:
        fail(f"Event.status = {event.status.value}  (expected {expected_status.value})")
        passed = False

    # --- Assert attempt count ---
    if len(attempts) == expected_attempts:
        ok(f"DeliveryAttempt rows: {len(attempts)} (expected {expected_attempts})")
    else:
        fail(
            f"DeliveryAttempt rows: {len(attempts)} (expected {expected_attempts})"
            + (" — Celery worker may still be processing" if len(attempts) < expected_attempts else "")
        )
        passed = False

    # --- Assert attempt_number sequence ---
    actual_nums = [a.attempt_number for a in attempts]
    expected_nums = list(range(1, expected_attempts + 1))
    if actual_nums == expected_nums:
        ok(f"attempt_number sequence: {actual_nums}")
    else:
        fail(f"attempt_number sequence: {actual_nums} (expected {expected_nums})")
        passed = False

    # --- Assert HTTP status codes ---
    actual_statuses = [a.http_status for a in attempts]
    if actual_statuses == expected_http_statuses:
        ok(f"HTTP status per attempt: {actual_statuses}")
    else:
        fail(
            f"HTTP status per attempt: {actual_statuses} "
            f"(expected {expected_http_statuses})"
        )
        passed = False

    # --- Audit trail ---
    print_audit_trail(attempts, name)

    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"\n{SEP}")
    print("  Webhook Engine — Phase 4 Retry + Backoff Verification")
    print(SEP)

    # Quick connectivity check
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

    async with httpx.AsyncClient(timeout=5.0) as client:

        # ----------------------------------------------------------------
        # Scenario A: Flaky endpoint — 503×2 then 200 → DELIVERED
        # ----------------------------------------------------------------
        passed_a = await run_scenario(
            client=client,
            name="Flaky (503 → 503 → 200)",
            target_url=MOCK_FLAKY_URL,
            expected_status=EventStatus.DELIVERED,
            expected_attempts=3,          # 2 retries + 1 success
            expected_http_statuses=[503, 503, 200],
            poll_timeout=POLL_TIMEOUT_FLAKY_S,
        )
        results.append(passed_a)

        # ----------------------------------------------------------------
        # Scenario B: Failing endpoint — always 500 → FAILED after 5 total
        # ----------------------------------------------------------------
        passed_b = await run_scenario(
            client=client,
            name="Failing (500 × 5, max retries exhausted)",
            target_url=MOCK_FAILING_URL,
            expected_status=EventStatus.FAILED,
            expected_attempts=5,          # 1 initial + 4 retries = 5 total
            expected_http_statuses=[500, 500, 500, 500, 500],
            poll_timeout=POLL_TIMEOUT_FAILING_S,
        )
        results.append(passed_b)

    # ----------------------------------------------------------------
    # Final verdict
    # ----------------------------------------------------------------
    all_passed = all(results)
    print(f"\n{SEP}")
    if all_passed:
        print(f"  {GREEN}PASS{RESET}  — Phase 4 retry + backoff verification succeeded.")
    else:
        print(f"  {RED}FAIL{RESET}  — One or more scenarios did not pass (see above).")
    print(SEP)
    print()

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
