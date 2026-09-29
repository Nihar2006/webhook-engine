"""
scripts/verify_phase6.py
~~~~~~~~~~~~~~~~~~~~~~~~
Phase 6 end-to-end verification script.

Tests two scenarios:

  Suite A \u2014 DLQ Transition:
    Register /mock/failing (always 500).
    POST an event and wait for Celery to exhaust all 4 retries.
    Assert Event.status == DEAD_LETTER in Postgres.
    Assert GET /api/v1/dlq lists the event with total_attempts=5.

  Suite B \u2014 Manual Replay:
    Update the endpoint\u2019s target_url to /mock/receiver (always 200).
    POST /api/v1/events/{event_id}/replay \u2192 assert 202 REPLAY_QUEUED.
    Poll until Event.status == DELIVERED.
    Assert a new DeliveryAttempt row was created (attempt_number > 5).
    Assert GET /api/v1/dlq no longer lists the event (it\u2019s DELIVERED now).

Prerequisites:
  - Postgres + Redis running  (docker compose up -d)
  - uvicorn app.main:app --port 8000 --reload
  - celery -A app.core.celery_app worker --loglevel=warning --pool=solo

Run from project root:
    python scripts/verify_phase6.py
"""
from __future__ import annotations

import asyncio
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy import select, update

sys.path.insert(0, ".")

from app.core.database import AsyncSessionLocal          # noqa: E402
from app.models.delivery import DeliveryAttempt         # noqa: E402
from app.models.endpoint import WebhookEndpoint          # noqa: E402
from app.models.event import Event, EventStatus          # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE_URL     = "http://127.0.0.1:8000"
EVENTS_URL       = f"{API_BASE_URL}/api/v1/events"
DLQ_URL          = f"{API_BASE_URL}/api/v1/dlq"
MOCK_FAILING_URL = f"{API_BASE_URL}/api/v1/mock/failing"
MOCK_RECEIVER_URL = f"{API_BASE_URL}/api/v1/mock/receiver"

# 5 total attempts (1 initial + 4 retries), each with jitter backoff.
# Worst-case cumulative jitter: 0 + 2 + 4 + 8 + 16 = 30 s cap per attempt
# We allow 120 s to cover realistic worst-case.
POLL_TIMEOUT_DLQ_S    = 120.0
POLL_TIMEOUT_REPLAY_S = 40.0
POLL_INTERVAL_S       = 1.0

# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
SEP    = "=" * 68


def ok(msg: str)    -> None: print(f"  {GREEN}[OK]{RESET}   {msg}")
def fail(msg: str)  -> None: print(f"  {RED}[FAIL]{RESET} {msg}")
def info(msg: str)  -> None: print(f"  {CYAN}[INFO]{RESET} {msg}")
def warn(msg: str)  -> None: print(f"  {YELLOW}[WARN]{RESET} {msg}")
def hdr(title: str) -> None:
    print(f"\n{'─'*68}")
    print(f"  {title}")
    print(f"{'─'*68}")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def deactivate_all_endpoints(session) -> int:
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
    ep = WebhookEndpoint(target_url=target_url, is_active=True)
    session.add(ep)
    await session.flush()
    await session.commit()
    return ep


async def update_endpoint_url(endpoint_id: uuid.UUID, new_url: str) -> None:
    """Change an endpoint\u2019s target_url in-place (without deactivating it)."""
    async with AsyncSessionLocal() as s:
        ep = await s.get(WebhookEndpoint, endpoint_id)
        if ep is None:
            raise RuntimeError(f"Endpoint {endpoint_id} not found")
        ep.target_url = new_url
        await s.commit()


async def poll_for_status(
    event_id: uuid.UUID,
    target_status: EventStatus,
    timeout_s: float,
    min_attempts: int = 1,
) -> tuple[Event | None, list[DeliveryAttempt]]:
    """Poll until Event.status == target_status (or timeout)."""
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
            and event.status == target_status
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


def print_attempt_table(attempts: list[DeliveryAttempt]) -> None:
    print(f"\n  {'─'*62}")
    print(f"  {'#':>3}  {'http_status':>11}  {'created_at (UTC)':>26}  {'Δ since prev':>14}")
    print(f"  {'─'*62}")
    prev_ts = None
    for a in attempts:
        ts = a.created_at
        if ts.tzinfo is None:
            from datetime import timezone as _tz
            ts = ts.replace(tzinfo=_tz.utc)
        delta_str = ""
        if prev_ts is not None:
            delta_str = f"{(ts - prev_ts).total_seconds():+.2f} s"
        status_str = str(a.http_status) if a.http_status else "N/A"
        print(
            f"  {a.attempt_number:>3}  {status_str:>11}  "
            f"{ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC':>26}  "
            f"{delta_str:>14}"
        )
        prev_ts = ts
    print(f"  {'─'*62}\n")


# ---------------------------------------------------------------------------
# Suite A: DLQ Transition
# ---------------------------------------------------------------------------

async def suite_a_dlq_transition(client: httpx.AsyncClient) -> tuple[bool, uuid.UUID | None]:
    """
    Drive an event into DEAD_LETTER by routing it to the always-500 mock.
    Returns (passed, event_id) so Suite B can replay the same event.
    """
    hdr("Suite A \u2014 DLQ Transition (5 \u00d7 500 \u2192 DEAD_LETTER)")
    passed = True

    # Setup
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} pre-existing endpoint(s).")

    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, MOCK_FAILING_URL)
    info(f"Registered failing endpoint: {MOCK_FAILING_URL}  id={ep.id}")

    # POST event
    idempotency_key = f"verify-phase6-dlq-{uuid.uuid4()}"
    payload = {
        "event_type": "payment.processed",
        "payload": {"amount": 9900, "currency": "USD", "source": "verify_phase6"},
        "idempotency_key": idempotency_key,
    }
    info("Posting event to failing endpoint ...")
    r = await client.post(EVENTS_URL, json=payload)
    if r.status_code != 202:
        fail(f"POST event returned HTTP {r.status_code} (expected 202)")
        return False, None
    ok("POST event \u2192 HTTP 202 Accepted")

    event_id = uuid.UUID(r.json()["event_id"])
    info(f"event_id = {event_id}")

    # Poll for DEAD_LETTER (5 attempts \u00d7 jitter \u2264 120s)
    info(f"Polling up to {POLL_TIMEOUT_DLQ_S}s for DEAD_LETTER status ...")
    event, attempts = await poll_for_status(
        event_id,
        target_status=EventStatus.DEAD_LETTER,
        timeout_s=POLL_TIMEOUT_DLQ_S,
        min_attempts=5,
    )

    # Assert DEAD_LETTER status
    if event is None:
        fail("Event not found in Postgres after polling")
        return False, None

    if event.status == EventStatus.DEAD_LETTER:
        ok(f"Event.status = DEAD_LETTER \u2713")
    else:
        fail(f"Event.status = {event.status.value}  (expected DEAD_LETTER)")
        passed = False

    # Assert 5 delivery attempts (1 initial + 4 retries)
    if len(attempts) == 5:
        ok(f"DeliveryAttempt count = 5 (1 initial + 4 retries)")
    else:
        fail(f"DeliveryAttempt count = {len(attempts)} (expected 5)")
        passed = False

    all_500 = all(a.http_status == 500 for a in attempts)
    if all_500:
        ok("All 5 delivery attempts returned HTTP 500")
    else:
        fail(f"HTTP statuses: {[a.http_status for a in attempts]} (expected all 500)")
        passed = False

    print_attempt_table(attempts)

    # Assert DLQ API lists the event
    info("Calling GET /api/v1/dlq ...")
    dlq_resp = await client.get(DLQ_URL)
    if dlq_resp.status_code != 200:
        fail(f"GET /api/v1/dlq returned HTTP {dlq_resp.status_code}")
        passed = False
    else:
        dlq_data = dlq_resp.json()
        dlq_ids = [item["id"] for item in dlq_data.get("items", [])]
        if str(event_id) in dlq_ids:
            ok(f"GET /api/v1/dlq lists event_id {event_id}")
        else:
            fail(f"GET /api/v1/dlq does NOT list event_id {event_id}")
            fail(f"DLQ items: {dlq_ids}")
            passed = False

        # Check attempt metadata
        dlq_item = next(
            (item for item in dlq_data.get("items", []) if item["id"] == str(event_id)),
            None,
        )
        if dlq_item:
            if dlq_item.get("total_attempts") == 5:
                ok(f"DLQ item total_attempts = 5")
            else:
                fail(f"DLQ item total_attempts = {dlq_item.get('total_attempts')} (expected 5)")
                passed = False

            if dlq_item.get("last_http_status") == 500:
                ok("DLQ item last_http_status = 500")
            else:
                fail(f"DLQ item last_http_status = {dlq_item.get('last_http_status')}")
                passed = False

    return passed, ep.id if passed else None


# ---------------------------------------------------------------------------
# Suite B: Manual Replay
# ---------------------------------------------------------------------------

async def suite_b_manual_replay(
    client: httpx.AsyncClient,
    event_id: uuid.UUID,
    endpoint_id: uuid.UUID,
) -> bool:
    """
    Fix the endpoint (point it at /mock/receiver \u2192 200), replay the event,
    and assert DELIVERED.
    """
    hdr("Suite B \u2014 Manual Replay (DEAD_LETTER \u2192 PENDING \u2192 DELIVERED)")
    passed = True

    # Fix the endpoint: change target_url to the working receiver
    info(f"Updating endpoint {endpoint_id} \u2192 {MOCK_RECEIVER_URL}")
    await update_endpoint_url(endpoint_id, MOCK_RECEIVER_URL)
    ok(f"Endpoint target_url updated to {MOCK_RECEIVER_URL}")

    # Call replay endpoint
    replay_url = f"{EVENTS_URL}/{event_id}/replay"
    info(f"Calling POST {replay_url}")
    r = await client.post(replay_url)
    info(f"Replay response: HTTP {r.status_code}  body={r.text[:200]}")

    if r.status_code == 202:
        ok("POST /replay \u2192 HTTP 202 Accepted")
    else:
        fail(f"POST /replay \u2192 HTTP {r.status_code} (expected 202)")
        passed = False

    replay_body = r.json()
    if replay_body.get("status") == "REPLAY_QUEUED":
        ok("Response status = REPLAY_QUEUED")
    else:
        fail(f"Response status = {replay_body.get('status')!r} (expected REPLAY_QUEUED)")
        passed = False

    if str(replay_body.get("event_id")) == str(event_id):
        ok(f"Replayed event_id matches: {event_id}")
    else:
        fail(f"Replayed event_id mismatch: {replay_body.get('event_id')} != {event_id}")
        passed = False

    # Poll for DELIVERED
    info(f"Polling up to {POLL_TIMEOUT_REPLAY_S}s for DELIVERED status ...")
    event, attempts = await poll_for_status(
        event_id,
        target_status=EventStatus.DELIVERED,
        timeout_s=POLL_TIMEOUT_REPLAY_S,
        min_attempts=6,   # at least 6 total: 5 DLQ + 1 replay
    )

    if event is None:
        fail("Event not found in Postgres")
        return False

    if event.status == EventStatus.DELIVERED:
        ok("Event.status = DELIVERED \u2713")
    else:
        fail(f"Event.status = {event.status.value} (expected DELIVERED)")
        passed = False

    # Assert new DeliveryAttempt created (attempt_number > 5)
    new_attempts = [a for a in attempts if a.attempt_number > 5]
    if new_attempts:
        ok(f"New DeliveryAttempt created: attempt_number={new_attempts[0].attempt_number}  "
           f"http_status={new_attempts[0].http_status}")
    else:
        fail(f"No new DeliveryAttempt with attempt_number > 5 found (total={len(attempts)})")
        passed = False

    print_attempt_table(attempts)

    # Assert DLQ no longer lists the event
    info("Calling GET /api/v1/dlq (event should be gone) ...")
    dlq_resp = await client.get(DLQ_URL)
    if dlq_resp.status_code == 200:
        dlq_ids = [item["id"] for item in dlq_resp.json().get("items", [])]
        if str(event_id) not in dlq_ids:
            ok("Event no longer appears in GET /api/v1/dlq (DELIVERED)")
        else:
            fail("Event STILL appears in DLQ after successful replay")
            passed = False
    else:
        warn(f"GET /api/v1/dlq returned HTTP {dlq_resp.status_code} \u2014 skipping check")

    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"\n{SEP}")
    print("  Webhook Engine \u2014 Phase 6 DLQ & Replay Verification")
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

    async with httpx.AsyncClient(timeout=10.0) as client:
        passed_a, endpoint_id = await suite_a_dlq_transition(client)

        if not passed_a or endpoint_id is None:
            print(f"\n{SEP}")
            print(f"  {RED}Suite A FAILED{RESET} \u2014 cannot run Suite B without a known event in DEAD_LETTER.")
            print(SEP)
            sys.exit(1)

        # Fetch the event_id from the DLQ listing for Suite B
        async with AsyncSessionLocal() as s:
            dlq_events = (
                await s.execute(
                    select(Event)
                    .where(Event.status == EventStatus.DEAD_LETTER)
                    .order_by(Event.created_at.desc())
                    .limit(1)
                )
            ).scalars().all()
        if not dlq_events:
            fail("No DEAD_LETTER events found for Suite B")
            sys.exit(1)
        event_id_for_replay = dlq_events[0].id

        passed_b = await suite_b_manual_replay(client, event_id_for_replay, endpoint_id)

    # Final verdict
    all_passed = passed_a and passed_b
    print(f"\n{SEP}")
    results = [
        (f"Suite A (DLQ Transition): {'PASS' if passed_a else 'FAIL'}", passed_a),
        (f"Suite B (Manual Replay):  {'PASS' if passed_b else 'FAIL'}", passed_b),
    ]
    for line, p in results:
        print(f"  {GREEN if p else RED}{line}{RESET}")
    print()
    if all_passed:
        print(f"  {GREEN}PASS{RESET}  \u2014 Phase 6 DLQ & Replay verification succeeded.")
    else:
        print(f"  {RED}FAIL{RESET}  \u2014 One or more suites did not pass (see above).")
    print(SEP)
    print()

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
