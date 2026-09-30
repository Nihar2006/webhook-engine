"""
scripts/verify_phase7.py
~~~~~~~~~~~~~~~~~~~~~~~~
Phase 7 end-to-end verification script.

Three suites:

  Suite A -- Valid Signature (live end-to-end):
    Register endpoint pointing to /mock/secure?secret=<TEST_SECRET>.
    POST an event. Worker signs with TEST_SECRET and delivers.
    Assert DeliveryAttempt.http_status == 200 (mock accepted the signature).

  Suite B -- Tamper Resistance (unit-level):
    Generate a known signature.
    Modify one character in the payload body.
    Assert verify_webhook_signature returns False.
    Assert the correct body returns True.

  Suite C -- Timing-Attack Defence and Expiration (unit-level):
    Generate signature with timestamp 400 s in the past.
    Assert verify_webhook_signature returns False (expired).
    Generate signature with fresh timestamp but wrong secret.
    Assert verify_webhook_signature returns False (HMAC mismatch).
    Assert hmac.compare_digest is used inside verify_webhook_signature.

Prerequisites:
  - Postgres + Redis running  (docker compose up -d)
  - uvicorn app.main:app --port 8000 --reload
  - celery -A app.core.celery_app worker --loglevel=warning --pool=solo

Run from project root:
    python scripts/verify_phase7.py
"""
from __future__ import annotations

import asyncio
import inspect
import sys
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
from sqlalchemy import select

sys.path.insert(0, ".")

from app.core.database import AsyncSessionLocal             # noqa: E402
from app.core.security import (                            # noqa: E402
    generate_webhook_signature,
    verify_webhook_signature,
)
from app.models.delivery import DeliveryAttempt            # noqa: E402
from app.models.endpoint import WebhookEndpoint             # noqa: E402
from app.models.event import Event, EventStatus             # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE_URL  = "http://127.0.0.1:8000"
EVENTS_URL    = f"{API_BASE_URL}/api/v1/events"
TEST_SECRET   = "phase7-test-secret-abc123"

POLL_TIMEOUT_S  = 30.0
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


def ok(msg: str)    -> None: print(f"  {GREEN}[OK]{RESET}   {msg}")
def fail(msg: str)  -> None: print(f"  {RED}[FAIL]{RESET} {msg}")
def info(msg: str)  -> None: print(f"  {CYAN}[INFO]{RESET} {msg}")
def hdr(title: str) -> None:
    print(f"\n{'--' * 34}")
    print(f"  {title}")
    print(f"{'--' * 34}")


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


async def register_endpoint(
    session,
    target_url: str,
    secret: str | None = None,
) -> WebhookEndpoint:
    ep = WebhookEndpoint(target_url=target_url, secret=secret, is_active=True)
    session.add(ep)
    await session.flush()
    await session.commit()
    return ep


async def poll_for_delivery(
    event_id: uuid.UUID,
    timeout_s: float = POLL_TIMEOUT_S,
    min_attempts: int = 1,
) -> tuple[Event | None, list[DeliveryAttempt]]:
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
# Suite A: Valid Signature -- live end-to-end
# ---------------------------------------------------------------------------

async def suite_a_valid_signature(client: httpx.AsyncClient) -> bool:
    hdr("Suite A -- Valid Signature (live end-to-end)")
    passed = True

    # Setup: deactivate all, register secure endpoint
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} pre-existing endpoint(s).")

    # Embed secret in the target URL so the delivery worker passes it transparently.
    secure_url = f"{API_BASE_URL}/api/v1/mock/secure?secret={quote(TEST_SECRET)}"
    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, secure_url, secret=TEST_SECRET)
    info(f"Registered secure endpoint: {secure_url}")
    info(f"Endpoint secret: {TEST_SECRET!r}")

    # POST event
    idempotency_key = f"phase7-suite-a-{uuid.uuid4()}"
    payload = {
        "event_type": "payment.signed",
        "payload": {"amount": 1999, "currency": "USD", "source": "verify_phase7"},
        "idempotency_key": idempotency_key,
    }
    info("Posting event ...")
    r = await client.post(EVENTS_URL, json=payload)
    if r.status_code != 202:
        fail(f"POST event returned HTTP {r.status_code} (expected 202)")
        return False
    ok("POST event -> HTTP 202 Accepted")
    event_id = uuid.UUID(r.json()["event_id"])
    info(f"event_id = {event_id}")

    # Poll for delivery
    info(f"Polling up to {POLL_TIMEOUT_S}s for delivery ...")
    event, attempts = await poll_for_delivery(event_id, timeout_s=POLL_TIMEOUT_S)

    if not attempts:
        fail("No DeliveryAttempt found -- Celery worker may not be running")
        return False

    attempt = attempts[0]
    info(f"DeliveryAttempt: id={attempt.id}  http_status={attempt.http_status}")
    info(f"Response body: {attempt.response_body}")

    if attempt.http_status == 200:
        ok("Mock secure endpoint returned HTTP 200 -- signature VERIFIED by receiver")
    elif attempt.http_status == 401:
        fail("Mock secure endpoint returned HTTP 401 -- signature REJECTED")
        fail(f"Response: {attempt.response_body}")
        passed = False
    elif attempt.http_status == 400:
        fail("Mock secure endpoint returned HTTP 400 -- timestamp expired")
        fail(f"Response: {attempt.response_body}")
        passed = False
    else:
        fail(f"Unexpected HTTP status from /mock/secure: {attempt.http_status}")
        passed = False

    if event and event.status == EventStatus.DELIVERED:
        ok("Event.status = DELIVERED")
    else:
        stat = event.status.value if event else "NOT FOUND"
        fail(f"Event.status = {stat} (expected DELIVERED)")
        passed = False

    return passed


# ---------------------------------------------------------------------------
# Suite B: Tamper Resistance (unit-level)
# ---------------------------------------------------------------------------

def suite_b_tamper_resistance() -> bool:
    hdr("Suite B -- Tamper Resistance (unit-level)")
    passed = True
    secret   = "tamper-test-secret"
    ts       = int(time.time())
    original = '{"amount":1999,"currency":"USD"}'
    sig      = generate_webhook_signature(secret, ts, original)
    info(f"Generated signature: {sig[:60]}...")

    # Correct body -> True
    if verify_webhook_signature(secret, sig, original.encode(), tolerance=60):
        ok("verify_webhook_signature(correct body) = True")
    else:
        fail("verify_webhook_signature(correct body) returned False -- should be True")
        passed = False

    # Tamper: change last char
    tampered = original[:-1] + ("X" if original[-1] != "X" else "Y")
    if not verify_webhook_signature(secret, sig, tampered.encode(), tolerance=60):
        ok("verify_webhook_signature(tampered body) = False  (tamper detected)")
    else:
        fail("verify_webhook_signature(tampered body) returned True -- VULNERABILITY!")
        passed = False

    # Tamper: flip one bit in the middle
    mid = len(original) // 2
    tampered2 = original[:mid] + chr(ord(original[mid]) ^ 1) + original[mid + 1:]
    if not verify_webhook_signature(secret, sig, tampered2.encode(), tolerance=60):
        ok("verify_webhook_signature(1-bit flip) = False  (tamper detected)")
    else:
        fail("verify_webhook_signature(1-bit flip) returned True -- VULNERABILITY!")
        passed = False

    return passed


# ---------------------------------------------------------------------------
# Suite C: Timing-Attack Defence and Expiration (unit-level)
# ---------------------------------------------------------------------------

def suite_c_timing_and_expiry() -> bool:
    hdr("Suite C -- Timing-Attack Defence and Expiration (unit-level)")
    passed = True
    secret  = "timing-test-secret"
    payload = '{"event":"test"}'

    # Expired timestamp (400 s old, tolerance=300)
    old_ts  = int(time.time()) - 400
    old_sig = generate_webhook_signature(secret, old_ts, payload)
    if not verify_webhook_signature(secret, old_sig, payload.encode(), tolerance=300):
        ok("Expired timestamp (400 s old) correctly REJECTED")
    else:
        fail("Expired timestamp was ACCEPTED -- replay protection broken!")
        passed = False

    # Fresh timestamp with wrong secret
    fresh_ts = int(time.time())
    correct_sig = generate_webhook_signature(secret, fresh_ts, payload)
    if not verify_webhook_signature("wrong-secret", correct_sig, payload.encode(), tolerance=300):
        ok("Wrong secret correctly REJECTED (HMAC mismatch)")
    else:
        fail("Wrong secret was ACCEPTED -- HMAC verification broken!")
        passed = False

    # Forged v1= value (correct timestamp, fabricated digest)
    forged_sig = f"t={fresh_ts},v1={'a' * 64}"
    if not verify_webhook_signature(secret, forged_sig, payload.encode(), tolerance=300):
        ok("Forged digest correctly REJECTED")
    else:
        fail("Forged digest was ACCEPTED -- HMAC verification broken!")
        passed = False

    # Malformed header
    if not verify_webhook_signature(secret, "not-a-valid-header", payload.encode()):
        ok("Malformed header correctly REJECTED (parse error)")
    else:
        fail("Malformed header was ACCEPTED")
        passed = False

    # Confirm hmac.compare_digest is used (inspect source)
    src = inspect.getsource(verify_webhook_signature)
    if "compare_digest" in src:
        ok("hmac.compare_digest is present in verify_webhook_signature source")
    else:
        fail("hmac.compare_digest NOT FOUND -- timing attack possible!")
        passed = False

    # tolerance=0 disables freshness check
    old_sig2 = generate_webhook_signature(secret, old_ts, payload)
    if verify_webhook_signature(secret, old_sig2, payload.encode(), tolerance=0):
        ok("tolerance=0 disables expiry check -- old signature accepted (expected)")
    else:
        fail("tolerance=0 still rejected the old signature -- freshness bypass broken")
        passed = False

    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"\n{SEP}")
    print("  Webhook Engine -- Phase 7 HMAC-SHA256 Signing Verification")
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

    # Run unit suites first (no network I/O needed)
    passed_b = suite_b_tamper_resistance()
    passed_c = suite_c_timing_and_expiry()

    # Live end-to-end suite
    async with httpx.AsyncClient(timeout=10.0) as client:
        passed_a = await suite_a_valid_signature(client)

    # Final verdict
    all_passed = passed_a and passed_b and passed_c
    print(f"\n{SEP}")
    results = [
        (f"Suite A (Valid Signature, live):   {'PASS' if passed_a else 'FAIL'}", passed_a),
        (f"Suite B (Tamper Resistance, unit): {'PASS' if passed_b else 'FAIL'}", passed_b),
        (f"Suite C (Expiry + Timing, unit):   {'PASS' if passed_c else 'FAIL'}", passed_c),
    ]
    for line, p in results:
        print(f"  {GREEN if p else RED}{line}{RESET}")
    print()
    if all_passed:
        print(f"  {GREEN}PASS{RESET}  -- Phase 7 HMAC signing verification succeeded.")
    else:
        print(f"  {RED}FAIL{RESET}  -- One or more suites did not pass (see above).")
    print(SEP)
    print()

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
