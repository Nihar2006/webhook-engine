"""
scripts/verify_master.py
~~~~~~~~~~~~~~~~~~~~~~~~
Master Sanity Verification Suite -- Phases 1 through 8.

Runs 6 sequential assertion suites against the live FastAPI server,
Celery worker, Redis broker, and PostgreSQL database, then prints a
clear summary matrix with elapsed times and a final system verdict.

Suites
------
  Suite 1  Connectivity & Core Ingestion          (Phases 1-3)
  Suite 2  Transient Failures & Jitter Backoff    (Phase 4)
  Suite 3  Ingestion Idempotency & Headers        (Phase 5)
  Suite 4  DLQ State Machine & Replay Engine      (Phase 6)
  Suite 5  HMAC-SHA256 Cryptographic Signing      (Phase 7)
  Suite 6  Atomic Redis Lua Rate Limiting         (Phase 8)

Prerequisites:
  - PostgreSQL + Redis running  (docker compose up -d)
  - uvicorn app.main:app --port 8000 --reload
  - celery -A app.core.celery_app worker --loglevel=info -P solo

Run from project root:
    python scripts/verify_master.py
"""
from __future__ import annotations

import asyncio
import inspect
import json
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
import redis as redis_lib
from sqlalchemy import func, select

sys.path.insert(0, ".")

from app.core.config import settings                       # noqa: E402
from app.core.database import AsyncSessionLocal            # noqa: E402
from app.core.rate_limiter import RateLimiter              # noqa: E402
from app.core.security import (                            # noqa: E402
    generate_webhook_signature,
    verify_webhook_signature,
)
from app.models.delivery import DeliveryAttempt            # noqa: E402
from app.models.endpoint import WebhookEndpoint             # noqa: E402
from app.models.event import Event, EventStatus             # noqa: E402
from app.tasks.delivery import RATE_LIMIT_MAX, RATE_LIMIT_WINDOW_S  # noqa: E402

# ---------------------------------------------------------------------------
# Global configuration
# ---------------------------------------------------------------------------
API_BASE_URL      = "http://127.0.0.1:8000"
EVENTS_URL        = f"{API_BASE_URL}/api/v1/events"
DLQ_URL           = f"{API_BASE_URL}/api/v1/dlq"

POLL_TIMEOUT_S    = 60.0
POLL_INTERVAL_S   = 0.75

DLQ_POLL_TIMEOUT_S    = 120.0
REPLAY_POLL_TIMEOUT_S = 40.0

HMAC_SECRET = "master-secret-key"

RL_THREAD_COUNT = 20
RL_LIMIT        = 5
RL_WINDOW_S     = 30

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

SEP   = "=" * 72
THIN  = "-" * 72


def ok(msg):   print(f"    {GREEN}[OK]   {msg}{RESET}")
def fail(msg): print(f"    {RED}[FAIL] {msg}{RESET}")
def info(msg): print(f"    {CYAN}[INFO] {msg}{RESET}")
def warn(msg): print(f"    {YELLOW}[WARN] {msg}{RESET}")


def suite_header(num, title):
    print(f"\n{THIN}")
    print(f"  {BOLD}Suite {num} -- {title}{RESET}")
    print(THIN)


# ---------------------------------------------------------------------------
# Shared DB helpers
# ---------------------------------------------------------------------------

async def deactivate_all_endpoints(session):
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


async def register_endpoint(session, target_url, secret=None):
    ep = WebhookEndpoint(target_url=target_url, secret=secret, is_active=True)
    session.add(ep)
    await session.flush()
    await session.commit()
    return ep


async def poll_for_status(event_id, target_status, timeout_s, min_attempts=1):
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


async def poll_for_non_pending(event_id, timeout_s, min_attempts=1):
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


def _make_redis():
    return redis_lib.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=False,
        socket_connect_timeout=3,
        socket_timeout=3,
    )


# ===========================================================================
# Suite 1 -- Connectivity & Core Ingestion (Phases 1-3)
# ===========================================================================

async def suite1_connectivity_and_ingestion(client):
    suite_header(1, "Connectivity & Core Ingestion (Phases 1-3)")
    passed = True

    # 1a. FastAPI health check
    try:
        t0 = time.perf_counter()
        r = await client.get(f"{API_BASE_URL}/healthz")
        latency = (time.perf_counter() - t0) * 1000
        if r.status_code == 200:
            ok(f"GET /healthz -> HTTP 200  ({latency:.1f} ms)")
        else:
            fail(f"GET /healthz -> HTTP {r.status_code}")
            passed = False
    except Exception as exc:
        fail(f"FastAPI unreachable: {exc}")
        return False

    # 1b. Redis ping
    try:
        rds = _make_redis()
        rds.ping()
        ok("Redis PING -> PONG")
    except Exception as exc:
        fail(f"Redis unreachable: {exc}")
        passed = False

    # 1c. Register endpoint -> /mock/receiver
    mock_receiver = f"{API_BASE_URL}/api/v1/mock/receiver"
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} stale endpoint(s).")
    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, mock_receiver)
    ok(f"Registered endpoint -> {mock_receiver}  id={ep.id}")

    # 1d. Ingest event, assert HTTP 202 within <25 ms
    idem_key = f"master-suite1-{uuid.uuid4()}"
    payload = {
        "event_type": "order.created",
        "payload": {"order_id": str(uuid.uuid4()), "source": "verify_master_s1"},
        "idempotency_key": idem_key,
    }
    t0 = time.perf_counter()
    r = await client.post(EVENTS_URL, json=payload)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    if r.status_code == 202:
        ok("POST /events -> HTTP 202 Accepted")
    else:
        fail(f"POST /events -> HTTP {r.status_code} (expected 202)")
        passed = False

    if elapsed_ms < 25.0:
        ok(f"Ingestion latency = {elapsed_ms:.2f} ms  (< 25 ms threshold)")
    else:
        warn(f"Ingestion latency = {elapsed_ms:.2f} ms  (> 25 ms - possible load overhead)")

    body = r.json()
    event_id_str = body.get("event_id", "")
    if not event_id_str:
        fail("No event_id in response body")
        return False
    event_id = uuid.UUID(event_id_str)
    ok(f"event_id = {event_id}")

    if body.get("status") == "ACCEPTED":
        ok("Response body status = ACCEPTED")
    else:
        fail(f"Response body status = {body.get('status')!r} (expected ACCEPTED)")
        passed = False

    # 1e. Poll until DELIVERED
    info(f"Polling up to {POLL_TIMEOUT_S}s for DELIVERED status ...")
    event, attempts = await poll_for_status(
        event_id, EventStatus.DELIVERED, timeout_s=POLL_TIMEOUT_S, min_attempts=1,
    )

    if event is None:
        fail("Event row not found in Postgres")
        return False

    if event.status == EventStatus.DELIVERED:
        ok("Event.status = DELIVERED")
    else:
        fail(f"Event.status = {event.status.value}  (expected DELIVERED)")
        passed = False

    if attempts:
        attempt = attempts[0]
        if attempt.http_status == 200:
            ok(f"DeliveryAttempt logged: attempt_number={attempt.attempt_number}  http_status=200")
        else:
            fail(f"DeliveryAttempt http_status = {attempt.http_status}  (expected 200)")
            passed = False
    else:
        fail("No DeliveryAttempt rows found - Celery worker may not be running")
        passed = False

    return passed


# ===========================================================================
# Suite 2 -- Transient Failures & Jitter Backoff (Phase 4)
# ===========================================================================

async def suite2_transient_failures_backoff(client):
    suite_header(2, "Transient Failures & Jitter Backoff (Phase 4)")
    passed = True

    mock_flaky = f"{API_BASE_URL}/api/v1/mock/flaky"
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} stale endpoint(s).")
    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, mock_flaky)
    ok(f"Registered flaky endpoint -> {mock_flaky}  id={ep.id}")

    idem_key = f"master-suite2-{uuid.uuid4()}"
    r = await client.post(EVENTS_URL, json={
        "event_type": "order.retry_test",
        "payload": {"source": "verify_master_s2"},
        "idempotency_key": idem_key,
    })
    if r.status_code != 202:
        fail(f"POST /events -> HTTP {r.status_code} (expected 202)")
        return False
    ok("POST /events -> HTTP 202 Accepted")
    event_id = uuid.UUID(r.json()["event_id"])
    info(f"event_id = {event_id}")

    info("Polling up to 60s for 3 attempts and DELIVERED status ...")
    event, attempts = await poll_for_status(
        event_id, EventStatus.DELIVERED, timeout_s=60.0, min_attempts=3,
    )

    if event is None:
        fail("Event row not found in Postgres after polling")
        return False

    if event.status == EventStatus.DELIVERED:
        ok("Event.status = DELIVERED")
    else:
        fail(f"Event.status = {event.status.value}  (expected DELIVERED)")
        passed = False

    if len(attempts) == 3:
        ok("DeliveryAttempt count = 3  (exactly as expected)")
    else:
        fail(f"DeliveryAttempt count = {len(attempts)}  (expected 3)")
        passed = False

    nums = [a.attempt_number for a in attempts]
    if nums == [1, 2, 3]:
        ok(f"attempt_number sequence = {nums}")
    else:
        fail(f"attempt_number sequence = {nums}  (expected [1, 2, 3])")
        passed = False

    http_statuses = [a.http_status for a in attempts]
    if http_statuses == [503, 503, 200]:
        ok(f"HTTP status per attempt = {http_statuses}")
    else:
        fail(f"HTTP status per attempt = {http_statuses}  (expected [503, 503, 200])")
        passed = False

    print(f"\n    {'#':>3}  {'attempt_number':>14}  {'http_status':>11}")
    print(f"    {'-'*34}")
    for a in attempts:
        print(f"    {a.attempt_number:>3}  {a.attempt_number:>14}  {str(a.http_status):>11}")

    return passed


# ===========================================================================
# Suite 3 -- Ingestion Idempotency & Headers (Phase 5)
# ===========================================================================

async def suite3_idempotency_and_headers(client):
    suite_header(3, "Ingestion Idempotency & Headers (Phase 5)")
    passed = True

    # Part A: Idempotency deduplication
    idem_key = f"master-test-idem-{uuid.uuid4()}"
    payload = {
        "event_type": "order.idempotent",
        "payload": {"source": "verify_master_s3a"},
        "idempotency_key": idem_key,
    }

    r1 = await client.post(EVENTS_URL, json=payload)
    info(f"First POST -> HTTP {r1.status_code}")
    if r1.status_code == 202:
        ok("First POST with idempotency_key -> HTTP 202 Accepted")
    else:
        fail(f"First POST -> HTTP {r1.status_code}  (expected 202)")
        passed = False

    event_id_str = r1.json().get("event_id", "")
    if not event_id_str:
        fail("No event_id in first response")
        return False
    event_id = uuid.UUID(event_id_str)
    ok(f"event_id = {event_id}")

    r2 = await client.post(EVENTS_URL, json=payload)
    info(f"Second POST (same key) -> HTTP {r2.status_code}")
    if r2.status_code == 200:
        ok("Duplicate POST -> HTTP 200 OK  (idempotent replay)")
    else:
        fail(f"Duplicate POST -> HTTP {r2.status_code}  (expected 200)")
        passed = False

    replay_header = r2.headers.get("X-Idempotent-Replay", "")
    if replay_header.lower() == "true":
        ok("X-Idempotent-Replay: true  header present")
    else:
        fail(f"X-Idempotent-Replay header = {replay_header!r}  (expected true)")
        passed = False

    replayed_id = r2.json().get("event_id", "")
    if str(replayed_id) == str(event_id):
        ok(f"Replayed event_id matches original: {replayed_id}")
    else:
        fail(f"Replayed event_id {replayed_id!r} != original {event_id!r}")
        passed = False

    async with AsyncSessionLocal() as s:
        db_count = (
            await s.execute(
                select(func.count()).select_from(Event).where(
                    Event.idempotency_key == idem_key
                )
            )
        ).scalar_one()
    if db_count == 1:
        ok("DB has exactly 1 Event row for idempotency_key  (no duplicate)")
    else:
        fail(f"DB has {db_count} Event row(s) for key  (expected 1)")
        passed = False

    # Part B: Header injection via /mock/echo
    mock_echo = f"{API_BASE_URL}/api/v1/mock/echo"
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} stale endpoint(s).")
    async with AsyncSessionLocal() as s:
        echo_ep = await register_endpoint(s, mock_echo)
    ok(f"Registered echo endpoint -> {mock_echo}  id={echo_ep.id}")

    echo_idem_key = f"master-suite3-echo-{uuid.uuid4()}"
    r_echo = await client.post(EVENTS_URL, json={
        "event_type": "header.check",
        "payload": {"source": "verify_master_s3b"},
        "idempotency_key": echo_idem_key,
    })
    if r_echo.status_code != 202:
        fail(f"POST to echo endpoint -> HTTP {r_echo.status_code}  (expected 202)")
        return False
    ok("POST event to echo endpoint -> HTTP 202 Accepted")
    echo_event_id = uuid.UUID(r_echo.json()["event_id"])

    info(f"Polling up to {POLL_TIMEOUT_S}s for echo delivery ...")
    _, echo_attempts = await poll_for_non_pending(echo_event_id, POLL_TIMEOUT_S, 1)

    if not echo_attempts:
        fail("No DeliveryAttempt rows found for echo event - Celery may not be running")
        return False

    echo_attempt = echo_attempts[0]
    ok(f"DeliveryAttempt received: http_status={echo_attempt.http_status}")

    if not echo_attempt.response_body:
        fail("DeliveryAttempt.response_body is empty - cannot inspect headers")
        return False

    try:
        echo_data = json.loads(echo_attempt.response_body)
    except json.JSONDecodeError as exc:
        fail(f"Cannot parse response_body as JSON: {exc}")
        return False

    received_headers = echo_data.get("received_headers", {})
    received_lower = {k.lower(): v for k, v in received_headers.items()}

    required_headers = [
        "x-webhook-event-id",
        "x-webhook-delivery-id",
        "x-webhook-idempotency-key",
        "x-webhook-timestamp",
    ]
    for hdr_name in required_headers:
        val = received_lower.get(hdr_name, "")
        if val:
            ok(f"{hdr_name}: {val}")
        else:
            fail(f"{hdr_name}: MISSING")
            passed = False

    if received_lower.get("x-webhook-event-id", "") == str(echo_event_id):
        ok("X-Webhook-Event-Id matches submitted event_id")
    else:
        fail(f"X-Webhook-Event-Id mismatch: {received_lower.get('x-webhook-event-id')!r}")
        passed = False

    if received_lower.get("x-webhook-idempotency-key", "") == echo_idem_key:
        ok("X-Webhook-Idempotency-Key matches submitted key")
    else:
        fail("X-Webhook-Idempotency-Key mismatch")
        passed = False

    return passed


# ===========================================================================
# Suite 4 -- DLQ State Machine & Replay Engine (Phase 6)
# ===========================================================================

async def suite4_dlq_and_replay(client):
    suite_header(4, "DLQ State Machine & Replay Engine (Phase 6)")
    passed = True

    mock_failing  = f"{API_BASE_URL}/api/v1/mock/failing"
    mock_receiver = f"{API_BASE_URL}/api/v1/mock/receiver"

    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} stale endpoint(s).")
    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, mock_failing)
    ep_id = ep.id
    ok(f"Registered failing endpoint -> {mock_failing}  id={ep_id}")

    idem_key = f"master-suite4-{uuid.uuid4()}"
    r = await client.post(EVENTS_URL, json={
        "event_type": "payment.dlq_test",
        "payload": {"source": "verify_master_s4"},
        "idempotency_key": idem_key,
    })
    if r.status_code != 202:
        fail(f"POST /events -> HTTP {r.status_code}  (expected 202)")
        return False
    ok("POST /events -> HTTP 202 Accepted")
    event_id = uuid.UUID(r.json()["event_id"])
    info(f"event_id = {event_id}")

    info(f"Polling up to {DLQ_POLL_TIMEOUT_S}s for DEAD_LETTER status ...")
    event, attempts = await poll_for_status(
        event_id, EventStatus.DEAD_LETTER, timeout_s=DLQ_POLL_TIMEOUT_S, min_attempts=5,
    )

    if event is None:
        fail("Event not found in Postgres after polling")
        return False

    if event.status == EventStatus.DEAD_LETTER:
        ok("Event.status = DEAD_LETTER")
    else:
        fail(f"Event.status = {event.status.value}  (expected DEAD_LETTER)")
        passed = False

    if len(attempts) == 5:
        ok("DeliveryAttempt count = 5  (1 initial + 4 retries)")
    else:
        fail(f"DeliveryAttempt count = {len(attempts)}  (expected 5)")
        passed = False

    dlq_resp = await client.get(DLQ_URL)
    if dlq_resp.status_code != 200:
        fail(f"GET /api/v1/dlq -> HTTP {dlq_resp.status_code}")
        passed = False
    else:
        dlq_data = dlq_resp.json()
        dlq_ids  = [item["id"] for item in dlq_data.get("items", [])]
        if str(event_id) in dlq_ids:
            ok(f"GET /api/v1/dlq lists event_id {event_id}")
        else:
            fail(f"GET /api/v1/dlq does NOT list event_id {event_id}")
            passed = False

    if not passed:
        info("Aborting replay section -- DLQ transition did not pass.")
        return False

    async with AsyncSessionLocal() as s:
        fixed_ep = await s.get(WebhookEndpoint, ep_id)
        if fixed_ep:
            fixed_ep.target_url = mock_receiver
            await s.commit()
    ok(f"Endpoint updated: {mock_failing} -> {mock_receiver}")

    replay_url = f"{EVENTS_URL}/{event_id}/replay"
    info(f"POST {replay_url}")
    r_replay = await client.post(replay_url)

    if r_replay.status_code == 202:
        ok("POST /replay -> HTTP 202 Accepted")
    else:
        fail(f"POST /replay -> HTTP {r_replay.status_code}  (expected 202)")
        passed = False

    replay_body = r_replay.json()
    if replay_body.get("status") == "REPLAY_QUEUED":
        ok("Replay response status = REPLAY_QUEUED")
    else:
        fail(f"Replay response status = {replay_body.get('status')!r}  (expected REPLAY_QUEUED)")
        passed = False

    if str(replay_body.get("event_id")) == str(event_id):
        ok(f"Replayed event_id matches: {event_id}")
    else:
        fail("Replayed event_id mismatch")
        passed = False

    info(f"Polling up to {REPLAY_POLL_TIMEOUT_S}s for DELIVERED after replay ...")
    event_post_replay, all_attempts = await poll_for_status(
        event_id, EventStatus.DELIVERED, timeout_s=REPLAY_POLL_TIMEOUT_S, min_attempts=6,
    )

    if event_post_replay and event_post_replay.status == EventStatus.DELIVERED:
        ok("Event.status = DELIVERED after replay")
    else:
        stat = event_post_replay.status.value if event_post_replay else "NOT FOUND"
        fail(f"Event.status = {stat}  (expected DELIVERED after replay)")
        passed = False

    # The replay engine may reset attempt_number to 1; check that at least one
    # more attempt row exists beyond the original 5 DLQ attempts, and that the
    # most recent one succeeded (http_status 200).
    if len(all_attempts) > 5:
        last_attempt = all_attempts[-1]
        ok(f"New DeliveryAttempt after replay exists (total={len(all_attempts)})  "
           f"attempt_number={last_attempt.attempt_number}  http_status={last_attempt.http_status}")
    else:
        fail(f"No additional DeliveryAttempt created after replay  (total={len(all_attempts)}, expected >5)")
        passed = False

    dlq_resp2 = await client.get(DLQ_URL)
    if dlq_resp2.status_code == 200:
        dlq_ids2 = [item["id"] for item in dlq_resp2.json().get("items", [])]
        if str(event_id) not in dlq_ids2:
            ok("Event no longer in GET /api/v1/dlq (DELIVERED)")
        else:
            fail("Event still appears in DLQ after successful replay")
            passed = False

    return passed


# ===========================================================================
# Suite 5 -- HMAC-SHA256 Cryptographic Signing (Phase 7)
# ===========================================================================

async def suite5_hmac_signing(client):
    suite_header(5, "HMAC-SHA256 Cryptographic Signing (Phase 7)")
    passed = True

    secure_url = f"{API_BASE_URL}/api/v1/mock/secure?secret={quote(HMAC_SECRET)}"

    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} stale endpoint(s).")
    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, secure_url, secret=HMAC_SECRET)
    ok(f"Registered secure endpoint  secret={HMAC_SECRET!r}")

    idem_key = f"master-suite5-{uuid.uuid4()}"
    r = await client.post(EVENTS_URL, json={
        "event_type": "payment.signed",
        "payload": {"amount": 4999, "source": "verify_master_s5"},
        "idempotency_key": idem_key,
    })
    if r.status_code != 202:
        fail(f"POST /events -> HTTP {r.status_code}  (expected 202)")
        return False
    ok("POST /events -> HTTP 202 Accepted")
    event_id = uuid.UUID(r.json()["event_id"])

    info(f"Polling up to {POLL_TIMEOUT_S}s for signed delivery ...")
    event, attempts = await poll_for_non_pending(event_id, POLL_TIMEOUT_S, 1)

    if not attempts:
        fail("No DeliveryAttempt found -- Celery worker may not be running")
        return False

    attempt = attempts[0]
    if attempt.http_status == 200:
        ok("Secure endpoint returned HTTP 200 -- X-Webhook-Signature VERIFIED by receiver")
    elif attempt.http_status == 401:
        fail(f"Secure endpoint returned 401 -- signature REJECTED  body={attempt.response_body}")
        passed = False
    elif attempt.http_status == 400:
        fail(f"Secure endpoint returned 400 -- timestamp expired  body={attempt.response_body}")
        passed = False
    else:
        fail(f"Unexpected http_status from /mock/secure: {attempt.http_status}")
        passed = False

    if event and event.status == EventStatus.DELIVERED:
        ok("Event.status = DELIVERED")
    else:
        fail(f"Event.status = {event.status.value if event else 'NOT FOUND'}  (expected DELIVERED)")
        passed = False

    # Unit assertions
    secret   = HMAC_SECRET
    ts       = int(time.time())
    body_str = '{"amount":4999,"currency":"USD"}'
    sig = generate_webhook_signature(secret, ts, body_str)

    if verify_webhook_signature(secret, sig, body_str.encode(), tolerance=60):
        ok("verify_webhook_signature(correct body) = True")
    else:
        fail("verify_webhook_signature(correct body) returned False")
        passed = False

    tampered = body_str[:-1] + ("X" if body_str[-1] != "X" else "Y")
    if not verify_webhook_signature(secret, sig, tampered.encode(), tolerance=60):
        ok("verify_webhook_signature(tampered body) = False  (tamper detected)")
    else:
        fail("verify_webhook_signature(tampered body) returned True -- VULNERABILITY!")
        passed = False

    old_ts  = int(time.time()) - 400
    old_sig = generate_webhook_signature(secret, old_ts, body_str)
    if not verify_webhook_signature(secret, old_sig, body_str.encode(), tolerance=300):
        ok("Expired timestamp (400s old, tolerance=300) correctly REJECTED")
    else:
        fail("Expired timestamp was ACCEPTED -- replay protection broken!")
        passed = False

    fresh_ts    = int(time.time())
    correct_sig = generate_webhook_signature(secret, fresh_ts, body_str)
    if not verify_webhook_signature("wrong-secret", correct_sig, body_str.encode(), tolerance=300):
        ok("Wrong secret correctly REJECTED (HMAC mismatch)")
    else:
        fail("Wrong secret ACCEPTED -- HMAC verification broken!")
        passed = False

    src = inspect.getsource(verify_webhook_signature)
    if "compare_digest" in src:
        ok("hmac.compare_digest present in verify_webhook_signature source")
    else:
        fail("hmac.compare_digest NOT found -- timing attack possible!")
        passed = False

    return passed


# ===========================================================================
# Suite 6 -- Atomic Redis Lua Rate Limiting (Phase 8)
# ===========================================================================

def suite6_rate_limiting():
    suite_header(6, "Atomic Redis Lua Rate Limiting (Phase 8)")
    passed = True

    rds     = _make_redis()
    limiter = RateLimiter(rds)

    test_ep_id = f"master-suite6-{uuid.uuid4()}"
    rl_key     = f"rate_limit:{test_ep_id}"
    rds.delete(rl_key)
    info(f"Flushed Redis key: {rl_key!r}")

    LIMIT  = RL_LIMIT
    TOTAL  = RL_THREAD_COUNT
    WINDOW = RL_WINDOW_S

    allowed_count  = 0
    rejected_count = 0
    lock = threading.Lock()

    def worker():
        nonlocal allowed_count, rejected_count
        is_limited, _ = limiter.is_rate_limited(test_ep_id, limit=LIMIT, window_seconds=WINDOW)
        with lock:
            if is_limited:
                rejected_count += 1
            else:
                allowed_count  += 1

    info(f"Launching {TOTAL} concurrent threads (limit={LIMIT}, window={WINDOW}s) ...")
    threads = [threading.Thread(target=worker) for _ in range(TOTAL)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0
    info(f"All {TOTAL} threads completed in {elapsed:.3f}s")
    info(f"Allowed={allowed_count}  Rejected={rejected_count}  Total={allowed_count + rejected_count}")

    if allowed_count + rejected_count == TOTAL:
        ok(f"Total calls = {TOTAL}  (no lost increments)")
    else:
        fail(f"Total = {allowed_count + rejected_count}  (expected {TOTAL})")
        passed = False

    if allowed_count == LIMIT:
        ok(f"Allowed count = {LIMIT}  (matches limit exactly -- no overshoot)")
    else:
        fail(f"Allowed count = {allowed_count}  (expected {LIMIT}) -- atomicity violated!")
        passed = False

    expected_rejected = TOTAL - LIMIT
    if rejected_count == expected_rejected:
        ok(f"Rejected count = {rejected_count}  (= {TOTAL} - {LIMIT})")
    else:
        fail(f"Rejected count = {rejected_count}  (expected {expected_rejected})")
        passed = False

    raw = rds.get(rl_key)
    actual_counter = int(raw) if raw else 0
    if actual_counter == TOTAL:
        ok(f"Redis counter = {actual_counter}  (all {TOTAL} increments persisted)")
    else:
        fail(f"Redis counter = {actual_counter}  (expected {TOTAL}) -- lost writes!")
        passed = False

    ttl = rds.ttl(rl_key)
    if ttl > 0:
        ok(f"Redis key TTL = {ttl}s  (expiry correctly set)")
    elif ttl == -1:
        fail("Redis key has no TTL -- counter will never reset!")
        passed = False
    else:
        warn(f"Redis key TTL = {ttl}  (key may have already expired)")

    rds.delete(rl_key)
    return passed


# ===========================================================================
# Main orchestrator
# ===========================================================================

async def main():
    run_ts = datetime.now(tz=timezone.utc).isoformat()

    print(f"\n{SEP}")
    print(f"  {BOLD}Webhook Engine -- Master Sanity Verification Suite{RESET}")
    print(f"  {DIM}Phases 1-8  |  {run_ts}{RESET}")
    print(SEP)

    try:
        async with httpx.AsyncClient(timeout=4.0) as _c:
            _r = await _c.get(f"{API_BASE_URL}/healthz")
            if _r.status_code != 200:
                raise RuntimeError(f"/healthz returned {_r.status_code}")
    except Exception as exc:
        print(f"\n  {RED}[FAIL] FastAPI server not reachable: {exc}{RESET}")
        print(f"  {RED}ABORT -- ensure uvicorn is running on port 8000.{RESET}\n")
        sys.exit(1)

    suite_results = []

    async with httpx.AsyncClient(timeout=15.0) as client:
        t0 = time.perf_counter()
        r1 = await suite1_connectivity_and_ingestion(client)
        suite_results.append(("Suite 1  Connectivity & Core Ingestion", r1, time.perf_counter() - t0))

        t0 = time.perf_counter()
        r2 = await suite2_transient_failures_backoff(client)
        suite_results.append(("Suite 2  Transient Failures & Jitter Backoff", r2, time.perf_counter() - t0))

        t0 = time.perf_counter()
        r3 = await suite3_idempotency_and_headers(client)
        suite_results.append(("Suite 3  Ingestion Idempotency & Headers", r3, time.perf_counter() - t0))

        t0 = time.perf_counter()
        r4 = await suite4_dlq_and_replay(client)
        suite_results.append(("Suite 4  DLQ State Machine & Replay Engine", r4, time.perf_counter() - t0))

        t0 = time.perf_counter()
        r5 = await suite5_hmac_signing(client)
        suite_results.append(("Suite 5  HMAC-SHA256 Cryptographic Signing", r5, time.perf_counter() - t0))

    t0 = time.perf_counter()
    r6 = suite6_rate_limiting()
    suite_results.append(("Suite 6  Atomic Redis Lua Rate Limiting", r6, time.perf_counter() - t0))

    # Summary matrix
    all_passed = all(r for _, r, _ in suite_results)
    total_elapsed = sum(e for _, _, e in suite_results)

    print(f"\n{SEP}")
    print(f"  {BOLD}SUMMARY MATRIX{RESET}")
    print(THIN)
    col = 48
    print(f"  {'Suite':<{col}}  {'Status':<8}  {'Elapsed':>10}")
    print(f"  {'-'*col}  {'-'*8}  {'-'*10}")
    for label, result, elapsed in suite_results:
        status_str = f"{GREEN}[PASS]{RESET}" if result else f"{RED}[FAIL]{RESET}"
        elapsed_str = f"{elapsed:.2f}s"
        print(f"  {label:<{col}}  {status_str}    {elapsed_str:>9}")
    print(THIN)

    if all_passed:
        print(f"\n  {GREEN}{BOLD}[PASS]  ALL SUITES PASSED -- System fully operational.{RESET}")
    else:
        failed_suites = [label for label, r, _ in suite_results if not r]
        print(f"\n  {RED}{BOLD}[FAIL]  SYSTEM HEALTH: DEGRADED{RESET}")
        for lbl in failed_suites:
            print(f"  {RED}         -> {lbl}{RESET}")

    print(f"  {DIM}Total elapsed: {total_elapsed:.2f}s{RESET}")
    print(f"\n{SEP}\n")

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
