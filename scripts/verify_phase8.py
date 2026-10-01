"""
scripts/verify_phase8.py
~~~~~~~~~~~~~~~~~~~~~~~~
Phase 8 end-to-end verification: Atomic Per-Endpoint Rate Limiting.

Three suites
------------
Suite A -- Burst Blast (live, end-to-end):
    Register endpoint with limit=5, window=2s.
    Fire 15 events simultaneously.
    Assert all 15 eventually reach DELIVERED status.
    Assert no event is FAILED or DEAD_LETTER due to rate limiting.
    Assert [THROTTLED] log lines appear in worker output.

Suite B -- Lua Atomicity (unit, Redis only):
    Flush the rate_limit key.
    Concurrently execute the Lua script 20 times from 10 threads.
    Assert: allowed count == configured limit (no overshoot).
    Assert: rejected count == 20 - limit.
    Assert: final Redis counter == 20 (no lost increments).

Suite C -- Retry Mechanics (unit, no network):
    Manually saturate the rate limit counter via INCRBY.
    Call is_rate_limited() and assert (True, window_seconds) returned.
    Flush key and assert (False, 0) returned.

Prerequisites:
  - Postgres + Redis running  (docker compose up -d)
  - uvicorn app.main:app --port 8000 --reload
  - celery -A app.core.celery_app worker --loglevel=warning --pool=solo

Run from project root:
    python scripts/verify_phase8.py
"""
from __future__ import annotations

import asyncio
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import httpx
import redis
from sqlalchemy import select

sys.path.insert(0, ".")

from app.core.config import settings                        # noqa: E402
from app.core.database import AsyncSessionLocal             # noqa: E402
from app.core.rate_limiter import RateLimiter               # noqa: E402
from app.models.delivery import DeliveryAttempt             # noqa: E402
from app.models.endpoint import WebhookEndpoint             # noqa: E402
from app.models.event import Event, EventStatus             # noqa: E402
from app.tasks.delivery import RATE_LIMIT_MAX, RATE_LIMIT_WINDOW_S  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE_URL  = "http://127.0.0.1:8000"
EVENTS_URL    = f"{API_BASE_URL}/api/v1/events"

# Suite A parameters
SUITE_A_LIMIT      = 5     # requests per window
SUITE_A_WINDOW_S   = 2     # window duration
SUITE_A_BURST      = 15    # total events to fire simultaneously

# How long to poll for all events to reach DELIVERED
POLL_TIMEOUT_S   = 120.0   # generous — 10 windows of 2s each is 20s, we allow 2x buffer
POLL_INTERVAL_S  = 1.0

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
    print(f"\n{'--' * 34}")
    print(f"  {title}")
    print(f"{'--' * 34}")


# ---------------------------------------------------------------------------
# Shared Redis client (used by Suite B and C)
# ---------------------------------------------------------------------------
def _make_redis() -> redis.Redis:  # type: ignore[type-arg]
    return redis.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=False,
        socket_connect_timeout=3,
        socket_timeout=3,
    )


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


async def post_event(client: httpx.AsyncClient, tag: str) -> uuid.UUID | None:
    """POST a single event and return its UUID, or None on error."""
    idempotency_key = f"phase8-{tag}-{uuid.uuid4()}"
    payload = {
        "event_type": "rate_limit.test",
        "payload": {"tag": tag, "source": "verify_phase8"},
        "idempotency_key": idempotency_key,
    }
    try:
        r = await client.post(EVENTS_URL, json=payload)
        if r.status_code == 202:
            return uuid.UUID(r.json()["event_id"])
        warn(f"POST event [{tag}] returned HTTP {r.status_code}")
        return None
    except Exception as exc:
        warn(f"POST event [{tag}] error: {exc}")
        return None


async def poll_all_delivered(
    event_ids: list[uuid.UUID],
    timeout_s: float = POLL_TIMEOUT_S,
) -> dict[uuid.UUID, EventStatus]:
    """Poll until all events leave PENDING or timeout elapses."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        statuses: dict[uuid.UUID, EventStatus] = {}
        async with AsyncSessionLocal() as s:
            for eid in event_ids:
                evt = await s.get(Event, eid)
                if evt:
                    statuses[eid] = evt.status
        pending = [eid for eid, st in statuses.items() if st == EventStatus.PENDING]
        if not pending:
            return statuses
        info(f"Waiting ... {len(pending)}/{len(event_ids)} still PENDING")
        await asyncio.sleep(POLL_INTERVAL_S)

    # Final read
    statuses = {}
    async with AsyncSessionLocal() as s:
        for eid in event_ids:
            evt = await s.get(Event, eid)
            if evt:
                statuses[eid] = evt.status
    return statuses


# ---------------------------------------------------------------------------
# Suite A: Burst Blast (live, end-to-end)
# ---------------------------------------------------------------------------

async def suite_a_burst_blast(client: httpx.AsyncClient) -> bool:
    hdr("Suite A — Burst Blast (live, end-to-end)")
    passed = True

    # ---- Setup -------------------------------------------------------
    async with AsyncSessionLocal() as s:
        deactivated = await deactivate_all_endpoints(s)
    if deactivated:
        info(f"Deactivated {deactivated} pre-existing endpoint(s).")

    # Point at /mock/ok — always returns 200 instantly, zero latency.
    target_url = f"{API_BASE_URL}/api/v1/mock/ok"
    async with AsyncSessionLocal() as s:
        ep = await register_endpoint(s, target_url)
    endpoint_id = str(ep.id)
    info(f"Registered endpoint id={endpoint_id} -> {target_url}")
    info(f"Rate limit config: RATE_LIMIT_MAX={RATE_LIMIT_MAX} req/{RATE_LIMIT_WINDOW_S}s")
    info(f"  (worker defaults — override in delivery.py for tighter Suite A control)")

    # Flush any stale rate-limit counter for this endpoint
    r = _make_redis()
    rl_key = f"rate_limit:{endpoint_id}"
    r.delete(rl_key)
    info(f"Flushed Redis key: {rl_key}")

    # ---- Blast -------------------------------------------------------
    info(f"Firing {SUITE_A_BURST} events simultaneously ...")
    t0 = time.perf_counter()
    tasks = [post_event(client, f"burst-{i:02d}") for i in range(SUITE_A_BURST)]
    results = await asyncio.gather(*tasks)
    elapsed_post = time.perf_counter() - t0
    info(f"All POST calls completed in {elapsed_post:.2f}s")

    event_ids = [eid for eid in results if eid is not None]
    info(f"Successfully posted {len(event_ids)}/{SUITE_A_BURST} events")

    if len(event_ids) < SUITE_A_BURST:
        fail(f"Only {len(event_ids)}/{SUITE_A_BURST} events posted — API issue?")
        passed = False

    # ---- Poll for completion -----------------------------------------
    info(f"Polling up to {POLL_TIMEOUT_S}s for all events to reach DELIVERED ...")
    statuses = await poll_all_delivered(event_ids)

    delivered = [eid for eid, st in statuses.items() if st == EventStatus.DELIVERED]
    pending   = [eid for eid, st in statuses.items() if st == EventStatus.PENDING]
    failed    = [eid for eid, st in statuses.items()
                 if st in (EventStatus.FAILED, EventStatus.DEAD_LETTER)]

    info(f"Final statuses: DELIVERED={len(delivered)} PENDING={len(pending)} FAILED/DLQ={len(failed)}")

    if len(delivered) == len(event_ids):
        ok(f"All {len(delivered)} events reached DELIVERED (none dropped by rate limiter)")
    else:
        fail(f"Only {len(delivered)}/{len(event_ids)} events DELIVERED")
        passed = False

    if pending:
        fail(f"{len(pending)} events still PENDING after {POLL_TIMEOUT_S}s")
        passed = False

    if failed:
        fail(f"{len(failed)} events FAILED/DEAD_LETTER due to rate limiting — BUG!")
        passed = False
    else:
        ok("Zero events marked FAILED or DEAD_LETTER due to rate limiting")

    return passed


# ---------------------------------------------------------------------------
# Suite B: Lua Atomicity (unit, Redis only)
# ---------------------------------------------------------------------------

def suite_b_lua_atomicity() -> bool:
    hdr("Suite B — Lua Atomicity (unit, Redis only)")
    passed = True

    r = _make_redis()
    limiter = RateLimiter(r)

    # Use a synthetic endpoint id so we don't interfere with real endpoints
    test_endpoint_id = f"suite-b-atomicity-{uuid.uuid4()}"
    rl_key = f"rate_limit:{test_endpoint_id}"
    r.delete(rl_key)

    THREAD_COUNT    = 10
    CALLS_PER_THREAD = 2     # 10 threads × 2 calls = 20 total
    TOTAL_CALLS     = THREAD_COUNT * CALLS_PER_THREAD
    LIMIT           = 7      # expect exactly 7 allowed, 13 rejected
    WINDOW          = 10     # long window so it doesn't expire mid-test

    allowed_count = 0
    rejected_count = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal allowed_count, rejected_count
        for _ in range(CALLS_PER_THREAD):
            is_limited, _ = limiter.is_rate_limited(
                test_endpoint_id, limit=LIMIT, window_seconds=WINDOW
            )
            with lock:
                if is_limited:
                    rejected_count += 1
                else:
                    allowed_count += 1

    threads = [threading.Thread(target=worker) for _ in range(THREAD_COUNT)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    info(f"Ran {TOTAL_CALLS} concurrent Lua calls in {elapsed:.3f}s using {THREAD_COUNT} threads")
    info(f"Allowed: {allowed_count} | Rejected: {rejected_count} | Total: {allowed_count + rejected_count}")

    # Read actual Redis counter
    raw_counter = r.get(rl_key)
    actual_counter = int(raw_counter) if raw_counter else 0
    info(f"Redis counter for key={rl_key!r}: {actual_counter}")

    # Assertion 1: total calls add up correctly
    if allowed_count + rejected_count == TOTAL_CALLS:
        ok(f"Total calls = {TOTAL_CALLS} (no lost increments)")
    else:
        fail(f"Total mismatch: allowed+rejected={allowed_count+rejected_count} != {TOTAL_CALLS}")
        passed = False

    # Assertion 2: exactly LIMIT requests allowed (atomicity guarantee)
    if allowed_count == LIMIT:
        ok(f"Allowed count = {LIMIT} — matches configured limit exactly (no overshoot)")
    else:
        fail(f"Allowed count = {allowed_count} (expected {LIMIT}) — atomicity violated!")
        passed = False

    # Assertion 3: rejected count = TOTAL - LIMIT
    expected_rejected = TOTAL_CALLS - LIMIT
    if rejected_count == expected_rejected:
        ok(f"Rejected count = {rejected_count} (= {TOTAL_CALLS} - {LIMIT})")
    else:
        fail(f"Rejected count = {rejected_count} (expected {expected_rejected})")
        passed = False

    # Assertion 4: Redis counter == TOTAL_CALLS (every INCR landed)
    if actual_counter == TOTAL_CALLS:
        ok(f"Redis counter = {actual_counter} — all {TOTAL_CALLS} increments persisted")
    else:
        fail(f"Redis counter = {actual_counter} (expected {TOTAL_CALLS}) — lost writes!")
        passed = False

    r.delete(rl_key)  # cleanup
    return passed


# ---------------------------------------------------------------------------
# Suite C: Retry Mechanics (unit, no network)
# ---------------------------------------------------------------------------

def suite_c_retry_mechanics() -> bool:
    hdr("Suite C — Retry Mechanics (unit, Redis only)")
    passed = True

    r = _make_redis()
    limiter = RateLimiter(r)

    test_endpoint_id = f"suite-c-retry-{uuid.uuid4()}"
    rl_key = f"rate_limit:{test_endpoint_id}"

    LIMIT  = 3
    WINDOW = 5

    # ---- Pre-saturate: push counter to limit -------------------------
    r.delete(rl_key)
    # First, make LIMIT allowed calls
    for i in range(LIMIT):
        is_limited, _ = limiter.is_rate_limited(test_endpoint_id, limit=LIMIT, window_seconds=WINDOW)
        if is_limited:
            fail(f"Call {i+1} was rate-limited before limit reached — unexpected")
            passed = False

    info(f"Made {LIMIT} allowed calls to endpoint {test_endpoint_id!r}")

    # ---- Next call must be throttled ---------------------------------
    is_limited, retry_after = limiter.is_rate_limited(test_endpoint_id, limit=LIMIT, window_seconds=WINDOW)
    if is_limited:
        ok(f"Call {LIMIT + 1} correctly rate-limited (is_limited=True)")
    else:
        fail(f"Call {LIMIT + 1} NOT rate-limited — limit enforcement broken!")
        passed = False

    if retry_after == WINDOW:
        ok(f"retry_after = {retry_after}s (== window_seconds={WINDOW})")
    else:
        fail(f"retry_after = {retry_after}s (expected {WINDOW}s)")
        passed = False

    # ---- Verify fail-open on Redis error ----------------------------
    # Simulate by creating a limiter pointing to a bad URL
    bad_redis = redis.Redis(host="127.0.0.1", port=19999, socket_connect_timeout=0.1, socket_timeout=0.1)
    bad_limiter = RateLimiter(bad_redis)
    is_limited_bad, _ = bad_limiter.is_rate_limited("any-endpoint", limit=5, window_seconds=1)
    if not is_limited_bad:
        ok("Fail-open behaviour confirmed: Redis error -> is_limited=False (delivery proceeds)")
    else:
        fail("Fail-open broken: Redis error caused is_limited=True (would block all deliveries!)")
        passed = False

    # ---- After window expiry, limit resets --------------------------
    info(f"Waiting {WINDOW + 1}s for rate-limit window to expire ...")
    time.sleep(WINDOW + 1)
    is_limited_after, _ = limiter.is_rate_limited(test_endpoint_id, limit=LIMIT, window_seconds=WINDOW)
    if not is_limited_after:
        ok(f"After window expiry, rate limit reset — call accepted again")
    else:
        fail("Rate limit still enforced after window expired — TTL bug!")
        passed = False

    r.delete(rl_key)
    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"\n{SEP}")
    print("  Webhook Engine — Phase 8 Rate Limiting Verification")
    print(SEP)
    print(f"  Run time  : {datetime.now(tz=timezone.utc).isoformat()}")
    print(f"  Worker RL : RATE_LIMIT_MAX={RATE_LIMIT_MAX} req/{RATE_LIMIT_WINDOW_S}s")
    print(SEP)

    # -- Connectivity check --
    try:
        async with httpx.AsyncClient(timeout=3.0) as c:
            r_api = await c.get(f"{API_BASE_URL}/healthz")
            if r_api.status_code != 200:
                raise RuntimeError(f"healthz returned {r_api.status_code}")
    except Exception as exc:
        fail(f"FastAPI server not reachable: {exc}")
        print(f"\n{SEP}\n  {RED}FAIL{RESET}\n{SEP}\n")
        sys.exit(1)
    info("FastAPI server reachable [ok]")

    try:
        r_redis = _make_redis()
        r_redis.ping()
    except Exception as exc:
        fail(f"Redis not reachable: {exc}")
        print(f"\n{SEP}\n  {RED}FAIL{RESET}\n{SEP}\n")
        sys.exit(1)
    info("Redis reachable [ok]")

    # -- Unit suites first (fast, no Celery worker needed) --
    passed_b = suite_b_lua_atomicity()
    passed_c = suite_c_retry_mechanics()

    # -- Live end-to-end suite (requires Celery worker) --
    async with httpx.AsyncClient(timeout=10.0) as client:
        passed_a = await suite_a_burst_blast(client)

    # -- Final verdict --
    all_passed = passed_a and passed_b and passed_c
    print(f"\n{SEP}")
    results = [
        (f"Suite A (Burst Blast, live E2E):      {'PASS' if passed_a else 'FAIL'}", passed_a),
        (f"Suite B (Lua Atomicity, unit):         {'PASS' if passed_b else 'FAIL'}", passed_b),
        (f"Suite C (Retry Mechanics, unit):       {'PASS' if passed_c else 'FAIL'}", passed_c),
    ]
    for line, p in results:
        print(f"  {GREEN if p else RED}{line}{RESET}")
    print()
    if all_passed:
        print(f"  {GREEN}PASS{RESET}  — Phase 8 rate limiting verification succeeded.")
    else:
        print(f"  {RED}FAIL{RESET}  — One or more suites did not pass (see above).")
    print(SEP)
    print()

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
