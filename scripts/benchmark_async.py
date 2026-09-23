"""
scripts/benchmark_async.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Phase 3 benchmark -- measures the throughput improvement of async, off-band
webhook delivery via Celery + Redis.

What it does
------------
1. Asserts the FastAPI server is reachable at http://localhost:8000.
2. Registers 2 endpoints pointing at the local mock receiver.
3. Dispatches 10 sequential events to POST /api/v1/events.
4. Measures API response latency per request (target: < 20 ms).
5. Polls the database to confirm the Celery worker created DeliveryAttempt
   rows in the background (polls every 0.5 s, up to 30 s per event batch).
6. Prints a side-by-side comparison table against Phase 2 numbers.

Prerequisites
-------------
- Docker services running: ``docker compose up``
- FastAPI server running:  ``uvicorn app.main:app --reload``
- Celery worker running:   ``celery -A app.core.celery_app worker --loglevel=info -P solo``

Run from project root::

    python scripts/benchmark_async.py

Expected output
---------------
* Each API call returns 202 in < 20 ms (vs ~3000 ms in Phase 2).
* Total wall-clock for 10 events < 1 s (vs ~30 s in Phase 2).
* DeliveryAttempt rows appear in DB within ~3-5 s of dispatch.
"""
from __future__ import annotations

import statistics
import sys
import time
import uuid

sys.path.insert(0, ".")

import httpx
import psycopg2  # sync driver -- no asyncio needed in this script

from app.core.config import settings

BASE_URL = "http://localhost:8000"
MOCK_RECEIVER = f"{BASE_URL}/api/v1/mock/receiver"
NUM_ENDPOINTS = 2
NUM_EVENTS = 10
DB_POLL_INTERVAL_S = 0.5
DB_POLL_TIMEOUT_S = 30.0

# ---------------------------------------------------------------------------
# Phase 2 reference numbers (from benchmark_sync.py run with 2 x 1.5 s receivers)
# ---------------------------------------------------------------------------
PHASE2_AVG_LATENCY_S = 3.0      # >= 3 s per event (2 endpoints x 1.5 s)
PHASE2_TOTAL_S = 30.0           # >= 30 s total for 10 events
PHASE2_THROUGHPUT = 0.33        # ~0.33 events/s


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_sync_db_conn():
    """Open a synchronous psycopg2 connection from the settings DATABASE_URL."""
    raw_url: str = settings.DATABASE_URL
    # Strip async driver specifier so psycopg2 can parse it.
    dsn = raw_url.replace("postgresql+asyncpg://", "postgresql://").replace(
        "postgresql+psycopg2://", "postgresql://"
    )
    return psycopg2.connect(dsn)


def count_delivery_attempts(conn, event_ids: list[str]) -> int:
    """Return how many DeliveryAttempt rows exist for the given event IDs."""
    with conn.cursor() as cur:
        placeholders = ",".join(["%s::uuid"] * len(event_ids))
        cur.execute(
            f"SELECT COUNT(*) FROM delivery_attempt WHERE event_id IN ({placeholders})",
            event_ids,
        )
        row = cur.fetchone()
        return row[0] if row else 0


def get_event_statuses(conn, event_ids: list[str]) -> dict[str, str]:
    """Return a mapping of event_id -> status for the given event IDs."""
    with conn.cursor() as cur:
        placeholders = ",".join(["%s::uuid"] * len(event_ids))
        cur.execute(
            f"SELECT id::text, status FROM event WHERE id IN ({placeholders})",
            event_ids,
        )
        return {row[0]: row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Benchmark steps
# ---------------------------------------------------------------------------

async def register_endpoints(client: httpx.AsyncClient) -> list[dict]:
    """Register NUM_ENDPOINTS mock receivers and return their response bodies."""
    endpoints = []
    for i in range(NUM_ENDPOINTS):
        resp = await client.post(
            f"{BASE_URL}/api/v1/endpoints",
            json={"target_url": MOCK_RECEIVER},
        )
        resp.raise_for_status()
        endpoints.append(resp.json())
        print(f"  [+] Registered endpoint {i + 1}: {endpoints[-1]['id']}")
    return endpoints


async def dispatch_events(client: httpx.AsyncClient) -> tuple[list[str], list[float]]:
    """
    Dispatch NUM_EVENTS events sequentially.

    Returns
    -------
    event_ids : list[str]
        UUIDs of the created events (for DB polling).
    latencies : list[float]
        Wall-clock seconds per API call.
    """
    event_ids: list[str] = []
    latencies: list[float] = []

    print(f"\n{'-' * 62}")
    print(f"  {'#':>3}   {'Latency (ms)':>14}   {'HTTP':>6}   Event ID")
    print("-" * 62)

    for i in range(NUM_EVENTS):
        idempotency_key = f"bench3-{uuid.uuid4()}"
        payload = {"event_type": "bench.async", "order_id": i}

        t0 = time.perf_counter()
        resp = await client.post(
            f"{BASE_URL}/api/v1/events",
            json={
                "event_type": "bench.async",
                "payload": payload,
                "idempotency_key": idempotency_key,
            },
            timeout=10.0,
        )
        elapsed_s = time.perf_counter() - t0
        latencies.append(elapsed_s)

        resp.raise_for_status()
        data = resp.json()
        event_id = data["event_id"]
        event_ids.append(event_id)

        latency_ms = elapsed_s * 1000
        flag = " OK" if latency_ms < 20 else " !"
        print(
            f"  {i + 1:>3}   {latency_ms:>12.1f} ms   "
            f"{resp.status_code:>6}   {event_id[:8]}...{flag}"
        )

    return event_ids, latencies


def poll_for_delivery(event_ids: list[str]) -> tuple[int, float]:
    """
    Poll the DB until all expected DeliveryAttempt rows are created
    (or timeout). Each event x each endpoint = one attempt row.

    Reconnects psycopg2 on Windows TCP connection aborts (10053) which
    can happen when Postgres is busy with concurrent writes.

    Returns
    -------
    total_attempts : int
    elapsed_s      : float   -- seconds waited
    """
    expected = len(event_ids) * NUM_ENDPOINTS
    print(f"\n  Polling DB for {expected} DeliveryAttempt rows "
          f"(timeout={DB_POLL_TIMEOUT_S:.0f} s)...")

    # Give the worker a head-start before first poll
    time.sleep(2.0)

    def fresh_conn():
        c = get_sync_db_conn()
        c.autocommit = True
        return c

    conn = fresh_conn()
    t0 = time.perf_counter()
    last_count = -1

    while True:
        elapsed = time.perf_counter() - t0
        try:
            count = count_delivery_attempts(conn, event_ids)
        except Exception as db_err:
            # Reconnect on any connection error (Windows 10053, etc.)
            print(f"    [{elapsed:5.1f} s]  DB poll error ({type(db_err).__name__}), reconnecting...")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(1.0)
            try:
                conn = fresh_conn()
            except Exception:
                pass
            continue

        if count != last_count:
            print(f"    [{elapsed:5.1f} s]  DeliveryAttempts found: {count}/{expected}")
            last_count = count

        if count >= expected:
            try:
                conn.close()
            except Exception:
                pass
            return count, elapsed

        if elapsed >= DB_POLL_TIMEOUT_S:
            try:
                statuses = get_event_statuses(conn, event_ids)
                conn.close()
            except Exception:
                statuses = {}
            print(f"\n  [WARN] Timeout after {elapsed:.1f} s -- "
                  f"only {count}/{expected} attempts recorded.")
            print(f"  Event statuses: {statuses}")
            return count, elapsed

        time.sleep(DB_POLL_INTERVAL_S)


def print_summary(latencies: list[float], attempts: int, poll_elapsed: float) -> None:
    """Print the benchmark summary and Phase 2 vs Phase 3 comparison table."""
    total = sum(latencies)
    avg = statistics.mean(latencies)
    p95 = sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)]
    throughput = len(latencies) / total

    avg_ms = avg * 1000
    p95_ms = p95 * 1000

    expected_attempts = NUM_EVENTS * NUM_ENDPOINTS
    delivery_ok = attempts >= expected_attempts

    print("\n" + "=" * 68)
    print("  PHASE 3 BENCHMARK SUMMARY -- Async Delivery (Celery + Redis)")
    print("=" * 68)
    print(f"  Endpoints registered  : {NUM_ENDPOINTS}")
    print(f"  Events dispatched     : {NUM_EVENTS}")
    print(f"  Total API wall-clock  : {total:>8.3f} s")
    print(f"  Average API latency   : {avg_ms:>8.1f} ms")
    print(f"  p95 API latency       : {p95_ms:>8.1f} ms")
    print(f"  API throughput        : {throughput:>8.1f} events/s")
    print(f"  DeliveryAttempts in DB: {attempts}/{expected_attempts} "
          f"({'OK ALL' if delivery_ok else 'FAIL INCOMPLETE'})")
    print(f"  Background work time  : {poll_elapsed:>8.1f} s")

    print("\n" + "=" * 68)
    print("  PHASE 2 vs PHASE 3 COMPARISON")
    print("=" * 68)
    header = f"  {'Metric':<30} {'Phase 2 (Sync)':>16} {'Phase 3 (Async)':>16}"
    print(header)
    print("  " + "-" * 64)

    rows = [
        ("Avg API latency",      f"~{PHASE2_AVG_LATENCY_S * 1000:.0f} ms",    f"{avg_ms:.1f} ms"),
        ("p95 API latency",      f"~{PHASE2_AVG_LATENCY_S * 1000:.0f} ms",    f"{p95_ms:.1f} ms"),
        ("Total (10 events)",    f"~{PHASE2_TOTAL_S:.0f} s",                   f"{total:.3f} s"),
        ("API throughput",       f"~{PHASE2_THROUGHPUT:.2f} events/s",         f"{throughput:.1f} events/s"),
        ("Worker blocked?",      "Yes (sync HTTP)",                            "No (async task)"),
        ("Horizontally scalable?","No (per-worker)",                           "Yes (add workers)"),
        ("Delivery verification","In-response body",                           "DB poll / Flower"),
    ]

    for label, p2, p3 in rows:
        print(f"  {label:<30} {p2:>16} {p3:>16}")

    print("=" * 68)

    # Speedup factor
    if avg_ms > 0:
        speedup = (PHASE2_AVG_LATENCY_S * 1000) / avg_ms
        print(f"\n  ?  API latency improved by ~{speedup:.0f}x  "
              f"({PHASE2_AVG_LATENCY_S*1000:.0f} ms -> {avg_ms:.1f} ms)")

    latency_ok = avg_ms < 20
    print(f"\n  Latency SLA (< 20 ms avg): {'OK PASS' if latency_ok else 'FAIL FAIL'}")
    print(f"  Background delivery:       {'OK PASS' if delivery_ok else 'FAIL INCOMPLETE (worker may still be running)'}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

import asyncio


async def main() -> None:
    print("=" * 68)
    print("  Webhook Engine -- Phase 3 Async Benchmark")
    print("=" * 68)

    async with httpx.AsyncClient() as client:
        # -- Health check --------------------------------------------------
        try:
            health = await client.get(f"{BASE_URL}/healthz", timeout=3.0)
            health.raise_for_status()
            print(f"\n  [OK] Server healthy: {health.json()}")
        except Exception as exc:
            print(f"\n  [ERROR] Cannot reach server at {BASE_URL}: {exc}")
            print("  Start it with: uvicorn app.main:app --reload")
            sys.exit(1)

        # -- Register endpoints --------------------------------------------
        print(f"\n  Registering {NUM_ENDPOINTS} mock receiver(s)...")
        await register_endpoints(client)

        # -- Dispatch events -----------------------------------------------
        print(f"\n  Dispatching {NUM_EVENTS} events (expecting 202 Accepted < 20 ms each)...\n")
        event_ids, latencies = await dispatch_events(client)

    # -- Poll DB for background delivery -----------------------------------
    attempts, poll_elapsed = poll_for_delivery(event_ids)

    # -- Summary -----------------------------------------------------------
    print_summary(latencies, attempts, poll_elapsed)


if __name__ == "__main__":
    asyncio.run(main())
