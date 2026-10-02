"""
scripts/load_test.py
~~~~~~~~~~~~~~~~~~~~
Phase 9 -- High-Concurrency Load Test

Concurrently dispatches 200 unique events to POST /api/v1/events via
asyncio + httpx, measures ingestion latency percentiles, then polls
PostgreSQL until every event reaches DELIVERED status and reports
end-to-end throughput.

Usage (from project root)::

    .venv\\Scripts\\python.exe scripts/load_test.py

Prerequisites:
    uvicorn app.main:app --port 8000 --reload
    celery -A app.core.celery_app worker --loglevel=info -P solo
    PostgreSQL + Redis (docker compose up -d)
"""
from __future__ import annotations

import asyncio
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy import select, func

sys.path.insert(0, ".")

from app.core.database import AsyncSessionLocal   # noqa: E402
from app.models.event import Event, EventStatus   # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE      = "http://127.0.0.1:8000"
EVENTS_URL    = f"{API_BASE}/api/v1/events"

NUM_EVENTS    = 200       # total events to ingest
CONCURRENCY   = 10        # asyncio worker count (semaphore slots)

DRAIN_TIMEOUT = 120.0     # seconds to wait for all events to drain
DRAIN_POLL_S  = 1.0       # DB polling interval during drain phase

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------
G  = "\033[92m"
R  = "\033[91m"
C  = "\033[96m"
Y  = "\033[93m"
B  = "\033[1m"
D  = "\033[2m"
RS = "\033[0m"

SEP  = "=" * 68
THIN = "-" * 68


def _p(label: str, value: str, unit: str = "") -> None:
    print(f"  {D}{label:<36}{RS}  {B}{value}{RS}{unit}")


# ---------------------------------------------------------------------------
# Ingestion worker
# ---------------------------------------------------------------------------

async def _ingest_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    idx: int,
) -> dict:
    """POST a single event; return latency and result metadata."""
    idem_key  = f"load-test-{uuid.uuid4()}"
    payload   = {
        "event_type":     "load.test.event",
        "payload":        {"index": idx, "batch": "phase9-load-test"},
        "idempotency_key": idem_key,
    }

    async with sem:
        t0 = time.perf_counter()
        try:
            resp = await client.post(EVENTS_URL, json=payload)
            latency_ms = (time.perf_counter() - t0) * 1000
            return {
                "idx":        idx,
                "status":     resp.status_code,
                "latency_ms": latency_ms,
                "event_id":   resp.json().get("event_id") if resp.status_code == 202 else None,
                "ok":         resp.status_code == 202,
            }
        except Exception as exc:
            latency_ms = (time.perf_counter() - t0) * 1000
            return {
                "idx":        idx,
                "status":     0,
                "latency_ms": latency_ms,
                "event_id":   None,
                "ok":         False,
                "error":      str(exc),
            }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    run_ts = datetime.now(tz=timezone.utc).isoformat()
    print(f"\n{SEP}")
    print(f"  {B}Webhook Engine — Phase 9 Load Test{RS}")
    print(f"  {D}{NUM_EVENTS} events  |  concurrency={CONCURRENCY}  |  {run_ts}{RS}")
    print(SEP)

    # Preflight: ensure server is up
    try:
        async with httpx.AsyncClient(timeout=3.0) as hc:
            r = await hc.get(f"{API_BASE}/healthz")
            if r.status_code != 200:
                raise RuntimeError(f"/healthz → {r.status_code}")
        print(f"  {G}[OK]{RS}  FastAPI server reachable\n")
    except Exception as exc:
        print(f"  {R}[ABORT]{RS}  Server not reachable: {exc}")
        print(f"         Start: uvicorn app.main:app --port 8000 --reload\n")
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Phase 1: Concurrent ingestion
    # -----------------------------------------------------------------------
    print(f"  {B}Phase 1 — Ingestion ({NUM_EVENTS} events, {CONCURRENCY} workers){RS}")
    print(THIN)

    sem     = asyncio.Semaphore(CONCURRENCY)
    limits  = httpx.Limits(max_connections=CONCURRENCY + 4, max_keepalive_connections=CONCURRENCY)
    results: list[dict] = []

    async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
        wall_t0 = time.perf_counter()
        tasks   = [_ingest_one(client, sem, i) for i in range(NUM_EVENTS)]
        results = await asyncio.gather(*tasks)
    wall_elapsed = time.perf_counter() - wall_t0

    # -----------------------------------------------------------------------
    # Compute ingestion metrics
    # -----------------------------------------------------------------------
    ok_results  = [r for r in results if r["ok"]]
    bad_results = [r for r in results if not r["ok"]]
    latencies   = sorted(r["latency_ms"] for r in ok_results)
    event_ids   = [r["event_id"] for r in ok_results if r["event_id"]]

    n           = len(latencies)
    rps         = NUM_EVENTS / wall_elapsed if wall_elapsed > 0 else 0

    def pct(p: float) -> float:
        if not latencies:
            return 0.0
        k = min(int(len(latencies) * p / 100), len(latencies) - 1)
        return latencies[k]

    mean_lat = statistics.mean(latencies) if latencies else 0.0
    p50      = pct(50)
    p95      = pct(95)
    p99      = pct(99)
    max_lat  = max(latencies) if latencies else 0.0
    min_lat  = min(latencies) if latencies else 0.0

    status_counts: dict[int, int] = {}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

    success_pct = 100.0 * len(ok_results) / NUM_EVENTS if NUM_EVENTS > 0 else 0

    # -----------------------------------------------------------------------
    # Print ingestion results
    # -----------------------------------------------------------------------
    _p("Total events submitted",  f"{NUM_EVENTS}")
    _p("Successful (HTTP 202)",   f"{len(ok_results)}", f"  ({success_pct:.1f}%)")
    _p("Failed / errors",         f"{len(bad_results)}")
    print()
    _p("Wall-clock time",         f"{wall_elapsed:.3f}", " s")
    _p("Ingestion RPS",           f"{rps:.1f}", " req/s")
    print()
    print(f"  {B}Latency Breakdown (ms):{RS}")
    _p("  Min",     f"{min_lat:.2f}", " ms")
    _p("  Mean",    f"{mean_lat:.2f}", " ms")
    _p("  p50 (Median)", f"{p50:.2f}", " ms")
    _p("  p95",    f"{p95:.2f}", " ms")
    _p("  p99",    f"{p99:.2f}", " ms")
    _p("  Max",    f"{max_lat:.2f}", " ms")
    print()
    print(f"  {B}HTTP Status Distribution:{RS}")
    for code, count in sorted(status_counts.items()):
        bar_len = int(30 * count / NUM_EVENTS)
        bar = "#" * bar_len + "-" * (30 - bar_len)
        colour = G if code == 202 else R
        print(f"  {colour}  HTTP {code if code else 'ERR'}{RS}  [{bar}]  {count:>3} ({100*count/NUM_EVENTS:.1f}%)")

    if bad_results:
        print(f"\n  {Y}[WARN]{RS}  {len(bad_results)} failed ingestion(s):")
        for br in bad_results[:5]:
            print(f"         idx={br['idx']}  status={br['status']}  err={br.get('error','')}")

    if not event_ids:
        print(f"\n  {R}[ABORT]{RS}  No events were successfully ingested. Cannot proceed to drain phase.")
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Phase 2: Delivery drain — poll until all events reach DELIVERED
    # -----------------------------------------------------------------------
    print(f"\n{THIN}")
    print(f"  {B}Phase 2 — Delivery Drain{RS}")
    print(f"  {D}Polling PostgreSQL every {DRAIN_POLL_S}s (timeout={DRAIN_TIMEOUT}s){RS}")
    print(THIN)

    event_uuid_set = {uuid.UUID(eid) for eid in event_ids}
    total_to_drain = len(event_uuid_set)

    drain_t0      = time.perf_counter()
    deadline      = drain_t0 + DRAIN_TIMEOUT
    delivered_set: set[uuid.UUID] = set()
    last_count    = -1

    while time.monotonic() < deadline:
        async with AsyncSessionLocal() as session:
            rows = (
                await session.execute(
                    select(Event.id, Event.status).where(
                        Event.id.in_(list(event_uuid_set)),
                        Event.status == EventStatus.DELIVERED,
                    )
                )
            ).all()
        for row in rows:
            delivered_set.add(row[0])

        done = len(delivered_set)
        remaining = total_to_drain - done
        elapsed_drain = time.perf_counter() - drain_t0

        if done != last_count:
            bar_len  = int(40 * done / total_to_drain) if total_to_drain else 0
            bar      = "#" * bar_len + "-" * (40 - bar_len)
            pct_done = 100.0 * done / total_to_drain if total_to_drain else 0
            print(f"  {D}[{elapsed_drain:5.1f}s]{RS}  [{bar}]  {done}/{total_to_drain} ({pct_done:.0f}%)")
            last_count = done

        if done >= total_to_drain:
            break

        await asyncio.sleep(DRAIN_POLL_S)

    drain_elapsed   = time.perf_counter() - drain_t0
    final_delivered = len(delivered_set)
    not_delivered   = total_to_drain - final_delivered
    delivery_rps    = final_delivered / drain_elapsed if drain_elapsed > 0 else 0

    # -----------------------------------------------------------------------
    # Phase 2 summary
    # -----------------------------------------------------------------------
    print()
    _p("Events queued for delivery", f"{total_to_drain}")
    _p("Events DELIVERED",           f"{final_delivered}")
    _p("Events NOT delivered",       f"{not_delivered}", "  (timed out or failed)")
    _p("Drain wall-clock time",      f"{drain_elapsed:.2f}", " s")
    _p("Delivery throughput",        f"{delivery_rps:.1f}", " events/s")

    # -----------------------------------------------------------------------
    # Final summary table
    # -----------------------------------------------------------------------
    all_pass = (len(ok_results) == NUM_EVENTS) and (final_delivered == total_to_drain)

    print(f"\n{SEP}")
    print(f"  {B}LOAD TEST RESULTS SUMMARY{RS}")
    print(THIN)
    print(f"  {'Metric':<36}  {'Value'}")
    print(f"  {'-'*36}  {'-'*24}")
    print(f"  {'Events Dispatched':<36}  {NUM_EVENTS}")
    print(f"  {'Successful Ingestion (202)':<36}  {len(ok_results)} / {NUM_EVENTS}  ({success_pct:.1f}%)")
    print(f"  {'Ingestion RPS':<36}  {rps:.1f} req/s")
    print(f"  {'Ingestion Wall Time':<36}  {wall_elapsed:.3f} s")
    print(f"  {'Latency p50 / p95 / p99':<36}  {p50:.1f} / {p95:.1f} / {p99:.1f} ms")
    print(f"  {'Latency Max':<36}  {max_lat:.1f} ms")
    print(f"  {'Events Delivered (DB)':<36}  {final_delivered} / {total_to_drain}")
    print(f"  {'Delivery Throughput':<36}  {delivery_rps:.1f} events/s")
    print(f"  {'Drain Wall Time':<36}  {drain_elapsed:.2f} s")
    print(THIN)

    if all_pass:
        print(f"\n  {G}{B}[PASS]  All {NUM_EVENTS} events ingested & delivered successfully.{RS}")
    else:
        print(f"\n  {R}{B}[PARTIAL]  {not_delivered} event(s) not delivered within {DRAIN_TIMEOUT}s timeout.{RS}")

    print(f"  {D}Total test elapsed: {wall_elapsed + drain_elapsed:.2f}s{RS}")
    print(f"\n{SEP}\n")

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
