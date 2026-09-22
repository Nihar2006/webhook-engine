"""
scripts/benchmark_sync.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Phase 2 benchmark — measures the throughput ceiling of synchronous,
in-band webhook delivery.

What it does:
  1. Registers 2 endpoints pointing at the local mock receiver.
  2. Dispatches 10 events sequentially.
  3. Collects per-event wall-clock times.
  4. Prints total time, average latency, p95 latency, and a per-event table.

Expected output (2 endpoints × 1.5 s each):
  Each event takes ≥ 3 s, total ≥ 30 s for 10 events.
  This proves that throughput is hard-capped at ~0.33 events/s with 2 slow receivers.

Run from project root (server must be running on :8000):
    python scripts/benchmark_sync.py
"""
import asyncio
import statistics
import sys
import uuid

sys.path.insert(0, ".")

import httpx

BASE_URL = "http://localhost:8000"
MOCK_RECEIVER = f"{BASE_URL}/api/v1/mock/receiver"
NUM_ENDPOINTS = 2
NUM_EVENTS = 10


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


async def dispatch_events(client: httpx.AsyncClient) -> list[float]:
    """
    Dispatch NUM_EVENTS events sequentially and return per-event elapsed times (s).
    Each event is dispatched only after the previous one completes — so the
    total time is the sum, not the max.
    """
    latencies: list[float] = []

    print(f"\n{'─' * 58}".encode('ascii', 'replace').decode())
    print(f"  {'#':>3}   {'Elapsed (s)':>12}   {'Status':<10}   Deliveries")
    print("-" * 58)

    for i in range(NUM_EVENTS):
        idempotency_key = f"bench-{uuid.uuid4()}"
        payload = {"event_type": "bench.test", "order_id": i}

        import time
        t0 = time.perf_counter()
        resp = await client.post(
            f"{BASE_URL}/api/v1/events",
            json={
                "event_type": "bench.test",
                "payload": payload,
                "idempotency_key": idempotency_key,
            },
            timeout=60.0,   # generous timeout — we KNOW this is slow
        )
        elapsed = time.perf_counter() - t0
        latencies.append(elapsed)

        resp.raise_for_status()
        data = resp.json()
        event_status = data["event"]["status"]
        n_deliveries = len(data["deliveries"])
        print(f"  {i + 1:>3}   {elapsed:>12.3f}   {event_status:<10}   {n_deliveries} attempt(s)")

    return latencies


def print_summary(latencies: list[float]) -> None:
    total = sum(latencies)
    avg = statistics.mean(latencies)
    p95_index = max(0, int(len(latencies) * 0.95) - 1)
    p95 = sorted(latencies)[p95_index]
    throughput = len(latencies) / total

    print("=" * 58)
    print("  BENCHMARK SUMMARY - Synchronous Delivery Baseline")
    print("=" * 58)
    print(f"  Endpoints registered : {NUM_ENDPOINTS}")
    print(f"  Events dispatched    : {NUM_EVENTS}")
    print(f"  Total wall-clock time: {total:>8.3f} s")
    print(f"  Average per event    : {avg:>8.3f} s")
    print(f"  p95 latency          : {p95:>8.3f} s")
    print(f"  Throughput           : {throughput:>8.3f} events/s")
    print("=" * 58)
    print()
    print("  INTERPRETATION")
    print(f"  Each event blocks the worker for ~{avg:.1f} s")
    print(f"  ({NUM_ENDPOINTS} endpoints x ~1.5 s receiver latency each).")
    print("  Throughput scales inversely with receiver count and latency.")
    print("  Adding a 3rd slow endpoint would push avg above 4.5 s/event.")
    print("  --> This is the architectural pain Phase 3 (Celery) resolves.")
    print()


async def main() -> None:
    print("=" * 58)
    print("  Webhook Engine - Phase 2 Synchronous Benchmark")
    print("=" * 58)

    async with httpx.AsyncClient() as client:
        # Verify server is reachable
        try:
            health = await client.get(f"{BASE_URL}/healthz", timeout=3.0)
            health.raise_for_status()
            print(f"\n  [OK] Server healthy: {health.json()}")
        except Exception as exc:
            print(f"\n  [ERROR] Cannot reach server at {BASE_URL}: {exc}")
            print("  Start it with: uvicorn app.main:app --reload")
            sys.exit(1)

        print(f"\n  Registering {NUM_ENDPOINTS} mock receiver(s)...")
        await register_endpoints(client)

        print(f"\n  Dispatching {NUM_EVENTS} events sequentially...")
        print("  (Each call blocks until ALL endpoints respond)\n")
        latencies = await dispatch_events(client)

    print_summary(latencies)


if __name__ == "__main__":
    asyncio.run(main())
