# Phase 4 — Automatic Retries with Full-Jitter Exponential Backoff

## Overview

Phase 4 makes the webhook delivery pipeline **resilient to transient failures**.
A downstream service going briefly unavailable — a rolling restart, a momentary
network blip, a 503 during a deployment — no longer permanently loses delivery.
The Celery worker automatically retries with an exponentially growing window of
randomised delays, then records a permanent audit trail of every attempt.

---

## 1. Transient vs Permanent Failures

Not all failures are the same.  Retrying indiscriminately wastes resources and
can even make the situation worse.

| HTTP Status | Classification | Retry? | Rationale |
|-------------|---------------|--------|-----------|
| **2xx** | Success | — | Delivery confirmed |
| **400 Bad Request** | Permanent | **No** | The payload is malformed — resending the same payload will always fail |
| **401 / 403** | Permanent | **No** | Authentication / authorization — retrying won't change credentials |
| **404 Not Found** | Permanent | **No** | The endpoint URL is gone — retrying is futile |
| **408 / 429** | Borderline | Optional | Timeout / Rate-limit — could retry with longer backoff (not implemented here) |
| **500 Internal Server Error** | Transient | **Yes** | Server-side bug or crash; may self-heal |
| **502 Bad Gateway** | Transient | **Yes** | Upstream proxy issue |
| **503 Service Unavailable** | Transient | **Yes** | Typical during restarts / overload |
| **504 Gateway Timeout** | Transient | **Yes** | Network hop timed out |
| **`TimeoutException`** | Transient | **Yes** | No response within 5 s |
| **`RequestError`** | Transient | **Yes** | TCP-level connection failure |

> **Interview defence:** "4xx errors indicate the *client* (our payload or config) is
> wrong.  Retrying a bad request wastes worker cycles, fills the dead-letter queue,
> and spams the downstream log with identical errors.  5xx and network errors indicate
> the *server* is temporarily unavailable — the same request will likely succeed once
> the server recovers, so retrying is both safe and beneficial."

---

## 2. Why Naive Exponential Backoff Causes Thundering Herds

Suppose 1 000 Celery workers all send requests to the same downstream service at
`T = 0`.  The service goes down.  All workers observe a 503 and apply naive
exponential backoff:

```
sleep = base * 2 ** attempt
```

| Attempt | Delay | All 1 000 workers retry at … |
|---------|-------|------------------------------|
| 1 | 1 s | **T = 1 s** — synchronized spike of 1 000 requests |
| 2 | 2 s | **T = 3 s** — synchronized spike of 1 000 requests |
| 3 | 4 s | **T = 7 s** — synchronized spike of 1 000 requests |

The recovering service — which may still be fragile — is hit by three
**thundering herds** of 1 000 simultaneous requests.  The second spike can
re-overwhelm it, resetting the failure clock and creating an indefinite loop.

### Mathematical intuition

The naive formula has **zero variance** for a given attempt number: every worker
that observed the failure at the same time sleeps for exactly the same duration
and wakes up simultaneously.

Expected concurrent requests at retry moment **= N** (all N workers at once).

---

## 3. Full Jitter — Flattening the Concurrency Spike

Full Jitter replaces the fixed delay with a **uniform random sample** from
`[0, cap]`:

```python
cap   = min(max_delay, base_delay * (2 ** attempt))   # exponential ceiling
sleep = random.uniform(0, cap)                          # Full Jitter
```

With the same 1 000 workers and `base_delay=1`, `max_delay=60`:

| Attempt | Cap | Workers are spread across |
|---------|-----|--------------------------|
| 1 | 2 s | `[0, 2]` — ~500 workers/second |
| 2 | 4 s | `[0, 4]` — ~250 workers/second |
| 3 | 8 s | `[0, 8]` — ~125 workers/second |
| 4 | 16 s | `[0, 16]` — ~62.5 workers/second |

Expected concurrent requests at any single moment ≈ **N / cap** — which shrinks
as `cap` grows.  The recovering service sees a **gradual ramp** rather than a
synchronized stampede.

### Variance comparison

| Strategy | E[sleep] | Var[sleep] | Peak concurrency |
|----------|----------|------------|-----------------|
| Naive exponential | `cap` | 0 | **N** (worst) |
| Equal Jitter | `0.75 · cap` | `cap²/48` | Moderate |
| **Full Jitter** | `cap/2` | `cap²/12` | **N/cap** (best) |

Full Jitter has the highest variance of all jitter strategies, which directly
translates to the lowest peak concurrency on the recovering server.

> Source: Amazon AWS Architecture Blog — *"Exponential Backoff And Jitter"* (2015).

---

## 4. Task Architecture (Phase 4)

```
POST /api/v1/events (FastAPI)
         │
         │  await db.commit()   ← row visible to all DB connections
         │  deliver_webhook_task.delay(event_id)
         ▼
 deliver_webhook_task(event_id)
   bind=True, max_retries=0   ← lightweight dispatcher, never retries
         │
         │  reads all active WebhookEndpoint rows
         │  for each endpoint:
         │    deliver_to_endpoint_task.delay(event_id, endpoint_id)
         ▼
 deliver_to_endpoint_task(event_id, endpoint_id)
   bind=True, max_retries=4   ← 1 initial + 4 retries = 5 total attempts
         │
         ├─ 2xx  → record DeliveryAttempt(http_status=2xx, attempt_number=N)
         │         update Event.status = DELIVERED
         │
         ├─ 4xx  → record DeliveryAttempt(http_status=4xx, attempt_number=N)
         │         update Event.status = FAILED
         │         (no retry)
         │
         └─ 5xx / timeout / RequestError
                 → record DeliveryAttempt(http_status=5xx, attempt_number=N)
                   if retries < max_retries:
                     countdown = calculate_full_jitter_backoff(retries)
                     raise self.retry(countdown=countdown)
                   else:
                     update Event.status = FAILED
```

**Why per-endpoint tasks?**
With a single task that fans out to N endpoints, a 5xx from endpoint B forces a
retry that also re-delivers to endpoint A (which already succeeded).  Splitting into
per-endpoint tasks means each endpoint's retry lifecycle is independent: a flaky
endpoint never delays or duplicates delivery to healthy endpoints.

---

## 5. Backoff Utility — `app/core/retry.py`

```python
def calculate_full_jitter_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> float:
    cap   = min(max_delay, base_delay * (2 ** attempt))
    return random.uniform(0, cap)
```

Example countdown values for successive retries (`base_delay=1`, `max_delay=60`):

| `attempt` | cap | Possible countdown range |
|-----------|-----|--------------------------|
| 0 (1st retry) | 1 s | [0.00, 1.00] s |
| 1 (2nd retry) | 2 s | [0.00, 2.00] s |
| 2 (3rd retry) | 4 s | [0.00, 4.00] s |
| 3 (4th retry) | 8 s | [0.00, 8.00] s |
| 4 (5th attempt — exhausted) | — | No retry |

---

## 6. Mock Endpoints — `app/api/v1/mock.py`

| Endpoint | Behaviour | Purpose |
|----------|-----------|---------|
| `POST /mock/receiver` | Sleeps 1.5 s, returns 200 | Phase 2/3 throughput baseline |
| `POST /mock/flaky` | Returns 503 on calls 1 & 2 of each 3-cycle, then 200 | Test successful recovery after retries |
| `POST /mock/failing` | Always returns 500 | Test max-retries exhaustion → FAILED |

---

## 7. Verification Audit Trail — `scripts/verify_phase4.py`

### Scenario A — Flaky endpoint (503 → 503 → 200 → DELIVERED)

```
Scenario: Flaky (503 → 503 → 200)
  [OK]   Event.status = DELIVERED
  [OK]   DeliveryAttempt rows: 3 (expected 3)
  [OK]   attempt_number sequence: [1, 2, 3]
  [OK]   HTTP status per attempt: [503, 503, 200]

  Audit trail — Flaky (503 → 503 → 200)
  ────────────────────────────────────────────────────────────────
    #  http_status            created_at (UTC)    Δ since prev
  ────────────────────────────────────────────────────────────────
    1          503  2026-09-28 14:36:24.069 UTC
    2          503  2026-09-28 14:36:24.565 UTC         +0.50 s
    3          200  2026-09-28 14:36:25.949 UTC         +1.38 s
  ────────────────────────────────────────────────────────────────
```

The backoff deltas (0.50 s, 1.38 s) are in `[0, 2]` — within the Full Jitter
range for attempts 0 and 1 respectively.

### Scenario B — Failing endpoint (500 × 5 → FAILED)

```
Scenario: Failing (500 × 5, max retries exhausted)
  [OK]   Event.status = FAILED
  [OK]   DeliveryAttempt rows: 5 (expected 5)
  [OK]   attempt_number sequence: [1, 2, 3, 4, 5]
  [OK]   HTTP status per attempt: [500, 500, 500, 500, 500]

  Audit trail — Failing (500 × 5, max retries exhausted)
  ────────────────────────────────────────────────────────────────
    #  http_status            created_at (UTC)    Δ since prev
  ────────────────────────────────────────────────────────────────
    1          500  2026-09-28 14:36:46.143 UTC
    2          500  2026-09-28 14:36:47.156 UTC         +1.01 s
    3          500  2026-09-28 14:36:48.751 UTC         +1.60 s
    4          500  2026-09-28 14:36:49.538 UTC         +0.79 s
    5          500  2026-09-28 14:36:51.367 UTC         +1.83 s
  ────────────────────────────────────────────────────────────────

  PASS — Phase 4 retry + backoff verification succeeded.
```

All 5 attempts recorded with incrementing `attempt_number`.  The Δ values are
stochastic (Full Jitter in `[0, cap]`), demonstrating that successive retries
are **not** synchronized.

---

## 8. Interview Defence — When NOT to Retry

> **"Tell me about your retry strategy and when you would choose not to retry."**

**Do retry:** 5xx, timeouts, connection errors.  These indicate the server is
temporarily unavailable.  The same payload, sent later, will likely succeed.
Full Jitter ensures retries are spread over time rather than synchronized.

**Do NOT retry:**
- **4xx client errors** — the payload or endpoint config is wrong.  Retrying
  the identical payload will produce the identical error indefinitely.  The
  correct action is to surface the error to the operator so they can fix the
  event or endpoint configuration.
- **After max_retries exhaustion** — at some point, continued retries consume
  Celery worker capacity that could be used for fresh events.  Marking the event
  FAILED and alerting on-call is the right escalation path.
- **Idempotency risk** — if the downstream endpoint is not idempotent, a 2xx
  that was never received (network failure after server processed) would lead to
  a duplicate on retry.  Mitigate with idempotency keys (already implemented in
  Phase 1 for the Event table; endpoints should also honour `X-Idempotency-Key`).

---

## 9. Files Changed

| File | Change |
|------|--------|
| `app/core/retry.py` | **New** — `calculate_full_jitter_backoff()` utility |
| `app/tasks/delivery.py` | **Rewrite** — split into dispatcher + per-endpoint retryable worker |
| `app/api/v1/mock.py` | **Extended** — `/flaky` and `/failing` endpoints |
| `app/core/celery_app.py` | Updated comment (no logic change; both tasks in same module) |
| `scripts/verify_phase4.py` | **New** — two-scenario end-to-end smoke test |
