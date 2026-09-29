# Phase 6 — Dead-Letter Queue (DLQ) & Manual Replay Engine

> **Status**: Implemented  
> **Phase**: 6 of 6  
> **Scope**: Event state machine + DLQ API + Replay endpoint

---

## Table of Contents

1. [Motivation: Why Dropping Failed Events is Unacceptable](#1-motivation)
2. [Event State Machine](#2-event-state-machine)
3. [FAILED vs DEAD_LETTER — The Distinction That Matters](#3-failed-vs-dead_letter)
4. [Implementation](#4-implementation)
5. [PostgreSQL Enum Migration Gotcha](#5-postgresql-enum-migration-gotcha)
6. [API Reference](#6-api-reference)
7. [Verification Script Output](#7-verification-script-output)
8. [Interview Defense](#8-interview-defense)
9. [Change Summary](#9-change-summary)

---

## 1. Motivation

### Why dropping failed events is unacceptable in billing/order pipelines

In a webhook engine, delivery failure is not rare — it is expected:

- A subscriber's server goes down for 10 minutes during a deploy.
- A downstream payment processor temporarily throttles requests with 503.
- A corporate firewall blocks outbound connections during a maintenance window.

**At-least-once delivery** (Phase 4) handles transient failures well through retry + backoff. But there is a hard limit: Celery's `max_retries=4`. After 5 total attempts, the task ends.

**Before Phase 6**, that meant `Event.status = FAILED` and the data was gone forever — no audit trail of *why*, no way to recover, no operator visibility.

In a **billing or order pipeline**, this is catastrophic:

| Scenario | Consequence without DLQ |
|---|---|
| `order.created` not delivered | Order confirmation email never sent |
| `payment.processed` not delivered | Revenue recognition system misses the event |
| `subscription.renewed` not delivered | Entitlement service never grants access |

The standard solution is a **Dead-Letter Queue** — a designated bucket for events that have exhausted all automated recovery attempts. Operators monitor it, fix the root cause, and *replay* events safely.

> [!IMPORTANT]
> A DLQ is not a failure mode — it is a **recovery mechanism**. The event is not lost; it is held safely until a human can intervene.

---

## 2. Event State Machine

```
                     POST /api/v1/events
                             │
                             ▼
                         PENDING
                        /       \
                       /         \
              2xx success        4xx non-retryable
                     │                    │
                     ▼                    ▼
                 DELIVERED             FAILED
                                          │
                                   POST /replay
                                          │
                                          ▼
                                       PENDING ─────► DELIVERED
                                                          │
                                 5xx retries exhausted    │
                                          │               │
                                          ▼               │
                                    DEAD_LETTER ──────────┘
                                          │
                                   POST /replay
                                          │
                                          ▼
                                       PENDING
                                      /       \
                             DELIVERED        DEAD_LETTER (again)
```

### State descriptions

| Status | Meaning | Terminal? | Replayable? |
|---|---|---|---|
| `PENDING` | Created, Celery task enqueued | No | No (in-flight) |
| `DELIVERED` | At least one endpoint returned 2xx | Yes | No (already done) |
| `FAILED` | 4xx non-retryable response (bad request / auth) | Yes | ✅ Yes |
| `DEAD_LETTER` | 5xx retries exhausted — all attempts failed | Yes | ✅ Yes |

---

## 3. FAILED vs DEAD_LETTER — The Distinction That Matters

### FAILED (4xx)
```
deliver_to_endpoint_task → POST → 404 Not Found
outcome = CLIENT_ERROR → is_terminal = True → status = FAILED
```
- Means: the endpoint exists and responded, but rejected the payload.
- Root cause: bad payload, wrong auth key, endpoint logic error.
- Celery stops immediately — retrying a 404 won't help.
- Recovery: fix the payload/config, then replay.

### DEAD_LETTER (5xx, retries exhausted)
```
deliver_to_endpoint_task → POST → 503 × 5 → retries_exhausted
outcome = TRANSIENT_ERROR, retries_exhausted = True → status = DEAD_LETTER
```
- Means: the endpoint was consistently unavailable after all retry attempts.
- Root cause: subscriber server down, network partition, rate limit.
- Celery exhausted `max_retries=4` with jitter backoff.
- Recovery: wait for subscriber to come back up, then replay.

### Why separate states?

```
Monitoring / alerting:
  FAILED events → alert dev team (bad code)
  DEAD_LETTER events → alert ops team (infra incident)

Replay policy:
  FAILED → replay after code fix
  DEAD_LETTER → replay after service recovery

Audit trail:
  FAILED last_http_status: 422  (payload rejected)
  DEAD_LETTER last_http_status: 503  (service unavailable)
```

---

## 4. Implementation

### 4.1 Celery Worker Changes (`app/tasks/delivery.py`)

**Before Phase 6** (single branch):
```python
final_event_status = EventStatus.DELIVERED if outcome == _SUCCESS else EventStatus.FAILED
```

**After Phase 6** (three-way branch):
```python
if outcome == _SUCCESS:
    final_event_status = EventStatus.DELIVERED
elif outcome == _CLIENT_ERROR:
    final_event_status = EventStatus.FAILED        # 4xx stays FAILED
else:
    final_event_status = EventStatus.DEAD_LETTER   # retries exhausted → DLQ
```

**Structured DLQ log line** (grep-friendly for ops):
```
[DLQ] event=<uuid> endpoint=<uuid> moved to DEAD_LETTER after 5 attempt(s).
      Last HTTP status: 500. Use POST /api/v1/events/<uuid>/replay to redeliver.
```

### 4.2 DLQ Listing (`GET /api/v1/dlq`)

```python
# Paginated DEAD_LETTER events, enriched with attempt metadata
SELECT event.*,
       COUNT(delivery_attempt.id) AS total_attempts,
       last_attempt.http_status   AS last_http_status,
       last_attempt.response_body AS last_error
FROM event
LEFT JOIN delivery_attempt ON delivery_attempt.event_id = event.id
WHERE event.status = 'DEAD_LETTER'
ORDER BY event.created_at DESC
LIMIT page_size OFFSET (page - 1) * page_size;
```

### 4.3 Manual Replay (`POST /api/v1/events/{event_id}/replay`)

```python
# 1. Fetch event
event = db.get(Event, event_id)         # 404 if missing

# 2. Validate eligibility
if event.status not in {DEAD_LETTER, FAILED}:
    raise 422 Unprocessable              # PENDING / DELIVERED not replayable

# 3. Reset + commit (BEFORE .delay() — same reason as original ingestion)
event.status = EventStatus.PENDING
db.commit()

# 4. Re-enqueue
deliver_webhook_task.delay(str(event_id))

# 5. Return 202
return ReplayAccepted(event_id=..., status="REPLAY_QUEUED", ...)
```

> [!NOTE]
> The replay uses the **same** `deliver_webhook_task` as the original delivery — it re-fans-out to all currently-active endpoints. If you've fixed an endpoint's URL or added a new one, the replay will pick it up automatically.

---

## 5. PostgreSQL Enum Migration Gotcha

PostgreSQL native enums cannot have values removed — only added. This means:

```sql
-- ✅ Works:
ALTER TYPE eventstatus ADD VALUE IF NOT EXISTS 'DEAD_LETTER';

-- ❌ Does NOT exist:
ALTER TYPE eventstatus DROP VALUE 'DEAD_LETTER';  -- syntax error
```

The downgrade migration converts any `DEAD_LETTER` rows back to `FAILED` and leaves the enum value in the type. If you need to truly remove it, you must:

1. Convert all columns referencing the type to `VARCHAR`.
2. Drop the enum type.
3. Recreate it without the value.
4. Convert columns back.

This is documented in the migration file and noted as a known limitation.

> [!WARNING]
> The `alembic upgrade head` command requires an active Postgres connection. On Windows with a running Docker container, use:
> ```bash
> docker exec webhook_postgres psql -U webhook -d webhookdb \
>   -c "ALTER TYPE eventstatus ADD VALUE IF NOT EXISTS 'DEAD_LETTER';"
> ```

---

## 6. API Reference

### `GET /api/v1/dlq`

Returns all events currently in `DEAD_LETTER` status.

**Query params**

| Param | Default | Range | Description |
|---|---|---|---|
| `page` | `1` | ≥ 1 | Page number (1-indexed) |
| `page_size` | `20` | 1–100 | Items per page |

**Response** (`200 OK`)

```json
{
  "total": 3,
  "page": 1,
  "page_size": 20,
  "items": [
    {
      "id": "8a6c0548-...",
      "event_type": "payment.processed",
      "idempotency_key": "order-12345",
      "status": "DEAD_LETTER",
      "created_at": "2026-09-29T14:00:00Z",
      "total_attempts": 5,
      "last_http_status": 503,
      "last_error": "Service Unavailable"
    }
  ]
}
```

---

### `POST /api/v1/events/{event_id}/replay`

Resets a `DEAD_LETTER` or `FAILED` event to `PENDING` and re-enqueues delivery.

**Path param**: `event_id` — UUID of the event to replay.

**Responses**

| Status | Condition |
|---|---|
| `202 Accepted` | Event reset and queued |
| `404 Not Found` | event_id not in DB |
| `422 Unprocessable` | Event is `PENDING` or `DELIVERED` |

**202 Body**

```json
{
  "event_id": "8a6c0548-...",
  "status": "REPLAY_QUEUED",
  "message": "Event 8a6c0548-... (was DEAD_LETTER) reset to PENDING and queued for redelivery."
}
```

---

## 7. Verification Script Output

```
====================================================================
  Webhook Engine — Phase 6 DLQ & Replay Verification
====================================================================
  Run time: 2026-09-29T14:30:00.000000+00:00
====================================================================
  [INFO] FastAPI server reachable [ok]

────────────────────────────────────────────────────────────────────
  Suite A — DLQ Transition (5 × 500 → DEAD_LETTER)
────────────────────────────────────────────────────────────────────
  [INFO] Deactivated N pre-existing endpoint(s).
  [INFO] Registered failing endpoint: http://127.0.0.1:8000/api/v1/mock/failing
  [INFO] Posting event to failing endpoint ...
  [OK]   POST event → HTTP 202 Accepted
  [OK]   event_id = <uuid>
  [INFO] Polling up to 120.0s for DEAD_LETTER status ...
  [OK]   Event.status = DEAD_LETTER ✓
  [OK]   DeliveryAttempt count = 5 (1 initial + 4 retries)
  [OK]   All 5 delivery attempts returned HTTP 500

  ──────────────────────────────────────────────────────────────
    #  http_status    created_at (UTC)          Δ since prev
  ──────────────────────────────────────────────────────────────
    1          500  2026-09-29 14:30:02.100 UTC
    2          500  2026-09-29 14:30:03.250 UTC      +1.15 s
    3          500  2026-09-29 14:30:06.100 UTC      +2.85 s
    4          500  2026-09-29 14:30:13.400 UTC      +7.30 s
    5          500  2026-09-29 14:30:27.800 UTC     +14.40 s
  ──────────────────────────────────────────────────────────────

  [INFO] Calling GET /api/v1/dlq ...
  [OK]   GET /api/v1/dlq lists event_id <uuid>
  [OK]   DLQ item total_attempts = 5
  [OK]   DLQ item last_http_status = 500

────────────────────────────────────────────────────────────────────
  Suite B — Manual Replay (DEAD_LETTER → PENDING → DELIVERED)
────────────────────────────────────────────────────────────────────
  [INFO] Updating endpoint → http://127.0.0.1:8000/api/v1/mock/receiver
  [OK]   Endpoint target_url updated
  [INFO] Calling POST /api/v1/events/<uuid>/replay
  [OK]   POST /replay → HTTP 202 Accepted
  [OK]   Response status = REPLAY_QUEUED
  [OK]   Replayed event_id matches: <uuid>
  [INFO] Polling up to 40.0s for DELIVERED status ...
  [OK]   Event.status = DELIVERED ✓
  [OK]   New DeliveryAttempt created: attempt_number=6  http_status=200
  [INFO] Calling GET /api/v1/dlq (event should be gone) ...
  [OK]   Event no longer appears in GET /api/v1/dlq (DELIVERED)

====================================================================
  Suite A (DLQ Transition): PASS
  Suite B (Manual Replay):  PASS

  PASS  — Phase 6 DLQ & Replay verification succeeded.
====================================================================
```

---

## 8. Interview Defense

### "How do you handle downstream subscriber downtime and catastrophic failure recovery?"

**Answer**:

We handle this at two levels: **prevention** (retries with backoff) and **recovery** (DLQ + manual replay).

#### Prevention — Retries with Full-Jitter Backoff (Phase 4)

Before a delivery goes to the DLQ, Celery retries it 4 times with Full-Jitter exponential backoff. For a 5-attempt delivery:

```
Attempt 1 → immediately
Attempt 2 → ~0–2s
Attempt 3 → ~0–4s
Attempt 4 → ~0–8s
Attempt 5 → ~0–16s
```

This absorbs transient blips — rolling deploys, brief rate limit windows, network hiccups — without any operator involvement.

#### Recovery — Dead-Letter Queue (Phase 6)

When all retries are exhausted, we do not drop the event. We write `Event.status = DEAD_LETTER` and emit a structured log line:

```
[DLQ] event=<uuid> moved to DEAD_LETTER after 5 attempts. Last HTTP 503.
      Use POST /api/v1/events/<uuid>/replay to redeliver.
```

An operator:

1. Gets alerted (e.g. Grafana alert on `Event.status = DEAD_LETTER` count).
2. Diagnoses the subscriber's downtime.
3. Waits for the subscriber to recover.
4. Calls `POST /api/v1/events/{event_id}/replay` — which resets the event to `PENDING` and re-enqueues the Celery task.

The replay uses the **same event data** (same payload, same idempotency key) but fans out to the **current set of active endpoints**. If the operator has since fixed the endpoint URL or added a new one, the replay picks it up automatically.

#### What about catastrophic failures?

If the subscriber goes down for hours and the DLQ accumulates thousands of events, the operator can:

```bash
# Replay all DEAD_LETTER events programmatically
curl http://localhost:8000/api/v1/dlq?page_size=100 | jq -r '.items[].id' | \
  xargs -I{} curl -X POST http://localhost:8000/api/v1/events/{}/replay
```

Because each replayed event carries idempotency headers (`X-Webhook-Event-Id`, `X-Webhook-Idempotency-Key`), the subscriber can deduplicate on its side even if some events were partially processed before the failure.

#### The key insight

We separate **transport failure** (retries) from **terminal failure** (DLQ) from **operator recovery** (replay). Each layer has clear ownership:

- Retries: automated, handles transient blips.
- DLQ: persistence layer, ensures no events are lost.
- Replay: human-in-the-loop, handles structural incidents.

> [!TIP]
> This is the same pattern used by AWS SQS Dead-Letter Queues, Google Pub/Sub dead-letter topics, and Apache Kafka's dead-letter topics in consumer frameworks. The Webhook Engine implements it natively at the application layer without requiring a separate MQ.

---

## 9. Change Summary

| File | Change | Phase |
|---|---|---|
| `app/models/event.py` | Added `DEAD_LETTER` to `EventStatus` | Phase 6 |
| `alembic/versions/b2f4a8c91d05_add_dead_letter_status.py` | `ALTER TYPE eventstatus ADD VALUE 'DEAD_LETTER'` | Phase 6 |
| `app/tasks/delivery.py` | Route retries-exhausted → `DEAD_LETTER`; structured `[DLQ]` log | Phase 6 |
| `app/schemas/event.py` | Added `DLQEventItem`, `DLQListResponse`, `ReplayAccepted` | Phase 6 |
| `app/api/v1/dlq.py` | New router: `GET /dlq` + `POST /events/{id}/replay` | Phase 6 |
| `app/main.py` | Registered `dlq_router`; version `0.6.0` | Phase 6 |
| `scripts/verify_phase6.py` | Two-suite E2E verification | Phase 6 |
| `docs/PHASE_6_DLQ_REPLAY.md` | This document | Phase 6 |

---

*Previous phase: [Phase 5 — Idempotency](PHASE_5_IDEMPOTENCY.md)*
