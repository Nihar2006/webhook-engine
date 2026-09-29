# Phase 5 — Guarantee End-to-End Idempotency

> **Status**: Implemented  
> **Phase**: 5 of 5  
> **Scope**: Ingestion layer (FastAPI) + delivery layer (Celery)

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Delivery Semantics: At-Least-Once vs Exactly-Once](#2-delivery-semantics-at-least-once-vs-exactly-once)
3. [Idempotency Strategy](#3-idempotency-strategy)
4. [Part A — Ingestion Idempotency](#4-part-a--ingestion-idempotency-apiv1eventspy)
5. [Part B — Consumer Idempotency Headers](#5-part-b--consumer-idempotency-headers-taskdeliverypy)
6. [Race Condition Analysis](#6-race-condition-analysis)
7. [Verification Script Output](#7-verification-script-output)
8. [Interview Defense](#8-interview-defense)
9. [Change Summary](#9-change-summary)

---

## 1. Motivation

Webhook delivery is inherently unreliable:

- **Callers retry** on network timeouts, getting HTTP 5xx from the API, or not trusting the first ack.
- **Celery retries** on transient 5xx from the subscriber endpoint (up to 4 retries + jitter backoff, Phase 4).
- **Network partitions** can cause a request to be processed successfully while the ACK never reaches the caller — triggering an application-level retry.

Without explicit idempotency guarantees, **every retry risks a duplicate** — duplicate database rows on the ingestion side, duplicate HTTP deliveries on the consumer side.

Phase 5 closes both gaps at the two seams where duplicates can enter the system.

---

## 2. Delivery Semantics: At-Least-Once vs Exactly-Once

### 2.1 At-Least-Once Delivery

> The system guarantees every event is delivered to each subscriber **at least once**, but makes no promise about duplicates.

- **How it works**: On failure or timeout, the task is retried.  Each retry fires an independent HTTP POST.
- **Tradeoff**: Simple to implement; subscribers may receive the same event 2+ times.
- **Webhook Engine before Phase 5**: At-least-once only.  Two API calls with the same key → two deliveries.

### 2.2 Exactly-Once Delivery

> The system guarantees every event is delivered **exactly once** — no duplicates, no losses.

- **Theoretically impossible** in a fully distributed system.  Proof: the *Two Generals Problem* — you cannot guarantee both parties agree on a state over an unreliable channel.
- **Practically achievable** through a combination of:
  - **Idempotent producers** (the ingestion layer deduplicates on `idempotency_key`)
  - **Idempotent consumers** (the subscriber deduplicates using headers like `X-Webhook-Event-Id`)

> [!IMPORTANT]
> **What we actually guarantee in Phase 5**: *Exactly-Once ingestion* (one Event row, one Celery task) **and** *At-Least-Once delivery with idempotency headers* so the subscriber can achieve its own deduplication.

### 2.3 The Practical Middle Ground

```
┌─────────────────────────────────────────────────────────────────┐
│         Webhook Engine Phase 5 Delivery Contract                │
│                                                                 │
│  Producer side (POST /api/v1/events):                          │
│    Exactly-once ingestion via idempotency_key + ON CONFLICT    │
│                                                                 │
│  Broker / Worker (Celery + Redis):                             │
│    At-least-once task execution (retries on failure)           │
│                                                                 │
│  Consumer side (subscriber endpoint):                          │
│    Subscriber uses X-Webhook-Event-Id / X-Webhook-            │
│    Idempotency-Key to deduplicate on its side                  │
│                                                                 │
│  Net result: effective exactly-once *processing* for           │
│  well-behaved consumers.                                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Idempotency Strategy

### 3.1 Two-Level Defence

| Level | Mechanism | Guarantees |
|---|---|---|
| **L1 — DB constraint** | `UNIQUE(idempotency_key)` on `event` table | No two rows with the same key can ever coexist, regardless of application code |
| **L2 — Application upsert** | `INSERT … ON CONFLICT DO NOTHING` | Concurrent inserts are silently collapsed to one; the loser gets 0 rows affected and falls back to a SELECT |
| **L3 — Headers** | 4 structured headers on every outbound POST | Subscribers get a stable `X-Webhook-Event-Id` + `X-Webhook-Idempotency-Key` to deduplicate on their side |

### 3.2 DB-level vs Application-level

#### DB-level (`ON CONFLICT`)
```sql
INSERT INTO event (id, event_type, payload, status, idempotency_key)
VALUES (…)
ON CONFLICT (idempotency_key)
DO NOTHING
RETURNING id;
```

**Pros**:
- Race-condition safe: handled entirely within a single DB transaction.
- No additional SELECT needed for the happy path.
- Works correctly even if two application servers send simultaneous inserts.

**Cons**:
- Requires knowing the conflict target column(s) at write time.
- Silent discard — the application must detect "no row returned" and handle it.

#### Application-level double-check (SELECT first)
```python
existing = db.execute(select(Event).where(Event.idempotency_key == key)).scalar_one_or_none()
if existing:
    return replay_response(existing)
```

**Pros**:
- Cheaper on the happy path (most duplicates are caught here, before reaching INSERT).
- Returns rich data (the existing event payload) without a second query.

**Cons**:
- **TOCTOU race**: two concurrent requests both see `existing = None`, both proceed to INSERT — the constraint saves you, but you still need the fallback.

#### Phase 5 uses both, in sequence:

```
SELECT → fast path for sequential duplicates (no wasted INSERT)
  ↓ not found
INSERT ON CONFLICT DO NOTHING → race-safe atomic upsert
  ↓ no row returned (lost the race)
SELECT fallback → fetch the winning row → 200 replay
```

---

## 4. Part A — Ingestion Idempotency (`app/api/v1/events.py`)

### 4.1 Flow Diagram

```
POST /api/v1/events  { idempotency_key: "k1", ... }
         │
         ▼
   SELECT event WHERE idempotency_key = "k1"
         │
    ┌────┴────────────────────┐
    │ EXISTS                  │ NOT EXISTS
    ▼                         ▼
Return HTTP 200          INSERT … ON CONFLICT DO NOTHING
X-Idempotent-Replay:          │
true                    ┌─────┴──────────────┐
(no task enqueued)      │ Row inserted        │ No row (race loser)
                        ▼                    ▼
                   commit + .delay()    SELECT fallback
                   HTTP 202 Accepted    HTTP 200 + replay header
```

### 4.2 Key Code (`app/api/v1/events.py`)

```python
# Fast-path SELECT (catches sequential duplicates cheaply)
existing_result = await db.execute(
    select(Event).where(Event.idempotency_key == body.idempotency_key)
)
existing_event = existing_result.scalar_one_or_none()

if existing_event is not None:
    return _replay_response(existing_event)   # HTTP 200 + X-Idempotent-Replay: true

# Race-safe INSERT
stmt = (
    pg_insert(Event)
    .values(id=new_id, ..., idempotency_key=body.idempotency_key)
    .on_conflict_do_nothing(index_elements=["idempotency_key"])
    .returning(Event.id)
)
result = await db.execute(stmt)
inserted_id = result.scalar_one_or_none()

if inserted_id is None:
    # Lost the race — fetch winner and replay
    await db.rollback()
    winner = (await db.execute(select(Event).where(...))).scalar_one()
    return _replay_response(winner)

await db.commit()
deliver_webhook_task.delay(str(inserted_id))
return JSONResponse(status_code=202, ...)
```

### 4.3 Response Comparison

| Scenario | HTTP Status | `X-Idempotent-Replay` | Celery task enqueued? |
|---|---|---|---|
| New key | **202 Accepted** | *(absent)* | ✅ Yes |
| Duplicate key (sequential) | **200 OK** | `true` | ❌ No |
| Duplicate key (concurrent race loser) | **200 OK** | `true` | ❌ No |

---

## 5. Part B — Consumer Idempotency Headers (`app/tasks/delivery.py`)

### 5.1 Headers Injected

Every `deliver_to_endpoint_task` HTTP POST includes:

| Header | Value | Stability across retries |
|---|---|---|
| `X-Webhook-Event-Id` | `str(event.id)` | **Stable** — same UUID every retry |
| `X-Webhook-Delivery-Id` | `str(delivery_attempt.id)` | **Changes** per attempt |
| `X-Webhook-Idempotency-Key` | `event.idempotency_key` | **Stable** — same as the producer key |
| `X-Webhook-Timestamp` | ISO 8601 UTC timestamp of dispatch | Changes per attempt |

### 5.2 Why the Delivery ID Changes

`X-Webhook-Delivery-Id` is a pre-generated UUID stored in `DeliveryAttempt.id`. Each retry creates a new `DeliveryAttempt` row with a new UUID. This lets the subscriber's audit log distinguish _which_ of the N delivery attempts a given HTTP call corresponds to — without conflating two retries as a single event.

### 5.3 Subscriber Deduplication Pattern

A well-implemented subscriber uses:

```python
event_id = request.headers["X-Webhook-Event-Id"]

if already_processed(event_id):          # idempotency check against local store
    return 200  # ACK without re-processing

process(request.body)
mark_processed(event_id)
return 200
```

`X-Webhook-Idempotency-Key` carries the **producer-assigned** key (e.g. `order-12345-created`), which is often more meaningful to the subscriber's domain than an opaque UUID.

### 5.4 Key Code (`app/tasks/delivery.py`)

```python
# Phase A: pre-generate UUID so it's consistent between headers and DB write
delivery_attempt_id: uuid.UUID = uuid.uuid4()
dispatch_timestamp: str = datetime.now(tz=timezone.utc).isoformat()

# Phase B: inject headers
delivery_headers = {
    "X-Webhook-Event-Id":        event_id_str,
    "X-Webhook-Delivery-Id":     str(delivery_attempt_id),
    "X-Webhook-Idempotency-Key": event_idempotency_key,
    "X-Webhook-Timestamp":       dispatch_timestamp,
}
resp = http_client.post(target_url, json=event_payload, headers=delivery_headers)

# Phase C: use the same UUID in the DB row
session.add(DeliveryAttempt(id=delivery_attempt_id, event_id=..., ...))
```

---

## 6. Race Condition Analysis

### Scenario: Two simultaneous POST /api/v1/events with the same key

```
Time  Request A                         Request B
───── ─────────────────────────────     ─────────────────────────────
T1    SELECT → None (key not seen)      SELECT → None (key not seen)
T2    INSERT ON CONFLICT DO NOTHING     INSERT ON CONFLICT DO NOTHING
T3    → row inserted, id=UUID-A         → 0 rows (constraint fires)
T4    COMMIT                            ROLLBACK
T5    delay(UUID-A)                     SELECT → finds UUID-A (winner)
T6    return 202                        return 200 + X-Idempotent-Replay
```

**Guarantees**:
- Exactly one `Event` row created (`UUID-A`).
- Exactly one `deliver_webhook_task` enqueued (Request A's delay).
- Request B receives the correct, stable `event_id = UUID-A`.
- No `IntegrityError` surfaces to the caller — both requests get clean responses.

> [!NOTE]
> The `UNIQUE` constraint on `idempotency_key` is enforced at the Postgres level, making this race-condition safe even across multiple FastAPI workers (e.g. `uvicorn --workers 4`).

---

## 7. Verification Script Output

Run with all services up:

```
uvicorn app.main:app --port 8000
celery -A app.core.celery_app worker --loglevel=warning --pool=solo
python scripts/verify_phase5.py
```

Expected output:

```
====================================================================
  Webhook Engine — Phase 5 Idempotency & Header Injection Verification
====================================================================
  Run time: 2026-09-29T13:50:00+00:00
====================================================================
  [INFO] FastAPI server reachable [ok]

────────────────────────────────────────────────────────────────────
  Suite A — Ingestion Deduplication
────────────────────────────────────────────────────────────────────
  [INFO] Posting first request  (key='test-idem-<uuid>')
  [INFO] First response: HTTP 202  body={"event_id":"...","status":"ACCEPTED",...}
  [OK]   First POST → HTTP 202 Accepted
  [OK]   event_id = <uuid>
  [INFO] Posting second request (same idempotency_key — should replay)
  [INFO] Second response: HTTP 200  headers={'x-idempotent-replay': 'true', ...}
  [OK]   Second POST → HTTP 200 OK (replay)
  [OK]   X-Idempotent-Replay: true header present
  [OK]   Replayed event_id matches original: <uuid>
  [OK]   DB: exactly 1 Event row for idempotency_key='test-idem-<uuid>'
  [INFO] Waiting up to 20 s for Celery delivery to complete ...
  [OK]   DeliveryAttempt count = 1 (1 per active endpoint — no double-delivery)

────────────────────────────────────────────────────────────────────
  Suite B — Delivery Header Injection
────────────────────────────────────────────────────────────────────
  [INFO] Deactivated N pre-existing endpoint(s).
  [INFO] Registered echo endpoint: http://127.0.0.1:8000/api/v1/mock/echo
  [INFO] Posting event (key='test-headers-<uuid>')
  [OK]   Event accepted: event_id=<uuid>
  [INFO] Polling up to 30.0s for Celery delivery ...
  [OK]   DeliveryAttempt found: id=<uuid>  http_status=200

  ────────────────────────────────────────────────────────────────
  Received webhook headers (as echoed by /mock/echo)
  ────────────────────────────────────────────────────────────────
  [OK]   x-webhook-event-id: <event-uuid>
  [OK]   x-webhook-delivery-id: <delivery-uuid>
  [OK]   x-webhook-idempotency-key: test-headers-<uuid>
  [OK]   x-webhook-timestamp: 2026-09-29T13:50:05.123456+00:00
  ────────────────────────────────────────────────────────────────

  [OK]   X-Webhook-Event-Id matches event_id: <event-uuid>
  [OK]   X-Webhook-Idempotency-Key matches submitted key: test-headers-<uuid>
  [OK]   X-Webhook-Delivery-Id is a valid UUID: <delivery-uuid>
  [OK]   X-Webhook-Timestamp is a valid ISO-8601 timestamp: 2026-09-29T13:50:05...

====================================================================
  Suite A (Deduplication): PASS
  Suite B (Header Injection): PASS

  PASS  — Phase 5 idempotency verification succeeded.
====================================================================
```

---

## 8. Interview Defense

### "How does your system prevent duplicate webhook processing during network partitions or retries?"

**Answer**:

We protect against duplicates at **three independent layers**, each defending a different failure mode:

#### Layer 1 — Producer idempotency (ingestion)

Every event carries a client-supplied `idempotency_key`. At ingestion, we run:

```sql
INSERT INTO event … ON CONFLICT (idempotency_key) DO NOTHING RETURNING id;
```

If the row already exists — whether because the caller retried after a timeout, or because two concurrent requests raced — the constraint fires and exactly one row is created. The "losing" request gets a `200 OK` with `X-Idempotent-Replay: true` and the original event payload, not a 409.

We also do a `SELECT` _before_ the insert (fast-path check) to avoid unnecessary insert noise on the common sequential-retry case. Together, the SELECT + ON CONFLICT gives us safety regardless of whether duplicates arrive sequentially or concurrently.

#### Layer 2 — Broker-level exactly-once dispatch

The `deliver_webhook_task` dispatcher is enqueued **after** `COMMIT`, never before. This eliminates the classic race where a worker picks up a task before the row is visible in the database and silently skips it.

The dispatcher is `max_retries=0` — it never re-dispatches per-endpoint tasks on its own failure. Per-endpoint retries are isolated in `deliver_to_endpoint_task`, so a flaky endpoint never causes a second fan-out to healthy endpoints.

#### Layer 3 — Consumer-side deduplication headers

On every outbound HTTP POST, we inject:

- `X-Webhook-Event-Id` — stable UUID across all retry attempts.
- `X-Webhook-Idempotency-Key` — the original producer key, domain-meaningful to the subscriber.
- `X-Webhook-Delivery-Id` — unique per attempt, for subscriber audit logs.
- `X-Webhook-Timestamp` — ISO 8601 UTC dispatch time.

A well-implemented subscriber checks `X-Webhook-Event-Id` against its own processed-event store before acting. Since the key is stable across retries, even if Celery delivers the same event twice (after a transient 5xx), the subscriber can safely deduplicate.

#### What about network partitions specifically?

A network partition is the hardest case: the POST arrives, the subscriber processes it successfully, but the HTTP ACK never makes it back to Celery. Celery sees a timeout → `TRANSIENT_ERROR` → retry.

The retry carries the **same** `X-Webhook-Event-Id` and `X-Webhook-Idempotency-Key`, giving the subscriber everything it needs to detect "I already processed this". The idempotency key is the subscriber's defence; our headers are the key material it needs to exercise that defence correctly.

> [!TIP]
> We deliver **at-least-once** at the transport layer and provide all the material for the subscriber to achieve **exactly-once processing**. This is the industry-standard pattern used by Stripe, GitHub, and Svix.

---

## 9. Change Summary

| File | Change | Phase |
|---|---|---|
| `app/api/v1/events.py` | Replaced 409 with true idempotent replay (200 + `X-Idempotent-Replay: true`), using `pg_insert().on_conflict_do_nothing()` | Phase 5 |
| `app/tasks/delivery.py` | Inject 4 structured headers on every outbound POST; pre-generate `delivery_attempt_id` UUID | Phase 5 |
| `app/api/v1/mock.py` | Add `/mock/echo` — mirrors request headers as JSON (test helper) | Phase 5 |
| `scripts/verify_phase5.py` | Two-suite E2E verification: deduplication + header injection | Phase 5 |
| `docs/PHASE_5_IDEMPOTENCY.md` | This document | Phase 5 |

---

*Previous phase: [Phase 4 — Retries & Backoff](PHASE_4_RETRIES_BACKOFF.md)*
