# Phase 2 -- Synchronous Delivery Baseline

> **Webhook Engine** * HTTP API, In-Band Delivery & Pain Verification

---

## 1. What Was Implemented

Phase 2 builds the first runnable version of the webhook engine. The codebase gains a
full HTTP API layer, Pydantic validation schemas, and a synchronous delivery loop that
intentionally delivers webhook payloads inside the HTTP request-response cycle.
Every design decision in Phase 2 has a dual purpose: make the system work, **and** make
its bottleneck measurable.

### New files

| File | Role |
|---|---|
| `app/main.py` | FastAPI application entry point; lifespan stub for Phase 3 |
| `app/schemas/endpoint.py` | `WebhookEndpointCreate` / `WebhookEndpointRead` |
| `app/schemas/event.py` | `EventCreate` / `EventRead` |
| `app/schemas/delivery.py` | `DeliveryAttemptRead` |
| `app/api/v1/endpoints.py` | `POST /api/v1/endpoints`, `GET /api/v1/endpoints` |
| `app/api/v1/events.py` | `POST /api/v1/events` -- synchronous delivery loop |
| `app/api/v1/mock.py` | `POST /api/v1/mock/receiver` -- slow mock receiver (1.5 s) |
| `scripts/benchmark_sync.py` | Benchmark script that captures real timing evidence |

### API surface

```
GET  /healthz                    -- liveness check
POST /api/v1/endpoints           -- register a webhook endpoint (201)
GET  /api/v1/endpoints           -- list active endpoints
POST /api/v1/events              -- dispatch an event (sync delivery, 200)
POST /api/v1/mock/receiver       -- slow mock target (used by benchmark)
```

### Schemas

**`WebhookEndpointCreate`** (request)
```json
{ "target_url": "https://...", "secret": "optional-hmac-key" }
```

**`EventCreate`** (request)
```json
{ "event_type": "order.created", "payload": {...}, "idempotency_key": "uuid-v4" }
```

**`POST /api/v1/events`** response (200)
```json
{
  "event": { "id": "...", "status": "DELIVERED", ... },
  "deliveries": [
    { "endpoint_id": "...", "http_status": 200, "attempt_number": 1, ... }
  ]
}
```
Duplicate `idempotency_key` returns **409 Conflict** immediately without re-delivering.

---

## 2. The Architectural Problem: Synchronous In-Band Delivery

### What happens inside `POST /api/v1/events`

```
Client                  FastAPI worker              Receiver A   Receiver B
  |                          |                           |            |
  |-- POST /api/v1/events -->|                           |            |
  |                          |-- INSERT Event (PENDING) -|            |
  |                          |                           |            |
  |                          |-- POST payload ---------->|            |
  |                          |<-- 200 OK (1.5 s later) --|            |
  |                          |-- INSERT DeliveryAttempt  |            |
  |                          |                           |            |
  |                          |-- POST payload ----------------------->|
  |                          |<-- 200 OK (1.5 s later) --------------|
  |                          |-- INSERT DeliveryAttempt  |            |
  |                          |                           |            |
  |                          |-- UPDATE Event (DELIVERED)|            |
  |                          |-- COMMIT                  |            |
  |<-- 200 OK (3.0+ s) ------|                           |            |
```

The FastAPI worker **cannot process any other request** while it is `await`-ing the
outbound HTTP calls. Even though `await` is non-blocking at the Python level, the
**worker's logical capacity** is consumed for the entire duration. Under asyncio a single
worker can interleave coroutines, but each `await client.post(...)` gives up the event
loop only until the response arrives from *that specific remote*. The sequential loop
means the worker is fully occupied for:

```
T_block = n_endpoints x T_receiver_latency
         = 2 endpoints x 1.5 s
         = 3.0 s per event
```

### Why this degrades under realistic conditions

| Factor | Effect |
|---|---|
| **More endpoints** | T_block grows linearly. 10 endpoints at 1 s each = 10 s/event |
| **Slower receivers** | A receiver timing out at 5 s = 10 s blocked for 2 endpoints |
| **Concurrent clients** | uvicorn spawns N workers; all N can be simultaneously blocked |
| **Retry storms** | Failed deliveries retried immediately = more blocking time stacked |
| **P99 receiver latency spikes** | One slow receiver extends EVERY event dispatched in that batch |

### The event-loop starvation diagram

```
Worker 1:  [Event A: await recv-1][await recv-2][commit]
Worker 2:  [Event B: await recv-1][await recv-2][commit]
Worker 3:  [Event C: await recv-1][await recv-2][commit]
           |------ 3.0 s --------|------ 3.0 s --------|
                                  ^ No worker is free here to accept new requests
```

With a 1-worker server the throughput ceiling is `1 / 3.0 = 0.33 events/s`.
With 4 workers it is `4 / 3.0 = 1.33 events/s`.
This is a **hard ceiling tied to receiver latency** -- a metric the webhook engine does
not control.

---

## 3. Benchmark Results

The benchmark registers 2 endpoints at `http://localhost:8000/api/v1/mock/receiver`
(each sleeps 1.5 s), then dispatches 10 events sequentially via `POST /api/v1/events`.

```
==========================================================
  Webhook Engine - Phase 2 Synchronous Benchmark
==========================================================

  Registering 2 mock receiver(s)...
  [+] Registered endpoint 1: a89ebb39-9c60-40f2-8b2c-ee53ff0e8b6a
  [+] Registered endpoint 2: be1eb2da-be56-4a1c-b0f0-dfbc1db5ac7d

  Dispatching 10 events sequentially...
  (Each call blocks until ALL endpoints respond)

  ----------------------------------------------------------
    #    Elapsed (s)   Status       Deliveries
  ----------------------------------------------------------
    1           3.162   DELIVERED   2 attempt(s)
    2           3.097   DELIVERED   2 attempt(s)
    3           3.033   DELIVERED   2 attempt(s)
    4           3.073   DELIVERED   2 attempt(s)
    5           3.009   DELIVERED   2 attempt(s)
    6           3.010   DELIVERED   2 attempt(s)
    7           3.003   DELIVERED   2 attempt(s)
    8           3.042   DELIVERED   2 attempt(s)
    9           3.016   DELIVERED   2 attempt(s)
   10           3.017   DELIVERED   2 attempt(s)

==========================================================
  BENCHMARK SUMMARY - Synchronous Delivery Baseline
==========================================================
  Endpoints registered : 2
  Events dispatched    : 10
  Total wall-clock time:   30.461 s
  Average per event    :    3.046 s
  p95 latency          :    3.097 s
  Throughput           :    0.328 events/s
==========================================================

  INTERPRETATION
  Each event blocks the worker for ~3.0 s
  (2 endpoints x ~1.5 s receiver latency each).
  Throughput scales inversely with receiver count and latency.
  Adding a 3rd slow endpoint would push avg above 4.5 s/event.
  --> This is the architectural pain Phase 3 (Celery) resolves.
```

### Key observations

| Metric | Value | Interpretation |
|---|---|---|
| Average per event | **3.046 s** | Almost exactly 2 x 1.5 s -- confirms sequential execution |
| p95 latency | **3.097 s** | Very low variance; the bottleneck is deterministic |
| Total for 10 events | **30.461 s** | No parallelism; each event waits for the previous to complete |
| Throughput | **0.328 events/s** | Hard ceiling at ~20 events/min with only 2 endpoints |
| First-event overhead | **3.162 s** | Slightly higher -- includes DB insert + connection pool warmup |

The numbers confirm the theoretical model exactly. The p95 barely exceeds the average,
which means the bottleneck is not intermittent -- it is structural.

---

## 4. Transition to Phase 3: Why Celery + Redis

The benchmark makes the problem concrete: **delivery latency is receiver-imposed, not
engine-imposed**. No amount of optimising the FastAPI handler will change the fact that
a receiver taking 2 s holds the worker for 2 s per endpoint.

### The only correct architectural fix

Move delivery **off the request path** entirely:

```
Phase 2 (synchronous):
  Client --> [FastAPI handler: insert + deliver + commit] --> Client
  Duration: n_endpoints x T_receiver

Phase 3 (async via Celery):
  Client --> [FastAPI handler: insert + enqueue task] --> Client (< 50 ms)
                               |
                       [Celery worker: deliver + record + retry]
                               |
                       [Redis: task queue + result store]
```

The HTTP response is returned **before any delivery happens**. Delivery becomes a
background concern, independent of the caller's latency budget.

### Why Celery specifically

| Requirement | Celery solution |
|---|---|
| Per-task retry with exponential backoff | `autoretry_for`, `max_retries`, `countdown` |
| Dead-letter / permanent failure tracking | `on_failure` hook, writes to `DeliveryAttempt` |
| Visibility into pending / running tasks | Celery Flower dashboard or Redis key inspection |
| Fan-out (one event to N endpoints) | One Celery task per endpoint per event |
| Broker already in the stack | Redis 7 is already running from Phase 1 |

### Why Redis as the broker (not RabbitMQ)

Redis Streams provide exactly-once delivery semantics sufficient for this use case,
Redis is already in the `docker-compose.yml` from Phase 1, and the ops cost of
introducing a second message broker (RabbitMQ) is not justified at this stage.

### The P3 contract

A Phase 3 `POST /api/v1/events` will:
1. Insert `Event(status=PENDING)` -- identical to Phase 2.
2. Enqueue one Celery task per active endpoint to Redis (< 5 ms total).
3. Return `202 Accepted` + `{ "event_id": "...", "queued_tasks": N }`.
4. Each Celery worker picks up its task, delivers, and writes `DeliveryAttempt`.
5. A final task (or delivery hook) updates `Event.status`.

The caller is **never blocked** by receiver latency. Throughput becomes limited only
by the number of Celery workers, which can be scaled horizontally.

---

## 5. Interview Defense

### Q1 -- "Why not just fire the delivery in a background `asyncio.Task` within the same FastAPI worker?"

**A:** You could do `asyncio.create_task(deliver())` and return the response immediately.
This solves the caller latency problem but creates three new ones:
(a) **Durability** -- if the process crashes, the in-memory task vanishes; no retry,
no record of the attempt.
(b) **Retry state** -- you cannot persist backoff state, attempt counts, or errors across
process restarts without an external store.
(c) **Observability** -- you cannot query "how many tasks are pending?" or "which
endpoint is failing?" without a separate mechanism.
Celery with a Redis broker gives you durable task storage, retry logic, and visibility
out of the box. The `asyncio.create_task` shortcut is fine for fire-and-forget
notifications; it is wrong for a delivery-guarantee system.

---

### Q2 -- "Your benchmark shows 0.33 events/s. Is that actually a problem for a real webhook system?"

**A:** Yes -- and the problem compounds non-linearly with realistic parameters.
A customer with 50 registered endpoints (common for enterprise integration platforms)
and receivers averaging 500 ms latency would see 25 s per event -- under 2.5 events/min
from a single worker. More importantly, the throughput is **receiver-controlled**, meaning
a misbehaving subscriber can drag down delivery for everyone sharing that worker.
In contrast, the Phase 3 design isolates each delivery to its own Celery task: a slow
or failing receiver stalls only its own task and its own retry queue, not the API or
other deliveries.

---

### Q3 -- "The `POST /api/v1/events` response contains full `DeliveryAttempt` records. Does that design survive in Phase 3?"

**A:** No, and that is intentional. In Phase 2 the response contains delivery results
because delivery happens synchronously before the response is sent -- the data exists.
In Phase 3, delivery has not happened when the response is returned, so the response
changes to `202 Accepted` with a task queue acknowledgement: `{ "event_id": "...",
"queued_tasks": 2 }`. The client can poll `GET /api/v1/events/{id}` to check
`Event.status` and `GET /api/v1/events/{id}/deliveries` to see `DeliveryAttempt` rows
as they are written by Celery workers. This is the standard webhook provider pattern
(Stripe, GitHub, Shopify all return `202` for event ingestion).

---

*Document generated: 2026-09-23 * Webhook Engine Phase 2*
