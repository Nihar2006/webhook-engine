# Webhook Engine

[![Python 3.13](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Celery](https://img.shields.io/badge/Celery-5.x-37814A?logo=celery&logoColor=white)](https://docs.celeryq.dev/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![SQLAlchemy 2.0](https://img.shields.io/badge/SQLAlchemy-2.0-CA3A31)](https://docs.sqlalchemy.org/en/20/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

A **production-grade, asynchronous webhook delivery engine** built as an
eight-phase engineering deep-dive. Each phase layers a real production concern
on top of the previous — from raw synchronous HTTP to cryptographic payload
signing, atomic rate limiting, and a full dead-letter queue with replay.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLIENT / PUBLISHER                        │
└──────────────────────┬───────────────────────────────────────────┘
                       │  POST /api/v1/events  (HTTP 202 Accepted)
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                      FastAPI  (Ingestion Layer)                  │
│                                                                  │
│  • Dual-layer idempotency gate (memory cache + ON CONFLICT)      │
│  • Validates payload schema (Pydantic)                           │
│  • Persists Event row → PostgreSQL (status = PENDING)            │
│  • Enqueues deliver_webhook_task → Redis (Celery broker)         │
│  • Returns 202 Accepted in < 5 ms  ◄── decoupled boundary        │
└──────────────────────────────────────┬───────────────────────────┘
                                       │  Celery task message
                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                   Redis  (Broker + Rate-Limit Store)             │
│                                                                  │
│  • Celery task queue (deliver_webhook_task)                      │
│  • Atomic Lua rate-limit counters per endpoint                   │
│  • Celery result backend                                         │
└──────────────────────────────────────┬───────────────────────────┘
                                       │  task.delay()
                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│              Celery Worker  (Delivery + Retry Layer)             │
│                                                                  │
│  deliver_webhook_task (dispatcher, no retries)                   │
│    └─► deliver_to_endpoint_task (per endpoint, max_retries=4)    │
│          • Redis Lua rate-limit gate (atomic, O(1))              │
│          • HMAC-SHA256 payload signing (optional per endpoint)   │
│          • HTTP POST to subscriber URL (httpx)                   │
│          • Full-Jitter exponential backoff on 5xx                │
│          • DeliveryAttempt row → PostgreSQL                      │
│          • On exhaustion → EventStatus.DEAD_LETTER               │
└──────────────────────────────────────┬───────────────────────────┘
                                       │  writes
                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                     PostgreSQL  (State Store)                    │
│                                                                  │
│  events           — status: PENDING / DELIVERED / FAILED / DLQ  │
│  delivery_attempt — per-attempt audit trail                      │
│  webhook_endpoint — subscriber registry                          │
└──────────────────────────────────────────────────────────────────┘
                                       │
                          ┌────────────┘
                          ▼
               Admin API  GET  /api/v1/dlq
                          POST /api/v1/events/{id}/replay
```

---

## Six Production Engineering Pillars

### 1 · Async Worker Decoupling

The FastAPI ingestion path and the Celery delivery path are fully **decoupled**
at the Redis broker boundary.  The HTTP handler commits an `Event` row and
enqueues a Celery task, then immediately returns **HTTP 202 Accepted** — the
caller is never blocked on subscriber availability.

| | Phase 2 (Sync) | Phase 3+ (Async) |
|---|---|---|
| Ingestion latency | ~2 000–3 000 ms (waits for HTTP delivery) | < 5 ms (queue only) |
| Subscriber failure impact | Caller sees 5xx | Celery retries invisibly |
| Horizontal scaling | Must scale API servers | Scale workers independently |

### 2 · Full-Jitter Exponential Backoff

Naive fixed-interval retries cause a **thundering herd**: all failed workers
wake up at the same instant and overwhelm the recovering service again.

```
# app/core/retry.py
cap   = min(max_delay, base_delay * 2 ** attempt)   # exponential ceiling
sleep = random.uniform(0, cap)                       # full-jitter draw
```

Workers that fail at the same moment independently draw from `[0, cap]`,
spreading retry load across the full interval.  Per the AWS Architecture Blog
(2015), Full Jitter reduces mean task completion time by ~50% vs naive
exponential backoff under high concurrency.

**Retry budget:** 1 initial attempt + 4 retries = 5 total.  On exhaustion,
the event transitions to `DEAD_LETTER`.

### 3 · Dual-Layer Idempotency

Duplicate event submissions are suppressed at two independent layers:

**Layer 1 — Producer gate (PostgreSQL)**
```sql
INSERT INTO events (idempotency_key, ...) VALUES (...)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id
```
Prevents duplicate rows even under concurrent submissions from multiple
FastAPI replicas.

**Layer 2 — Consumer headers (Celery)**
Every delivery includes stable, per-event tracing headers:
```
X-Webhook-Event-Id:        <stable across all retries>
X-Webhook-Idempotency-Key: <stable across all retries>
X-Webhook-Delivery-Id:     <unique per attempt>
X-Webhook-Timestamp:       <ISO-8601 dispatch time>
```
Subscribers can use `X-Webhook-Event-Id` or `X-Webhook-Idempotency-Key` to
deduplicate on their side, even when Celery retries a transient failure.

The ingestion API also returns `HTTP 200 + X-Idempotent-Replay: true` on
duplicate submissions, giving publishers a clear signal without creating
duplicate database rows.

### 4 · Dead-Letter Queue (DLQ) & Admin Replay Engine

When all retries are exhausted, the event transitions to `EventStatus.DEAD_LETTER`
and a structured `[DLQ]` log line is emitted.  Operators can:

```bash
# List all dead-lettered events
GET /api/v1/dlq

# Replay a specific event (re-enqueues Celery task, resets status to PENDING)
POST /api/v1/events/{event_id}/replay
```

The replay path is idempotent — replaying an already-delivered event is a no-op.
The admin can update the endpoint's `target_url` between failure and replay to
fix misconfigured subscriber URLs without losing the original event payload.

### 5 · Cryptographic Security (HMAC-SHA256)

Webhook deliveries to endpoints configured with a `secret` are signed using a
**Stripe-compatible** HMAC-SHA256 scheme:

```
X-Webhook-Signature: t=<unix_timestamp>,v1=<hmac_sha256_hex>

Signed payload: f"t={timestamp}.{raw_json_body}"
```

Security properties:
- **Unforgeable** — requires the shared secret to produce a valid `v1=` digest.
- **Tamper-evident** — the body is part of the signed message; any modification
  invalidates the digest.
- **Replay-protected** — the timestamp is embedded in the signed payload (not a
  separate header); attackers cannot strip or alter it without breaking the HMAC.
  Default tolerance: 300 seconds.
- **Timing-attack resistant** — verification uses `hmac.compare_digest`
  (constant-time) instead of `==`.

### 6 · Atomic Per-Endpoint Rate Limiting

Rate limiting in distributed systems has one hard requirement: **atomicity**.
A Python read-check-increment sequence has a race condition under concurrent
workers.  This is solved with a **Redis Lua script** — Redis executes it as a
single atomic operation:

```lua
local current = redis.call('INCR', key)
if current == 1 then
    redis.call('EXPIRE', key, window)  -- anchor TTL on first request only
end
if current > limit then
    return 0  -- REJECTED
end
return 1      -- ALLOWED
```

Why `EXPIRE` only on `current == 1`?  Resetting TTL on every request would
make the window "1 second after the last request", preventing rate limits from
ever triggering under sustained load.

- **O(1)** time and memory per endpoint.
- Zero race conditions — verified by the Suite 6 stress test (20 threads, limit=5:
  exactly 5 allowed, 15 rejected, counter=20, no lost increments).
- Throttled tasks defer via `self.retry(countdown=window, max_retries=None)` —
  bypasses the 4-retry failure budget entirely.

---

## Benchmark & Performance Results

### Sync vs Async Ingestion Comparison

| Metric | Phase 2 (Synchronous) | Phase 3+ (Async Celery) |
|---|---|---|
| Ingestion latency | ~2 000–3 000 ms | **< 5 ms** |
| Bottleneck | Blocking HTTP to subscriber | None (queue only) |
| Failure isolation | API returns 5xx | Worker retries silently |
| Scalability | Tied to subscriber SLA | Independent |

### Phase 9 Live Load Test — 200 Events / 10 Concurrent Workers

> Run `.venv\Scripts\python.exe scripts/load_test.py` to reproduce.

| Metric | Result |
|---|---|
| Events dispatched | 200 |
| Successful ingestion | 200 / 200 (100%) |
| HTTP 202 rate | 100% |
| Ingestion wall time | 2.50 s |
| **Ingestion RPS** | **80.1 req/s** |
| Latency p50 (Median) | 101 ms |
| Latency p95 | 519 ms |
| Latency p99 | 555 ms |
| Latency Max | 562 ms |
| Events delivered (DB) | 200 / 200 (100%) |
| Delivery throughput | 3.1 events/s |
| Drain wall time | 64.1 s |
| **Total test elapsed** | **66.6 s** |

> **Environment:** Windows 11, Ryzen 7, 32 GB RAM, Docker Desktop (PostgreSQL + Redis).
> Celery worker runs in `-P solo` mode (single-threaded, Windows constraint).
> In a Linux production environment with `prefork` workers the delivery throughput
> and drain time scale linearly with worker concurrency.

---

## Master Verification Suite

`scripts/verify_master.py` runs **6 sequential assertion suites** against the
live stack in ~17 seconds:

| Suite | Phases | What it verifies |
|---|---|---|
| 1 — Connectivity & Core Ingestion | 1-3 | `/healthz`, Redis PING, 202 response, DELIVERED status |
| 2 — Transient Failures & Backoff | 4 | 3 attempts `[503, 503, 200]`, DELIVERED |
| 3 — Idempotency & Headers | 5 | Duplicate deduplication, all 4 tracing headers |
| 4 — DLQ & Replay Engine | 6 | DEAD_LETTER after 5 failures, replay → DELIVERED |
| 5 — HMAC-SHA256 Signing | 7 | Live delivery + 4 unit assertions (tamper, expiry, wrong secret, `compare_digest`) |
| 6 — Atomic Rate Limiting | 8 | 20 threads, limit=5 → exactly 5 allowed, 15 rejected |

```bash
.venv\Scripts\python.exe scripts/verify_master.py
```

---

## Local Setup & Quickstart

### 1. Prerequisites

- Python 3.13
- Docker Desktop (for PostgreSQL + Redis)

### 2. Clone & create virtualenv

```bash
git clone https://github.com/Nihar2006/webhook-engine.git
cd webhook-engine
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Start infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL on port `5432` and Redis on port `6379`.

### 4. Run database migrations

```bash
alembic upgrade head
```

### 5. Start the FastAPI server

```bash
uvicorn app.main:app --reload --port 8000
```

### 6. Start the Celery worker (separate terminal)

```bash
# Windows (solo pool — no os.fork())
celery -A app.core.celery_app worker --loglevel=info -P solo

# macOS / Linux
celery -A app.core.celery_app worker --loglevel=info
```

### 7. Register a webhook endpoint

```bash
curl -X POST http://localhost:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -d '{"target_url": "https://your-receiver.example.com/webhook"}'
```

### 8. Dispatch an event

```bash
curl -X POST http://localhost:8000/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "order.created",
    "payload": {"order_id": "abc-123"},
    "idempotency_key": "unique-key-001"
  }'
```

Response:
```json
{"event_id": "...", "status": "ACCEPTED"}
```

### 9. Run the full verification suite

```bash
.venv\Scripts\python.exe scripts/verify_master.py
```

### 10. Run the load test

```bash
.venv\Scripts\python.exe scripts/load_test.py
```

---

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Health check — returns `{"status": "ok"}` |
| `POST` | `/api/v1/events` | Ingest an event — returns `202 Accepted` |
| `POST` | `/api/v1/events/{id}/replay` | Replay a dead-lettered event |
| `GET` | `/api/v1/endpoints` | List all registered endpoints |
| `POST` | `/api/v1/endpoints` | Register a new webhook endpoint |
| `GET` | `/api/v1/dlq` | List all dead-lettered events |
| `GET` | `/docs` | Interactive Swagger UI |

---

## Project Structure

```
webhook-engine/
├── app/
│   ├── api/v1/
│   │   ├── endpoints.py   # Subscriber registry API
│   │   ├── events.py      # Ingestion + replay API
│   │   ├── dlq.py         # Dead-letter queue API
│   │   └── mock.py        # Mock receiver endpoints (testing)
│   ├── core/
│   │   ├── celery_app.py  # Celery singleton + configuration
│   │   ├── config.py      # Pydantic Settings
│   │   ├── database.py    # Async SQLAlchemy engine + session factory
│   │   ├── rate_limiter.py# Redis Lua atomic rate limiter
│   │   ├── retry.py       # Full-Jitter exponential backoff
│   │   └── security.py    # HMAC-SHA256 signing + verification
│   ├── models/
│   │   ├── delivery.py    # DeliveryAttempt ORM model
│   │   ├── endpoint.py    # WebhookEndpoint ORM model
│   │   └── event.py       # Event ORM model + EventStatus enum
│   ├── tasks/
│   │   └── delivery.py    # Celery delivery + retry tasks
│   └── main.py            # FastAPI application entry point
├── alembic/               # Database migrations
├── scripts/
│   ├── verify_master.py   # Master sanity verification suite (Phases 1-8)
│   └── load_test.py       # Phase 9 high-concurrency load test
├── docker-compose.yml
└── requirements.txt
```

---

## License

MIT — see [LICENSE](LICENSE) for details.
