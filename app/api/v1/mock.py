"""
app/api/v1/mock.py
~~~~~~~~~~~~~~~~~~
Mock webhook receivers — Phase 4 / Phase 5 / Phase 8 extensions.

Endpoints
---------
POST /api/v1/mock/ok         -- Phase 8: always returns 200 instantly.  Zero
                               latency; used by verify_phase8.py as a fast,
                               reliable delivery target for rate-limit tests.
POST /api/v1/mock/receiver   -- slow (1.5 s) success; used in Phase 2/3 benchmarks.
POST /api/v1/mock/flaky      -- fails with 503 on the first 2 calls per cycle,
                               succeeds with 200 on the 3rd.  Cycles every 3
                               calls so the endpoint works for repeated test runs.
POST /api/v1/mock/failing    -- always returns 500.  Used to exercise
                               max-retries-exhausted behaviour.
POST /api/v1/mock/echo       -- Phase 5: mirrors all received request headers
                               back as a JSON body.
POST /api/v1/mock/secure     -- Phase 7: verifies X-Webhook-Signature HMAC-SHA256
                               against a secret passed as ?secret= query param.
                               Returns 200 (verified), 400 (timestamp expired),
                               or 401 (invalid / missing signature).

Implementation note — flaky call counter
-----------------------------------------
The flaky counter is a module-level integer protected by a threading.Lock.
Uvicorn runs as a single-process ASGI server in dev mode, so this is
deterministic for sequential verify runs.  A multi-process deployment would
need a Redis counter instead.
"""
from __future__ import annotations

import asyncio
import threading
import time

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from app.core.security import verify_webhook_signature

router = APIRouter(prefix="/mock", tags=["mock"])

# ---------------------------------------------------------------------------
# Phase 8 — instant always-200 receiver (zero latency, for rate-limit tests)
# ---------------------------------------------------------------------------

@router.post(
    "/ok",
    summary="Phase 8: Always returns 200 instantly (zero latency)",
)
async def mock_ok() -> dict:
    """
    Instantly returns ``{"status": "ok"}`` with HTTP 200.

    Used by ``verify_phase8.py`` Suite A as the delivery target for the burst
    blast.  Zero latency avoids any artificial delay interfering with
    rate-limit window timing — we want to measure the throttle/deferral
    mechanics, not receiver slowness.
    """
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Phase 2/3 slow receiver
# ---------------------------------------------------------------------------

# Simulated receiver latency — intentionally slow to make blocking pain visible.
_RECEIVER_LATENCY_S: float = 1.5


@router.post(
    "/receiver",
    summary="Simulate a slow webhook receiver",
)
async def mock_receiver() -> dict:
    """
    Pretends to be a third-party webhook consumer.

    Sleeps for ``_RECEIVER_LATENCY_S`` seconds before responding, simulating
    network round-trip + slow processing on the receiving end.  When called
    sequentially this sleep accumulates per endpoint per event, creating an
    obvious throughput ceiling.
    """
    await asyncio.sleep(_RECEIVER_LATENCY_S)
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Phase 4 — flaky receiver (503 × 2, then 200, cycling every 3 calls)
# ---------------------------------------------------------------------------

_flaky_call_count: int = 0
_flaky_lock: threading.Lock = threading.Lock()

# How many leading calls in each cycle return 503 before the cycle succeeds.
_FLAKY_FAIL_FIRST_N: int = 2


@router.post(
    "/flaky",
    summary="Flaky receiver — 503 on first 2 calls per cycle, 200 on 3rd",
)
async def mock_flaky() -> JSONResponse:
    """
    Simulates a transiently failing downstream service.

    **Behaviour** (repeats every 3 calls so the endpoint remains useful
    across multiple verify runs without a server restart):

    =========  =======
    Call mod 3  Status
    =========  =======
    1           503
    2           503
    0           200
    =========  =======

    Celery's retry logic should absorb the first two 503 responses and
    succeed on the third attempt, producing three ``DeliveryAttempt`` rows
    with ``attempt_number`` 1, 2, 3 and ``http_status`` 503, 503, 200.
    """
    global _flaky_call_count

    with _flaky_lock:
        _flaky_call_count += 1
        call_number = _flaky_call_count

    # Calls 1 and 2 of each 3-cycle fail; call 3 succeeds.
    # (call_number - 1) % 3:  0 → fail, 1 → fail, 2 → succeed
    position_in_cycle = (call_number - 1) % (_FLAKY_FAIL_FIRST_N + 1)

    if position_in_cycle < _FLAKY_FAIL_FIRST_N:
        return JSONResponse(
            status_code=503,
            content={
                "error": "Service Unavailable",
                "call_number": call_number,
                "detail": "Simulated transient failure — retry expected",
            },
        )

    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "call_number": call_number,
            "detail": "Recovered after transient failures",
        },
    )


# ---------------------------------------------------------------------------
# Phase 4 — permanently failing receiver (always 500)
# ---------------------------------------------------------------------------

@router.post(
    "/failing",
    summary="Permanently failing receiver — always returns 500",
)
async def mock_failing() -> JSONResponse:
    """
    Simulates a permanently broken downstream service.

    Always returns HTTP 500 Internal Server Error.  Used to exercise the
    max-retries-exhausted code path in ``deliver_to_endpoint_task``:
    the Celery worker retries up to ``max_retries`` times and then writes
    ``Event.status = FAILED`` to Postgres.
    """
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "detail": "Simulated permanent failure"},
    )


# ---------------------------------------------------------------------------
# Phase 5 — echo receiver: mirrors request headers as JSON (for header assert)
# ---------------------------------------------------------------------------

@router.post(
    "/echo",
    summary="Phase 5: Echo all received request headers as JSON body",
)
async def mock_echo(request: Request) -> JSONResponse:
    """
    Returns every HTTP request header as a JSON object.

    Used by ``verify_phase5.py`` Suite B to assert that the four webhook
    delivery headers injected by ``deliver_to_endpoint_task`` arrive intact
    at the destination server:

    * ``X-Webhook-Event-Id``
    * ``X-Webhook-Delivery-Id``
    * ``X-Webhook-Idempotency-Key``
    * ``X-Webhook-Timestamp``

    The Celery worker stores the JSON response body in ``DeliveryAttempt.
    response_body``, which ``verify_phase5.py`` then parses and inspects.
    """
    headers_dict = dict(request.headers)
    return JSONResponse(
        status_code=200,
        content={"received_headers": headers_dict},
    )


# ---------------------------------------------------------------------------
# Phase 7 -- secure receiver: real-time HMAC-SHA256 signature verification
# ---------------------------------------------------------------------------

@router.post(
    "/secure",
    summary="Phase 7: Verify X-Webhook-Signature HMAC-SHA256",
)
async def mock_secure(
    request: Request,
    secret: str = Query(..., description="Shared secret for HMAC-SHA256 verification"),
) -> JSONResponse:
    """
    Verifies the X-Webhook-Signature header injected by deliver_to_endpoint_task.

    The secret query-parameter is the endpoint's shared secret. By
    embedding it in WebhookEndpoint.target_url (e.g. /api/v1/mock/secure?secret=abc),
    the delivery worker passes it transparently without any worker-side changes.

    Response codes:
      200  Signature valid and timestamp fresh.
      400  Timestamp > 300 s old (replay attack window exceeded).
      401  Signature missing, malformed, or HMAC mismatch.
    """
    raw_body: bytes = await request.body()
    sig_header: str = request.headers.get("X-Webhook-Signature", "")

    if not sig_header:
        return JSONResponse(
            status_code=401,
            content={"error": "missing_signature", "detail": "X-Webhook-Signature header not present"},
        )

    # Check timestamp freshness separately to return 400 (not 401) on expiry.
    t_val: int | None = None
    try:
        for part in sig_header.split(","):
            part = part.strip()
            if part.startswith("t="):
                t_val = int(part[2:])
                break
    except (ValueError, AttributeError):
        pass

    if t_val is not None and abs(int(time.time()) - t_val) > 300:
        return JSONResponse(
            status_code=400,
            content={"error": "timestamp_expired"},
        )

    # Full HMAC verification (constant-time compare_digest inside).
    # Pass tolerance=0 since freshness is already checked above.
    if verify_webhook_signature(secret, sig_header, raw_body, tolerance=0):
        return JSONResponse(
            status_code=200,
            content={"status": "verified", "detail": "Signature valid"},
        )

    return JSONResponse(
        status_code=401,
        content={"error": "invalid_signature", "detail": "HMAC mismatch"},
    )
