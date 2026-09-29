"""
app/api/v1/mock.py
~~~~~~~~~~~~~~~~~~
Mock webhook receivers — Phase 4 / Phase 5 extensions.

Endpoints
---------
POST /api/v1/mock/receiver   — slow (1.5 s) success; used in Phase 2/3 benchmarks.
POST /api/v1/mock/flaky      — fails with 503 on the first 2 calls per cycle,
                               succeeds with 200 on the 3rd.  Cycles every 3
                               calls so the endpoint works for repeated test runs.
POST /api/v1/mock/failing    — always returns 500.  Used to exercise
                               max-retries-exhausted behaviour.
POST /api/v1/mock/echo       — Phase 5: mirrors all received request headers
                               back as a JSON body.  Used by verify_phase5.py
                               Suite B to assert idempotency headers arrive
                               intact at the destination server.

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

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/mock", tags=["mock"])

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
