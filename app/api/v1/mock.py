"""
app/api/v1/mock.py
~~~~~~~~~~~~~~~~~~
Mock webhook receiver — simulates a slow third-party endpoint.

Used exclusively by the Phase 2 benchmark to demonstrate the throughput ceiling
of synchronous, in-band delivery.
"""
import asyncio

from fastapi import APIRouter

router = APIRouter(prefix="/mock", tags=["mock"])

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
