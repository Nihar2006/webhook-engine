"""
scripts/verify_db.py
~~~~~~~~~~~~~~~~~~~~
Phase 1 verification script.

Demonstrates:
- Inserting a WebhookEndpoint and Event
- Unique constraint enforcement on idempotency_key (IntegrityError)
- Fetching the persisted records back

Run from the project root:
    python scripts/verify_db.py
"""
import asyncio
import sys
import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

# Make sure the project root is on sys.path when running as a script
sys.path.insert(0, ".")

from app.core.database import AsyncSessionLocal
from app.models.endpoint import WebhookEndpoint
from app.models.event import Event, EventStatus


IDEMPOTENCY_KEY = "verify-phase1-" + str(uuid.uuid4())


async def main() -> None:
    print("=" * 60)
    print("Webhook Engine — Phase 1 DB Verification")
    print("=" * 60)

    async with AsyncSessionLocal() as session:
        # ------------------------------------------------------------------
        # 1. Insert a WebhookEndpoint
        # ------------------------------------------------------------------
        endpoint = WebhookEndpoint(
            target_url="https://example.com/webhook",
            secret="super-secret",
            is_active=True,
        )
        session.add(endpoint)
        await session.flush()          # assigns id without committing
        print(f"\n[OK] Inserted endpoint: {endpoint}")

        # ------------------------------------------------------------------
        # 2. Insert an Event with idempotency_key
        # ------------------------------------------------------------------
        event = Event(
            payload={"type": "order.created", "order_id": 42},
            event_type="order.created",
            status=EventStatus.PENDING,
            idempotency_key=IDEMPOTENCY_KEY,
        )
        session.add(event)
        await session.flush()
        print(f"[OK] Inserted event:    {event}")

        await session.commit()

    # ------------------------------------------------------------------
    # 3. Attempt a duplicate insert — must raise IntegrityError
    # ------------------------------------------------------------------
    print(f"\n[TEST] Inserting duplicate idempotency_key={IDEMPOTENCY_KEY!r}...")
    try:
        async with AsyncSessionLocal() as dup_session:
            duplicate = Event(
                payload={"duplicate": True},
                event_type="order.created",
                status=EventStatus.PENDING,
                idempotency_key=IDEMPOTENCY_KEY,   # same key!
            )
            dup_session.add(duplicate)
            await dup_session.flush()
            await dup_session.commit()
        print("[FAIL] Expected IntegrityError but none was raised!")
        sys.exit(1)
    except IntegrityError as exc:
        print(f"[OK] IntegrityError raised as expected: {exc.orig}")

    # ------------------------------------------------------------------
    # 4. Fetch and display the persisted records
    # ------------------------------------------------------------------
    async with AsyncSessionLocal() as fetch_session:
        fetched_endpoint = await fetch_session.get(WebhookEndpoint, endpoint.id)
        fetched_event = (
            await fetch_session.execute(
                select(Event).where(Event.idempotency_key == IDEMPOTENCY_KEY)
            )
        ).scalar_one()

    print("\n--- Fetched records ---")
    print(f"Endpoint : {fetched_endpoint}")
    print(f"Event    : {fetched_event}")
    print(f"Payload  : {fetched_event.payload}")
    print(f"Status   : {fetched_event.status.value}")
    print("\n[PASS] All Phase 1 DB verification checks passed!")


if __name__ == "__main__":
    asyncio.run(main())
