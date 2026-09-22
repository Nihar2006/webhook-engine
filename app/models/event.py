import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class EventStatus(str, enum.Enum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


class Event(Base):
    """An inbound event to be fanned out to registered endpoints."""

    __tablename__ = "event"

    __table_args__ = (
        # Explicit index on idempotency_key (unique constraint already
        # creates one, but being explicit makes the intent clear).
        Index("ix_event_idempotency_key", "idempotency_key", unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[EventStatus] = mapped_column(
        nullable=False,
        default=EventStatus.PENDING,
    )
    # Unique + indexed — DB enforces idempotency at the constraint level.
    idempotency_key: Mapped[str] = mapped_column(
        String,
        nullable=False,
        unique=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<Event id={self.id} "
            f"event_type={self.event_type!r} "
            f"status={self.status.value} "
            f"idempotency_key={self.idempotency_key!r}>"
        )
