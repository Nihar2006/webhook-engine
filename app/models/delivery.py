import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class DeliveryAttempt(Base):
    """Records each attempt to deliver an Event to a WebhookEndpoint."""

    __tablename__ = "delivery_attempt"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("event.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    endpoint_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("webhook_endpoint.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_number: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships (lazy="raise" to prevent accidental N+1 queries)
    event = relationship("Event", back_populates=None, lazy="raise")
    endpoint = relationship("WebhookEndpoint", back_populates=None, lazy="raise")

    def __repr__(self) -> str:
        return (
            f"<DeliveryAttempt id={self.id} "
            f"event_id={self.event_id} "
            f"endpoint_id={self.endpoint_id} "
            f"attempt={self.attempt_number} "
            f"http_status={self.http_status}>"
        )
