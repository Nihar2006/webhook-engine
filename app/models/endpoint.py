import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class WebhookEndpoint(Base):
    """A registered endpoint that will receive webhook deliveries."""

    __tablename__ = "webhook_endpoint"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    target_url: Mapped[str] = mapped_column(String, nullable=False)
    secret: Mapped[str | None] = mapped_column(String, nullable=True)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<WebhookEndpoint id={self.id} "
            f"target_url={self.target_url!r} "
            f"is_active={self.is_active}>"
        )
