import uuid
import json
from typing import Optional
from datetime import datetime, timezone
from sqlmodel import SQLModel, Field


class OrderBase(SQLModel):
	total: float = Field(gt=0, description="Order total must be greater than 0")


class OrderCreate(OrderBase):
	items: list[str] = Field(min_length=1, description="At least one item required")


class OrderRead(OrderBase):
    order_id: str
    user_id: int
    items: list[str]
    status: str
    error_msg: Optional[str]
    created_at: datetime


class Order(OrderBase, table=True):
    order_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True,
    )
    user_id: int = Field(foreign_key="user.id")
    items: str
    status: str = Field(default="pending")

    error_msg: Optional[str] = Field(default=None)

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class OrderEvent(SQLModel):
    event_type: str = "order.placed"
    order_id: str
    user_id: int
    items: list[str]
    total: float
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4())
    )
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_kafka_bytes(self) -> bytes:
        """Serialize this event to JSON bytes for Kafka produce()."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_order(cls, order: Order) -> "OrderEvent":
        """Build a Kafka event from a DB Order object."""
        return cls(
            order_id=order.order_id,
            user_id=order.user_id,
            items=json.loads(order.items),
            total=order.total,
        )
