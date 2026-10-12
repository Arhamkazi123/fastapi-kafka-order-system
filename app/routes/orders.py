import json
from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session, select
from confluent_kafka import Producer

from app.database import get_session
from app.auth import get_current_user
from app.core import get_producer, TOPIC_ORDERS
from app.models import User, Order, OrderCreate, OrderRead, OrderEvent

router = APIRouter(prefix="/orders", tags=["orders"])


@router.get("/", response_model=list[OrderRead])
def get_my_orders(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = session.exec(
        select(Order).where(Order.user_id == current_user.id)
    ).all()

    return [
        OrderRead(
            order_id=o.order_id,
            user_id=o.user_id,
            items=json.loads(o.items),
            total=o.total,
            status=o.status,
            error_msg=o.error_msg,
            created_at=o.created_at,
        )
        for o in orders
    ]


@router.get("/{order_id}", response_model=OrderRead)
def get_order(
    order_id: str,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    # Fetch order from DB by primary key
    order = session.get(Order, order_id)

    # 404 if order doesn't exist
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order '{order_id}' not found",
        )

    # 403 if order belongs to a different user
    # Users should only see their own orders
    if order.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this order",
        )

    # Convert items JSON string back to list before returning
    return OrderRead(
        order_id=order.order_id,
        user_id=order.user_id,
        items=json.loads(order.items),
        total=order.total,
        status=order.status,
        error_msg=order.error_msg,
        created_at=order.created_at,
    )


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=OrderRead)
def create_order(
    order_data: OrderCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
    producer: Producer = Depends(get_producer),
):
    db_order = Order(
        user_id=current_user.id,
        items=json.dumps(order_data.items),
        total=order_data.total,
    )
    session.add(db_order)
    session.commit()
    session.refresh(db_order)

    event = OrderEvent.from_order(db_order)

    producer.produce(
        topic=TOPIC_ORDERS,
        key=event.order_id,
        value=event.to_kafka_bytes(),
    )

    producer.poll(0)

    return OrderRead(
        order_id=db_order.order_id,
        user_id=db_order.user_id,
        items=json.loads(db_order.items),
        total=db_order.total,
        status=db_order.status,
        error_msg=db_order.error_msg,
        created_at=db_order.created_at,
    )
