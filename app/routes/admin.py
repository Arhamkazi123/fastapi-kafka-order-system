import json
import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from confluent_kafka import Consumer, Producer, KafkaError

from app.auth import get_current_user
from app.core import get_producer, TOPIC_ORDERS, TOPIC_DLQ
from app.models import User

router = APIRouter(prefix="/admin", tags=["admin"])

@router.get("/dlq")
def get_dlq_messages(current_user: User = Depends(get_current_user)):

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dlq-viewer-{current_user.id}-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_DLQ])

    messages = []
    empty_polls = 0

    while empty_polls < 15:   # 15 × 0.5s = 7.5 seconds — enough for partition assignment
        msg = consumer.poll(timeout=0.5)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            # _PARTITION_EOF = reached end of partition, not a real error
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
            continue

        empty_polls = 0

        try:
            dlq_event = json.loads(msg.value().decode("utf-8"))
            messages.append({
                "kafka_offset": msg.offset(),
                "kafka_partition": msg.partition(),
                **dlq_event,
            })
        except Exception:
            continue

    consumer.close()

    return {
        "total": len(messages),
        "failed_orders": messages,
    }


@router.post("/dlq/{order_id}/replay", status_code=status.HTTP_202_ACCEPTED)
def replay_dlq_message(
    order_id: str,
    current_user: User = Depends(get_current_user),
    producer: Producer = Depends(get_producer),
):
    # Read DLQ to find this specific order
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dlq-replay-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_DLQ])

    found = None
    empty_polls = 0

    while empty_polls < 15:
        msg = consumer.poll(timeout=0.5)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
            continue

        empty_polls = 0

        try:
            dlq_event = json.loads(msg.value().decode("utf-8"))
            if dlq_event.get("original_key") == order_id:
                found = dlq_event
                break
        except Exception:
            continue

    consumer.close()

    # 404 if order not in DLQ
    if not found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order '{order_id}' not found in DLQ",
        )

    # Rebuild the original event and republish to orders topic
    original = found["original_value"]

    replay_event = {
        "event_type": "order.placed",
        "order_id": original["order_id"],
        "user_id": original["user_id"],
        "items": original["items"],
        "total": original["total"],
        "correlation_id": found.get("correlation_id", str(uuid.uuid4())),
        "created_at": original.get("created_at", ""),
        "replayed": True,              # flag so you can track replays in logs
    }

    producer.produce(
        topic=TOPIC_ORDERS,
        key=order_id,
        value=json.dumps(replay_event).encode("utf-8"),
    )
    producer.poll(0)

    return {
        "message": f"Order '{order_id}' replayed successfully",
        "order_id": order_id,
        "replayed_to_topic": TOPIC_ORDERS,
    }
