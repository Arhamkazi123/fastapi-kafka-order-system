"""
Order Worker
============
Reads from "orders" topic, validates each order, updates DB status,
and routes to "orders.confirmed" or "orders.dlq".

Run this as a separate process:
    python -m app.workers.order_worker

It runs forever — polling Kafka and processing orders.
"""

import json
import traceback
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from sqlmodel import Session

from app.core.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDERS,
    TOPIC_CONFIRMED,
    TOPIC_DLQ,
)
from app.database import engine
from app.models import Order

MAX_RETRIES = 3   # How many times to retry a failing order before sending to DLQ

def validate_order(event: dict) -> None:
    total = event.get("total")
    items = event.get("items", [])

    if not items:
        raise ValueError("Order must have at least one item")

    if total is None:
        raise ValueError("Order total is missing")

    if total <= 0:
        raise ValueError(f"Invalid total: {total}. Must be greater than 0")

    if total > 10_000:
        raise ValueError(f"Total {total} exceeds maximum allowed order value of $10,000")


def send_to_dlq(producer: Producer, original_msg, error: Exception, attempts: int) -> None:
    """
    Wrap the failed message with error context and send to DLQ topic.
    Preserves the original data so it can be replayed later.
    """
    dlq_payload = {
        # Original Kafka metadata — so admin knows where it came from
        "original_topic":     original_msg.topic(),
        "original_partition": original_msg.partition(),
        "original_offset":    original_msg.offset(),
        "original_key":       original_msg.key().decode("utf-8") if original_msg.key() else None,

        # Original order data — so admin can replay it
        "original_value": json.loads(original_msg.value().decode("utf-8")),

        # What went wrong
        "error_type":      type(error).__name__,
        "error_message":   str(error),
        "error_traceback": traceback.format_exc(),

        # Metadata
        "failed_at":      datetime.now(timezone.utc).isoformat(),
        "retry_attempts": attempts,
        "consumer_group": "order-processors",
    }

    producer.produce(
        topic=TOPIC_DLQ,
        key=original_msg.key(),
        value=json.dumps(dlq_payload).encode("utf-8"),
    )
    producer.flush()

def process_event(event: dict, session: Session, producer: Producer) -> None:
    """
    Core processing for a single order event:
    1. Validate business rules
    2. Update DB status to "confirmed"
    3. Produce to "orders.confirmed" topic
    """
    order_id = event["order_id"]

    # Validate
    validate_order(event)

    # Fetch the order from DB and update status
    order = session.get(Order, order_id)
    if not order:
        raise ValueError(f"Order '{order_id}' not found in DB")

    order.status = "confirmed"
    order.updated_at = datetime.now(timezone.utc)
    session.commit()

    # Produce to orders.confirmed so notification worker can pick it up
    confirmed_event = {
        "event_type":       "order.confirmed",
        "order_id":         order_id,
        "user_id":          event["user_id"],
        "items":            event["items"],
        "total":            event["total"],
        "correlation_id":   event.get("correlation_id", ""),
        "confirmed_at":     datetime.now(timezone.utc).isoformat(),
    }

    producer.produce(
        topic=TOPIC_CONFIRMED,
        key=order_id,
        value=json.dumps(confirmed_event).encode("utf-8"),
    )
    producer.poll(0)


def run() -> None:
    print("🚀 Order worker starting...")

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           "order-processors",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,       # manual commits — at-least-once
    })

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks":              "all",
        "enable.idempotence": True,
    })

    consumer.subscribe([TOPIC_ORDERS])
    print(f"📡 Subscribed to '{TOPIC_ORDERS}' | group: order-processors\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue

            # Deserialize
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"❌ Failed to deserialize message at offset {msg.offset()}: {e}")
                consumer.commit(asynchronous=False)
                continue

            order_id = event.get("order_id", "unknown")
            print(f"📥 Processing order: {order_id}")

            # Retry loop
            success = False
            last_error = None

            with Session(engine) as session:
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        process_event(event, session, producer)
                        success = True
                        break
                    except Exception as e:
                        last_error = e
                        print(f"  ⚠️  Attempt {attempt}/{MAX_RETRIES} failed: {e}")

            if success:
                print(f"  ✅ Order {order_id} confirmed\n")
            else:
                # All retries exhausted — send to DLQ, mark as failed in DB
                print(f"  ❌ Order {order_id} failed after {MAX_RETRIES} attempts → DLQ\n")
                send_to_dlq(producer, msg, last_error, MAX_RETRIES)

                # Update DB status to "failed"
                with Session(engine) as session:
                    order = session.get(Order, order_id)
                    if order:
                        order.status = "failed"
                        order.error_msg = str(last_error)
                        order.updated_at = datetime.now(timezone.utc)
                        session.commit()

            # Always commit — whether success or DLQ
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("\n⛔ Order worker stopped.")
    finally:
        consumer.close()
        producer.flush()
        print("🔒 Worker shut down cleanly.")


if __name__ == "__main__":
    run()
