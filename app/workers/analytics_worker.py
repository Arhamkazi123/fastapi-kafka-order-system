import json
from confluent_kafka import Consumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDERS


def run() -> None:
    print("🚀 Analytics worker starting...")

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           "analytics-service",  
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC_ORDERS])
    print(f"📡 Subscribed to '{TOPIC_ORDERS}' | group: analytics-service\n")

    # In-memory stats (in production: write to a DB or metrics system)
    stats = {
        "total_orders": 0,
        "total_revenue": 0.0,
        "orders_per_user": {},
    }

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"❌ Failed to deserialize: {e}")
                consumer.commit(asynchronous=False)
                continue

            # Only track "order.placed" events (not replays or other types)
            if event.get("event_type") != "order.placed":
                consumer.commit(asynchronous=False)
                continue

            order_id = event.get("order_id", "unknown")
            user_id  = str(event.get("user_id", "unknown"))
            total    = event.get("total", 0)

            # Update stats
            stats["total_orders"]   += 1
            stats["total_revenue"]  += total
            stats["orders_per_user"][user_id] = stats["orders_per_user"].get(user_id, 0) + 1

            print(f"📊 Analytics update — order {order_id}")
            print(f"   Total orders:   {stats['total_orders']}")
            print(f"   Total revenue:  ${stats['total_revenue']:.2f}")
            print(f"   Orders by user: {stats['orders_per_user']}\n")

            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("\n⛔ Analytics worker stopped.")
    finally:
        consumer.close()
        print("🔒 Worker shut down cleanly.")


if __name__ == "__main__":
    run()
