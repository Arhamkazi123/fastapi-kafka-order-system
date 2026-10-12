import json
from confluent_kafka import Consumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CONFIRMED


def run() -> None:
    print("🚀 Notification worker starting...")

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           "notification-service",  
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC_CONFIRMED])
    print(f"📡 Subscribed to '{TOPIC_CONFIRMED}' | group: notification-service\n")

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

            order_id = event.get("order_id", "unknown")
            user_id  = event.get("user_id", "unknown")
            items    = event.get("items", [])
            total    = event.get("total", 0)

            # Simulate sending email
            # In production: call SendGrid, SES, etc.
            print(f"📧 Email sent!")
            print(f"   To:      user-{user_id}@example.com")
            print(f"   Subject: Order {order_id} confirmed")
            print(f"   Body:    Your order for {items} totalling ${total} is confirmed.\n")

            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("\n⛔ Notification worker stopped.")
    finally:
        consumer.close()
        print("🔒 Worker shut down cleanly.")


if __name__ == "__main__":
    run()
