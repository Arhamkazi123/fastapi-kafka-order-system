from confluent_kafka import Producer
from app.core.config import KAFKA_BOOTSTRAP_SERVERS


_producer: Producer | None = None

def init_producer() -> None:
    """
    Create the global producer. Called once in FastAPI lifespan on startup.
    """
    global _producer
    _producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",                  # wait for broker confirmation
        "enable.idempotence": True,     # no duplicate messages on retry
        "retries": 5,                   # retry transient failures
    })
    print(f"✅ Kafka producer initialised → {KAFKA_BOOTSTRAP_SERVERS}")


def get_producer() -> Producer:
    """
    Return the global producer.
    Used as a FastAPI dependency in routes:

        producer: Producer = Depends(get_producer)
    """
    if _producer is None:
        raise RuntimeError("Kafka producer not initialised. Call init_producer() first.")
    return _producer


def close_producer() -> None:
    """
    Flush pending messages and close the producer.
    Called once in FastAPI lifespan on shutdown.
    """
    if _producer is not None:
        _producer.flush(timeout=10)
        print("🔒 Kafka producer flushed and closed.")
