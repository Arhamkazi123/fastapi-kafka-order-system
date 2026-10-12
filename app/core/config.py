import os
from dotenv import load_dotenv

load_dotenv()

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- Topics ---
TOPIC_ORDERS: str    = os.getenv("KAFKA_TOPIC_ORDERS",    "orders")
TOPIC_CONFIRMED: str = os.getenv("KAFKA_TOPIC_CONFIRMED", "orders.confirmed")
TOPIC_DLQ: str       = os.getenv("KAFKA_TOPIC_DLQ",       "orders.dlq")
