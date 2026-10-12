from app.core.kafka import init_producer, get_producer, close_producer
from app.core.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDERS, TOPIC_CONFIRMED, TOPIC_DLQ
