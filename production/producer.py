"""Production-style Kafka producer.

Builds on producer.py with the settings and patterns you'd actually run in
production: durable delivery guarantees, per-user ordering, graceful shutdown,
and a dead-letter path instead of silently dropping failed messages.
"""
import json
import logging
import os
import signal
import uuid
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

# APP_ENV picks which .env file to load — see .env.development.example and
# .env.production.example. .env.production carries real SASL credentials, so
# it's gitignored; run scripts/generate-production-credentials.sh to create it.
APP_ENV = os.environ.get("APP_ENV", "development")
load_dotenv(Path(__file__).resolve().parent.parent / f".env.{APP_ENV}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("orders-producer")

TOPIC = os.environ.get("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.environ.get("ORDERS_DLQ_TOPIC", "orders-dlq")

producer_config = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    # idempotence + acks=all: each message lands exactly once per partition, even
    # if librdkafka has to retry after a broker timeout.
    "enable.idempotence": True,
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 300,
    # Small batching window: trade a few ms of latency for much better throughput.
    "linger.ms": 20,
    "compression.type": "lz4",
}

# Only .env.production sets KAFKA_SECURITY_PROTOCOL — .env.development targets
# the unauthenticated PLAINTEXT listener, so this is a no-op locally.
if os.environ.get("KAFKA_SECURITY_PROTOCOL"):
    producer_config.update({
        "security.protocol": os.environ["KAFKA_SECURITY_PROTOCOL"],
        "sasl.mechanisms": os.environ["KAFKA_SASL_MECHANISM"],
        "sasl.username": os.environ["KAFKA_SASL_USERNAME"],
        "sasl.password": os.environ["KAFKA_SASL_PASSWORD"],
    })

producer = Producer(producer_config)

_shutdown = False


def _handle_signal(signum, _frame):
    global _shutdown
    logger.info("Received signal %s, shutting down...", signum)
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def delivery_report(err, msg):
    if err is not None:
        # Never just drop a failed message in production — route it somewhere a
        # human or a replay job can pick it up.
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        producer.produce(DLQ_TOPIC, value=msg.value(), key=msg.key())
    else:
        logger.info(
            "Delivered order [partition %s @ offset %s]", msg.partition(), msg.offset()
        )


def send_order(order: dict) -> None:
    payload = json.dumps(order).encode("utf-8")
    # Keying by user_id keeps every order for the same user on the same partition,
    # so a consumer reading that partition always sees them in send order.
    producer.produce(
        TOPIC,
        key=order["user_id"].encode("utf-8"),
        value=payload,
        callback=delivery_report,
    )
    # poll() drives delivery-report callbacks for already-acked messages; without
    # it, callbacks (and errors) only surface later, at flush().
    producer.poll(0)


if __name__ == "__main__":
    orders = [
        {"order_id": str(uuid.uuid4()), "user_id": "user-42", "item": "mushroom pizza", "quantity": 2},
        {"order_id": str(uuid.uuid4()), "user_id": "user-42", "item": "garlic bread", "quantity": 1},
        {"order_id": str(uuid.uuid4()), "user_id": "user-7", "item": "margherita pizza", "quantity": 1},
    ]
    for order in orders:
        if _shutdown:
            break
        send_order(order)

    # Blocks until every buffered message is acked or times out. Skipping this on
    # exit is how you silently lose whatever was still in flight.
    producer.flush(10)
    logger.info("Producer shut down cleanly")
