"""Production-style Kafka consumer.

Builds on tracker-consumer.py with the patterns you'd actually run in
production: manual offset commits (only after a message is fully handled),
graceful shutdown, and a dead-letter topic for messages that fail processing.
"""
import json
import logging
import os
import signal
from pathlib import Path

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# APP_ENV picks which .env file to load — see .env.development.example and
# .env.production.example. .env.production carries real SASL credentials, so
# it's gitignored; run scripts/generate-production-credentials.sh to create it.
APP_ENV = os.environ.get("APP_ENV", "development")
load_dotenv(Path(__file__).resolve().parent.parent / f".env.{APP_ENV}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("orders-consumer")

TOPIC = os.environ.get("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.environ.get("ORDERS_DLQ_TOPIC", "orders-dlq")

consumer_config = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.environ.get("ORDERS_CONSUMER_GROUP", "order-tracker"),
    "auto.offset.reset": "earliest",
    # Commit manually after processing, not on a timer — otherwise a crash between
    # the auto-commit and finishing the work silently loses that message.
    "enable.auto.commit": False,
}

# Only .env.production sets KAFKA_SECURITY_PROTOCOL — .env.development targets
# the unauthenticated PLAINTEXT listener, so this is a no-op locally.
if os.environ.get("KAFKA_SECURITY_PROTOCOL"):
    consumer_config.update({
        "security.protocol": os.environ["KAFKA_SECURITY_PROTOCOL"],
        "sasl.mechanisms": os.environ["KAFKA_SASL_MECHANISM"],
        "sasl.username": os.environ["KAFKA_SASL_USERNAME"],
        "sasl.password": os.environ["KAFKA_SASL_PASSWORD"],
    })

consumer = Consumer(consumer_config)
# Minimal producer just for routing poison messages to the DLQ; production/producer.py
# already covers the settings that matter for a "real" producer. It shares the
# same bootstrap/SASL settings as the consumer since it talks to the same broker.
dlq_producer = Producer({
    key: value
    for key, value in consumer_config.items()
    if key in {"bootstrap.servers", "security.protocol", "sasl.mechanisms", "sasl.username", "sasl.password"}
})

_shutdown = False


def _handle_signal(signum, _frame):
    global _shutdown
    logger.info("Received signal %s, shutting down...", signum)
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def process(order: dict) -> None:
    """Business logic goes here. Raise on anything that shouldn't be retried forever."""
    logger.info("📦 %s x %s for %s", order["quantity"], order["item"], order["user_id"])


def send_to_dlq(msg, error: str) -> None:
    dlq_producer.produce(
        DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers={"error": error.encode("utf-8")},
    )
    dlq_producer.flush(5)


def main() -> None:
    consumer.subscribe([TOPIC])
    logger.info("Starting order tracker consumer...")
    try:
        while not _shutdown:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error() is not None:
                logger.error("Broker error: %s", msg.error())
                continue

            try:
                order = json.loads(msg.value().decode("utf-8"))
                process(order)
            except Exception as exc:
                # A malformed or unprocessable message should never block the
                # partition forever — log it, route it to the DLQ, and move on.
                logger.exception("Failed to process message, routing to DLQ")
                send_to_dlq(msg, str(exc))

            # Commit only after the message is handled (or safely routed to the
            # DLQ), so a crash mid-processing causes a re-read, not silent loss.
            consumer.commit(msg)
    finally:
        logger.info("Closing consumer...")
        consumer.close()


if __name__ == "__main__":
    main()
