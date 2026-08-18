"""Production-style Kafka consumer.

Builds on tracker-consumer.py with the patterns you'd actually run in
production: manual offset commits (only after a message is fully handled),
graceful shutdown, and a dead-letter topic for messages that fail processing.
"""
import json
import logging
import os
import signal
from confluent_kafka import Consumer, Producer

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

consumer = Consumer(consumer_config)
# Minimal producer just for routing poison messages to the DLQ; production/producer.py
# already covers the settings that matter for a "real" producer.
dlq_producer = Producer({"bootstrap.servers": consumer_config["bootstrap.servers"]})

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
