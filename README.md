# Kafka Crash Course

A hands-on Apache Kafka playground: a single-broker cluster running in Docker, a minimal producer/consumer pair to learn the basics, and a second, production-style pair that shows how those basics get hardened for real systems.

The scenario is simple on purpose — an "orders" event stream, like the one that sits behind checkout in an e-commerce app — so the focus stays on how the messaging works, not on the business logic.

## What this demonstrates

- Standing up a Kafka cluster with Docker Compose, using KRaft mode (no ZooKeeper)
- Producing and consuming JSON events with Python (`confluent-kafka`)
- The delivery guarantees that separate a tutorial script from a production service: idempotent writes, ordering keys, manual offset commits, graceful shutdown, and dead-letter handling for messages that fail

## Project structure

```
.
├── docker-compose.yaml       # Single-broker Kafka cluster (KRaft mode)
├── producer.py                # Basics: publish one order event
├── tracker-consumer.py        # Basics: consume and print order events
├── production/
│   ├── producer.py            # Same flow, hardened for production use
│   └── consumer.py            # Same flow, hardened for production use
└── main.py
```

## Prerequisites

- Docker
- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (or `pip`, using the provided `pyproject.toml`)

## Quick start

```bash
# 1. Start the Kafka broker
docker-compose up -d

# 2. Install dependencies
uv sync   # or: pip install confluent_kafka

# 3. Run the basics — in one terminal, then the other
python tracker-consumer.py
python producer.py
```

You should see the consumer print the order the producer just sent.

## Basics vs. production example

| | `producer.py` / `tracker-consumer.py` | `production/producer.py` / `production/consumer.py` |
|---|---|---|
| Purpose | Learn the core produce/consume API | Show what changes before this ships |
| Delivery guarantee | Best-effort | Idempotent producer, `acks=all` — no duplicate or lost writes on retry |
| Ordering | None | Messages keyed by `user_id`, so one user's events stay in order |
| Offset commits | Default (auto) | Manual, only after a message is fully processed |
| Failure handling | Errors are printed | Failed messages are routed to a dead-letter topic (`orders-dlq`) instead of being dropped |
| Shutdown | Ctrl+C | Listens for `SIGINT`/`SIGTERM` and shuts down cleanly |

Run the production pair the same way:

```bash
python production/consumer.py
python production/producer.py
```

## Useful Kafka CLI commands

```bash
# List all topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Inspect the orders topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

# Tail every message on the orders topic from the beginning
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
```
