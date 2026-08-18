import json
from confluent_kafka import Consumer

consumer_config: dict[str, str] = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

print(f"🚀 Starting order tracker consumer...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error() is not None:
            print(f"❌ Error: {msg.error()}")
            continue
        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(f"📦 Received order: {order['quantity']} x {order['item']} from: {order['user']}")
except KeyboardInterrupt:
    print(f"🛑 Interrupted")
finally:
    consumer.close()
