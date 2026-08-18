import uuid
import json
from confluent_kafka import Producer

producer_config = {
  'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Delivery failed: {err}')
    else:
        print(f'✅ Delivered: {msg.value().decode('utf-8')}')
        print(f'✅ Topic: {msg.topic()}')
        print(f'✅ Partition: {msg.partition()}')
        print(f'✅ Offset: {msg.offset()}')

order = {
  "order_id": str(uuid.uuid4()),
  "user": "César Arellano",
  "item": "mushroom pizza",
  "quantity": 2
}

order_json = json.dumps(order).encode('utf-8')

producer.produce(
  'orders', 
  value=order_json,
  callback=delivery_report
)
producer.flush() # Make sure this runs cleanly, best practice, this is a synchronous method that forces all buffered, un-sent records to be sent immediately and blocks the calling thread until all outstanding message deliveries are fully complete