"""
Kafka Order Producer
Produces order messages serialized with Avro (schema embedded locally).
"""

import io
import random
import time
import fastavro
from confluent_kafka import Producer

KAFKA_BROKER = "localhost:9092"
TOPIC        = "orders"
PRODUCTS     = ["Item1", "Item2", "Item3", "Item4", "Item5"]

AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "Order",
    "namespace": "com.assignment.orders",
    "fields": [
        {"name": "orderId", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price",   "type": "float"},
    ],
})


def serialize(record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, AVRO_SCHEMA, record)
    return buf.getvalue()


def delivery_report(err, msg):
    if err:
        print(f"[Producer] Delivery FAILED: {err}")
    else:
        print(f"[Producer] Delivered → partition={msg.partition()}, offset={msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    order_id = 1000

    print(f"[Producer] Publishing to '{TOPIC}'. Press Ctrl+C to stop.\n")
    try:
        while True:
            order = {
                "orderId": str(order_id),
                "product": random.choice(PRODUCTS),
                "price":   round(random.uniform(5.0, 500.0), 2),
            }
            producer.produce(
                topic=TOPIC,
                value=serialize(order),
                key=order["orderId"].encode(),
                callback=delivery_report,
            )
            producer.poll(0)
            print(f"[Producer] Sent → orderId={order['orderId']}, product={order['product']}, price=${order['price']:.2f}")
            order_id += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Producer] Stopping...")
    finally:
        producer.flush()
        print("[Producer] Done.")


if __name__ == "__main__":
    main()
