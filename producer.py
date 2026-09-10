"""
Kafka Order Producer
- 95% normal orders with randomized prices
- 5% intentionally invalid orders (price=0) to exercise retry/DLQ path
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


def make_order(order_id: int) -> dict:
    # 5% chance of an invalid order (price=0) to demonstrate retry + DLQ
    if random.random() < 0.05:
        return {"orderId": str(order_id), "product": random.choice(PRODUCTS), "price": 0.0}
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price":   round(random.uniform(10.0, 500.0), 2),
    }


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    order_id = 1000

    print(f"[Producer] Publishing to '{TOPIC}'. Press Ctrl+C to stop.\n")
    try:
        while True:
            order = make_order(order_id)
            producer.produce(
                topic=TOPIC,
                value=serialize(order),
                key=order["orderId"].encode(),
                callback=delivery_report,
            )
            producer.poll(0)
            tag = " [INVALID]" if order["price"] == 0.0 else ""
            print(f"[Producer] Sent → orderId={order['orderId']}, product={order['product']}, price=${order['price']:.2f}{tag}")
            order_id += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Producer] Stopping...")
    finally:
        producer.flush()
        print("[Producer] Done.")


if __name__ == "__main__":
    main()
