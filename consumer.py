"""
Kafka Order Consumer
- Deserializes Avro messages from 'orders' topic
- Maintains real-time running average of prices
- Retries transient failures (up to MAX_RETRIES) with exponential back-off
- Sends permanently failed messages to a Dead Letter Queue (DLQ) topic
"""

import io
import time
import random
import fastavro
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

KAFKA_BROKER  = "localhost:9092"
TOPIC         = "orders"
DLQ_TOPIC     = "orders.dlq"
GROUP_ID      = "order-consumer-group"
MAX_RETRIES   = 3
BASE_BACKOFF  = 2   # seconds; doubles each retry (2 → 4 → 8)

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


def deserialize(raw: bytes) -> dict:
    return fastavro.schemaless_reader(io.BytesIO(raw), AVRO_SCHEMA)


def send_to_dlq(dlq_producer: Producer, raw: bytes, reason: str):
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        value=raw,
        headers={"failure_reason": reason.encode()},
    )
    dlq_producer.poll(0)
    print(f"  [DLQ] Forwarded to '{DLQ_TOPIC}' — reason: {reason}")


def process_order(order: dict):
    """Simulate ~20% transient failure rate for demo purposes."""
    if random.random() < 0.20:
        raise RuntimeError("Simulated transient processing error")


class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, value: float) -> float:
        self.count += 1
        self.total += value
        return self.total / self.count


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,    # commit manually after processing
    })
    consumer.subscribe([TOPIC])

    dlq_producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    avg = RunningAverage()

    print(f"[Consumer] Subscribed to '{TOPIC}'. Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            raw     = msg.value()
            success = False

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    order = deserialize(raw)
                    process_order(order)

                    current_avg = avg.update(order["price"])
                    print(
                        f"[Consumer] OK  orderId={order['orderId']:>6} | "
                        f"product={order['product']:<6} | "
                        f"price=${order['price']:>7.2f} | "
                        f"running avg=${current_avg:>7.2f} | "
                        f"count={avg.count}"
                    )
                    success = True
                    break

                except RuntimeError as e:
                    wait = BASE_BACKOFF * (2 ** (attempt - 1))
                    print(f"  [Retry {attempt}/{MAX_RETRIES}] {e} — waiting {wait}s...")
                    time.sleep(wait)

                except Exception as e:
                    # Non-retryable (e.g. bad Avro bytes) — go straight to DLQ
                    print(f"  [Error] Non-retryable: {e}")
                    send_to_dlq(dlq_producer, raw, str(e))
                    success = True  # treated as handled
                    break

            if not success:
                send_to_dlq(dlq_producer, raw, f"Exhausted {MAX_RETRIES} retries")

            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\n[Consumer] Stopping...")
    finally:
        consumer.close()
        dlq_producer.flush()
        if avg.count:
            print(f"\n[Consumer] Session summary — orders={avg.count}, avg price=${avg.total/avg.count:.2f}")
        else:
            print("[Consumer] No messages processed.")


if __name__ == "__main__":
    main()
