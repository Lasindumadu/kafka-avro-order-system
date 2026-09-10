"""
Kafka Order Dashboard
Flask + SocketIO server — runs the Kafka consumer in a background thread
and streams real-time events to the browser dashboard.
"""

import io
import time
import random
import threading
import fastavro
from flask import Flask, render_template
from flask_socketio import SocketIO
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC        = "orders"
DLQ_TOPIC    = "orders.dlq"
GROUP_ID     = "order-dashboard-group"
MAX_RETRIES  = 3
BASE_BACKOFF = 2

app      = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

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

stats = {
    "count": 0, "total": 0.0,
    "min_price": None, "max_price": None,
    "dlq_count": 0, "retry_count": 0,
}
stats_lock = threading.Lock()


def deserialize(raw: bytes) -> dict:
    return fastavro.schemaless_reader(io.BytesIO(raw), AVRO_SCHEMA)


def validate(order: dict):
    if order["price"] <= 0:
        raise ValueError(f"Invalid price {order['price']} — order {order['orderId']}")

def simulate_processing(order: dict):
    """Simulate a downstream service call with ~15% transient failure rate."""
    if random.random() < 0.15:
        raise RuntimeError("Transient processing error (downstream service unavailable)")


def send_to_dlq(dlq_producer: Producer, raw: bytes, reason: str):
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        value=raw,
        headers={"failure_reason": reason.encode()},
    )
    dlq_producer.poll(0)
    with stats_lock:
        stats["dlq_count"] += 1
        count = stats["dlq_count"]
    socketio.emit("dlq_event", {"reason": reason, "count": count})


def consume_loop():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                break

            raw     = msg.value()
            success = False

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    order = deserialize(raw)
                    validate(order)
                    simulate_processing(order)

                    with stats_lock:
                        stats["count"] += 1
                        stats["total"] += order["price"]
                        if stats["min_price"] is None or order["price"] < stats["min_price"]:
                            stats["min_price"] = order["price"]
                        if stats["max_price"] is None or order["price"] > stats["max_price"]:
                            stats["max_price"] = order["price"]
                        avg       = stats["total"] / stats["count"]
                        snap      = dict(stats)

                    socketio.emit("new_order", {
                        "orderId":  order["orderId"],
                        "product":  order["product"],
                        "price":    round(order["price"], 2),
                        "avg":      round(avg, 2),
                        "count":    snap["count"],
                        "min":      round(snap["min_price"], 2),
                        "max":      round(snap["max_price"], 2),
                        "dlq_count":   snap["dlq_count"],
                        "retry_count": snap["retry_count"],
                    })
                    success = True
                    break

                except (ValueError, RuntimeError) as e:
                    wait = BASE_BACKOFF * (2 ** (attempt - 1))
                    with stats_lock:
                        stats["retry_count"] += 1
                        retry_count = stats["retry_count"]
                    socketio.emit("retry_event", {
                        "attempt": attempt,
                        "max":     MAX_RETRIES,
                        "reason":  str(e),
                        "wait":    wait,
                        "total":   retry_count,
                    })
                    time.sleep(wait)

                except Exception as e:
                    send_to_dlq(dlq_producer, raw, str(e))
                    success = True
                    break

            if not success:
                send_to_dlq(dlq_producer, raw, f"Exhausted {MAX_RETRIES} retries")

            consumer.commit(msg)
    finally:
        consumer.close()
        dlq_producer.flush()


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    print("[Dashboard] http://localhost:5000")
    socketio.run(app, debug=False, port=5000, allow_unsafe_werkzeug=True)
