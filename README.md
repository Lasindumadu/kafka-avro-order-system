# Kafka Avro Order System

A Kafka-based order messaging system with:
- Avro serialization via Confluent Schema Registry
- Real-time running average of order prices
- Retry logic with exponential back-off
- Dead Letter Queue (DLQ) for permanently failed messages

---

## Prerequisites

| Tool | Version | Download |
|------|---------|----------|
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop |
| Python | 3.8+ | https://www.python.org/downloads |

---

## Project Structure

```
.
├── docker-compose.yml   # Kafka + Zookeeper + Schema Registry
├── order.avsc           # Avro schema definition
├── producer.py          # Publishes random order messages
├── consumer.py          # Consumes, retries, aggregates, routes DLQ
├── requirements.txt     # Python dependencies
└── README.md
```

---

## Setup & Run

### Step 1 — Start Kafka infrastructure

```bash
docker-compose up -d
```

Wait ~15 seconds for all services to be healthy. Verify:

```bash
docker-compose ps
```

All three services (`zookeeper`, `kafka`, `schema-registry`) should show **Up**.

---

### Step 2 — Install Python dependencies

```bash
pip install -r requirements.txt
```

---

### Step 3 — Run the Consumer (Terminal 1)

```bash
python consumer.py
```

The consumer subscribes to the `orders` topic and waits for messages.

---

### Step 4 — Run the Producer (Terminal 2)

```bash
python producer.py
```

The producer sends one order per second to Kafka.

---

## What to Observe

### Producer output
```
[Producer] Sent → orderId=1000, product=Item3, price=$243.50
[Producer] Sent → orderId=1001, product=Item1, price=$87.20
```

### Consumer output — successful message
```
[Consumer] ✓ orderId=  1000 | product=Item3  | price=$243.50 | running avg=$243.50 | total orders=1
[Consumer] ✓ orderId=  1001 | product=Item1  | price= $87.20 | running avg=$165.35 | total orders=2
```

### Consumer output — retry then DLQ
```
  [Retry 1/3] Simulated transient processing error — retrying in 2s...
  [Retry 2/3] Simulated transient processing error — retrying in 4s...
  [Retry 3/3] Simulated transient processing error — retrying in 8s...
  [DLQ] Message sent to 'orders.dlq' — reason: Exhausted 3 retries
```

---

## Key Design Points

### Avro + Schema Registry
- Messages are serialized using the **Confluent wire format**: `0x00 + 4-byte schema ID + Avro payload`
- The schema is registered once at producer startup; consumers fetch it by ID on first use

### Running Average
- Uses an incremental O(1) formula: `avg = total / count`
- Printed after every successfully processed message

### Retry Logic
- Up to **3 retries** for `RuntimeError` (simulated transient failure)
- **Exponential back-off**: 2s → 4s → 8s between retries
- Non-retryable errors (e.g. deserialization) go straight to DLQ

### Dead Letter Queue
- Topic: `orders.dlq`
- Failed messages are forwarded with a `failure_reason` header
- Offset is committed even for DLQ messages so the consumer advances

### Manual Offset Commit
- `enable.auto.commit=False` — offset is committed only after the message is either processed successfully or sent to DLQ

---

## Teardown

```bash
docker-compose down
```

To also remove stored data:

```bash
docker-compose down -v
```
