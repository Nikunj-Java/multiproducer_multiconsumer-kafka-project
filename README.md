# Kafka Multi-Producer Multi-Consumer — Spring Boot

This project demonstrates a real Kafka **Multi-Producer + Multi-Consumer** architecture using Spring Boot.

## Architecture

```text
                    +----------------------+
                    | Kafka Topic          |
                    | business-events      |
                    | 3 partitions         |
                    +----------+-----------+
                               ^
                     +---------+---------+
                     |                   |
              +------+-------+    +------+-------+
              | Trade       |    | Payment     |
              | Producer    |    | Producer    |
              | :8081       |    | :8082       |
              +-------------+    +-------------+
                               |
              +----------------+----------------+
              |                |                |
              v                v                v
       +-------------+  +-------------+  +-------------+
       | Settlement  |  | Audit       |  | Notification |
       | Consumer    |  | Consumer    |  | Consumer     |
       | :8091       |  | :8092       |  | :8093        |
       | settlement- |  | audit-      |  | notification-|
       | group       |  | group       |  | group        |
       +-------------+  +-------------+  +-------------+
```

### Key idea

There are **2 producers** publishing to the same Kafka topic and **3 independent consumers** subscribing to that topic.

Because every consumer uses a **different consumer group**, every event is delivered to all three consumer applications.

```text
Trade Producer ------\
                      > business-events topic ---> settlement-group
Payment Producer ----/                         ---> audit-group
                                                  ---> notification-group
```

## Components

| Component | Port | Role | Consumer Group |
|---|---:|---|---|
| kafka-trade-producer | 8081 | Produces TRADE events | - |
| kafka-payment-producer | 8082 | Produces PAYMENT events | - |
| kafka-settlement-consumer | 8091 | Settlement processing | settlement-group |
| kafka-audit-consumer | 8092 | Audit logging | audit-group |
| kafka-notification-consumer | 8093 | Notification processing | notification-group |
| Kafka | 8085 | Broker | - |
| Kafka UI | 8086 | Web UI | - |

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Desktop / Docker Engine
- Postman or curl

## 1. Start Kafka

From the project root:

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

## 2. Create the Kafka topic

Linux / WSL / Git Bash:

```bash
./scripts/create-topic.sh
```

Windows CMD / PowerShell:

```bash
docker exec -it kafka-mpmc /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic business-events --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1
```

Verify:

```bash
docker exec -it kafka-mpmc /opt/kafka/bin/kafka-topics.sh --describe --topic business-events --bootstrap-server localhost:29092
```

You should see **3 partitions**.

## 3. Start the applications

Open five terminals.

### Terminal 1 — Trade Producer

```bash
cd kafka-trade-producer
mvn spring-boot:run
```

Runs on `http://localhost:8081`.

### Terminal 2 — Payment Producer

```bash
cd kafka-payment-producer
mvn spring-boot:run
```

Runs on `http://localhost:8082`.

### Terminal 3 — Settlement Consumer

```bash
cd kafka-settlement-consumer
mvn spring-boot:run
```

### Terminal 4 — Audit Consumer

```bash
cd kafka-audit-consumer
mvn spring-boot:run
```

### Terminal 5 — Notification Consumer

```bash
cd kafka-notification-consumer
mvn spring-boot:run
```

## 4. Test Trade Producer

Postman:

```http
POST http://localhost:8081/events?key=ACC-001&trade=AAPL-BUY-200
```

Or curl:

```bash
curl -X POST "http://localhost:8081/events?key=ACC-001&trade=AAPL-BUY-200"
```

The same Kafka event should appear in all three consumer consoles:

```text
[SETTLEMENT] group=settlement-group | partition=1 | offset=0 | key=ACC-001 | value={...}
[AUDIT] group=audit-group | partition=1 | offset=0 | key=ACC-001 | value={...}
[NOTIFICATION] group=notification-group | partition=1 | offset=0 | key=ACC-001 | value={...}
```

The exact partition and offset depend on the Kafka state.

## 5. Test Payment Producer

```http
POST http://localhost:8082/events?key=ACC-002&payment=50000-INR
```

Again, all three consumers receive the PAYMENT event.

## 6. Test multiple producers

Send several events:

```bash
curl -X POST "http://localhost:8081/events?key=ACC-001&trade=AAPL-BUY-200"
curl -X POST "http://localhost:8081/events?key=ACC-002&trade=GOOG-SELL-50"
curl -X POST "http://localhost:8082/events?key=ACC-001&payment=50000-INR"
curl -X POST "http://localhost:8082/events?key=ACC-003&payment=25000-INR"
```

Now you have:

```text
Producer 1 ---->\
                \
                 Kafka Topic ---> Consumer 1
                /                 Consumer 2
Producer 2 ---->                  Consumer 3
```

## 7. Why different consumer groups?

Kafka delivers each record once **per consumer group**.

Here:

```text
business-events
       |
       +---- settlement-group      -> gets every event
       |
       +---- audit-group           -> gets every event
       |
       +---- notification-group    -> gets every event
```

Therefore one event is processed three times, once by each business function.

### If consumers use the SAME group

Suppose Settlement and Audit both use:

```text
group-id=business-group
```

Kafka treats them as members of one consumer group. A partition is assigned to only one member at a time, so they **share the work** rather than each receiving every message.

This distinction is extremely important:

```text
Different Groups = Fan-out / Broadcast behavior
Same Group      = Load balancing / Work sharing behavior
```
to execute this check the profiles in [text](kafka-settlement-consumer/src/main/resources)

```
run both profile
```
- Default Profile
```
mvn spring-boot:run
```
- Run Another Profile
```
mvn spring-boot:run -Dspring-boot.run.profiles=i1
```
```
Here Both are running on different Springboot port, But They are using same Group, so KAFKA will not send every event to both, now Kafka Will do the ALB automatically, try sending 5-10 request with different key, and check the console output
```

## 8. Partitions, keys and offsets

The topic has 3 partitions.

The producers send:

```java
kafkaTemplate.send("business-events", key, payload);
```

The `key` helps Kafka consistently route records with the same key to the same partition under the default partitioning behavior.

Each consumer prints:

```text
partition=...
offset=...
key=...
value=...
```

For example:

```text
partition=2 | offset=17 | key=ACC-001
```

- **Partition** = physical/logical lane where the record is stored
- **Offset** = record position inside that partition
- **Key** = useful for partitioning and ordering related records
- **Consumer Group** = logical team of consumers sharing consumption work

## 9. Kafka UI

Open:

```text
http://localhost:8086
```

Look for:

- `business-events` topic
- 3 partitions
- Messages
- Consumer groups
- Offsets

You should see:

```text
settlement-group
 audit-group
 notification-group
```

## 10. Classroom demonstration

A good teaching sequence is:

### Step 1 — One producer, one consumer

```text
Producer -> Kafka -> Consumer
```

### Step 2 — One producer, multiple consumers

```text
             -> Consumer A
Producer -> Kafka -> Consumer B
             -> Consumer C
```

Use different groups to demonstrate fan-out.

### Step 3 — Multiple producers, multiple consumers

```text
Producer A --\
              \
               Kafka ---> Consumer A
              /          Consumer B
Producer B --/           Consumer C
```

Ask students:

> If two producers publish to the same topic, do consumers know which producer sent the event?

Answer: the message can carry producer/event metadata in its payload. Kafka itself stores the record; your application defines the event structure.

## 11. Important experiment: same group vs different groups

Change Audit Consumer's group ID from:

```properties
spring.kafka.consumer.group-id=audit-group
```

to the same group used by Settlement:

```properties
spring.kafka.consumer.group-id=settlement-group
```

Restart the consumers and send multiple events.

Now Settlement and Audit become members of the same group and Kafka can distribute partitions between them.

Change it back to:

```properties
spring.kafka.consumer.group-id=audit-group
```

and each application becomes an independent consumer group again.

## 12. Stop everything

```bash
docker compose down
```

To also remove Kafka's stored data:

```bash
docker compose down -v
```

## Project structure

```text
kafka-mpmc-project/
├── docker-compose.yml
├── README.md
├── scripts/
│   └── create-topic.sh
├── kafka-trade-producer/
├── kafka-payment-producer/
├── kafka-settlement-consumer/
├── kafka-audit-consumer/
└── kafka-notification-consumer/
```

## Summary

This project demonstrates:

- Multiple Kafka producers
- Multiple Kafka consumers
- One shared Kafka topic
- 3 Kafka partitions
- Kafka keys
- Consumer groups
- Partition and offset visibility
- Fan-out using different consumer groups
- Load balancing using the same consumer group
- Spring Boot + Spring Kafka
- Docker-based Kafka
- Kafka UI
