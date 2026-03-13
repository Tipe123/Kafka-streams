# Kafka Streams — Greetings Pipeline

A three-application pipeline built with **Apache Camel**, **Kafka Streams**, and the **Kafka Client** library.

```
[You type a message]
        │
        ▼
┌─────────────────────┐
│  camel-kafka-       │  reads stdin, publishes to
│  producer           │──────────────────────────▶  topic: greetings
└─────────────────────┘
                                                           │
                                                           ▼
                                               ┌─────────────────────┐
                                               │  greetings-streams  │  toUpperCase
                                               └─────────────────────┘
                                                           │
                                                           ▼
                                               topic: greetings-uppercase
                                                           │
                                                           ▼
                                               ┌─────────────────────┐
                                               │  greetings-consumer │  prints to terminal
                                               └─────────────────────┘
```

## Prerequisites

| Tool                    | Version            |
| ----------------------- | ------------------ |
| Java                    | 17+                |
| Maven                   | 3.8+               |
| Docker & Docker Compose | any recent version |

---

## 1 — Start Kafka

From the project root, start Zookeeper and the Kafka broker:

```bash
docker compose -f docker-compose-kafka-cluster-confluent.yml up -d zookeeper broker
```

Wait a few seconds for the broker to be ready. You can verify with:

```bash
docker logs broker | grep "started (kafka.server.KafkaServer)"
```

> **Optional:** also start Schema Registry and Confluent Control Center (UI at `http://localhost:9021`):
>
> ```bash
> docker compose -f docker-compose-kafka-cluster-confluent.yml up -d
> ```

---

## 2 — Create the Kafka Topics

The topics are auto-created on first use, but you can create them explicitly:

```bash
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic greetings --partitions 1 --replication-factor 1

docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic greetings-uppercase --partitions 1 --replication-factor 1
```

---

## 3 — Run the Applications

Open **three separate terminals** in the project root.

### Terminal 1 — Kafka Streams (uppercase transformer)

```bash
mvn compile exec:java -pl greetings-streams
```

Consumes from `greetings`, transforms each message to uppercase, publishes to `greetings-uppercase`.

### Terminal 2 — Consumer (printer)

```bash
mvn compile exec:java -pl greetings-consumer
```

Consumes from `greetings-uppercase` and prints every message to the terminal.

### Terminal 3 — Camel Producer (your input)

```bash
mvn compile exec:java -pl camel-kafka-producer
```

You will see a prompt:

```
Enter message (or 'quit' to exit):
```

Type any message and press **Enter** — it will flow through the pipeline and appear uppercased in Terminal 2.  
Type `quit` to stop the producer.

---

## 4 — Stop Everything

Press `Ctrl+C` in each terminal to stop the applications, then shut down Kafka:

```bash
docker compose -f docker-compose-kafka-cluster-confluent.yml down
```

---

## Project Structure

```
kafka-streams/
├── docker-compose-kafka-cluster-confluent.yml
├── pom.xml                          ← root aggregator (all modules)
│
├── camel-kafka-producer/            ← reads stdin → publishes to Kafka
│   └── src/main/java/com/example/
│       ├── MainApp.java             ← entry point
│       └── GreetingRoute.java       ← Camel route (stream:in → kafka:greetings)
│
├── greetings-streams/               ← Kafka Streams topology
│   └── src/main/java/com/example/
│       ├── GreetingsUpperCaseApp.java  ← entry point
│       └── GreetingsTopology.java      ← topology (greetings → uppercase → greetings-uppercase)
│
└── greetings-consumer/              ← plain Kafka consumer → prints to terminal
    └── src/main/java/com/example/
        └── GreetingsUpperCasePrinter.java
```

## Kafka Topics

| Topic                 | Producer               | Consumer             |
| --------------------- | ---------------------- | -------------------- |
| `greetings`           | `camel-kafka-producer` | `greetings-streams`  |
| `greetings-uppercase` | `greetings-streams`    | `greetings-consumer` |
