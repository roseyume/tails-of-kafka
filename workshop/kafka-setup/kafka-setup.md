# 🛠 Kafka Setup Guide (Local)

This guide walks you through setting up Apache Kafka locally using **Docker**, the simplest way for a workshop/demo environment.

---

## 🚀 Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Terminal or command-line access

---

## 🐳 Step 1: Create a `docker-compose.yml`

Create a new file named `docker-compose.yml` in your project directory:

```yaml
dversion: "2"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

---

## ▶️ Step 2: Start Kafka

In your terminal:

```bash
docker-compose up -d
```

Wait a few seconds for Kafka and ZooKeeper to start up.

Check that Kafka is running:

```bash
docker ps
```

You should see two containers: one for `cp-zookeeper` and one for `cp-kafka`.

---

## 🧪 Step 3: Create a Kafka Topic

You can create a topic using the Kafka CLI tool inside the container.

```bash
docker exec -it $(docker ps --filter ancestor=confluentinc/cp-kafka:7.5.0 -q) bash
```

Once inside the container:

```bash
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic PetGossip
```

You can list topics to confirm:

```bash
kafka-topics --list --bootstrap-server localhost:9092
```

---

## 📦 Step 4: Stop and Clean Up

To stop Kafka and ZooKeeper:

```bash
docker-compose down
```

To remove containers and volumes completely:

```bash
docker-compose down -v
```

---

## ✅ You're Ready!

You now have Kafka running locally on `localhost:9092`.

You can now:

- Run the Python producer and consumer scripts from the `workshop/src/` folder
- Modify or create new topics as needed

Need help? [Confluent Docs](https://docs.confluent.io/platform/current/installation/docker/index.html) are a great reference.

Happy gossiping! 🐾
