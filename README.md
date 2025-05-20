# 🐾 Tails of Kafka: How Pets Stream the Tea

Welcome to the companion repo for the Grace Hopper Celebration workshop:  
**Tails of Kafka: How Pets Stream the Tea**

This beginner-friendly session uses a lighthearted pet gossip metaphor to teach the fundamentals of Apache Kafka.

---

## 📚 What You'll Learn

- What Apache Kafka is and why it matters
- Core Kafka concepts: producers, topics, consumers, partitions, offsets
- How to stream real-time events using Python and Kafka
- How to process and filter messages with consumer groups

---

## 🐾 Metaphor Overview

Think of Kafka like a neighborhood pet gossip system:

- 🐶 Pets cause drama (producers)
- 🗂️ Gossip goes to categories like `YardDrama` or `KitchenCrimes` (topics)
- 🐱 Cats watch and collect updates (consumers)
- 🧠 Kafka organizes and remembers everything

---

## 🛠️ Kafka Setup

See [workshop/kafka-setup.md](workshop/kafka-setup.md) for local Kafka setup instructions using Docker.

---

## ▶️ Run the Demo

1. Start Kafka locally (see `kafka-setup.md`)
2. In one terminal:  
   `python3 workshop/producer.py`
3. In another terminal:  
   `python3 workshop/consumer.py`

---

## 🐕 Sample Gossip Message

```json
{
  "pet": "Fluffy",
  "event": "knocked over vase",
  "location": "living room",
  "timestamp": "2025-05-13T14:12:00Z"
}
```

## 📦 Scenario 1: Scaling the Pet Gossip

“Pet gossip has gone viral. One partition isn’t enough!”

What Are Partitions in Kafka?
In Kafka, a partition is a way to break up a topic into smaller chunks. Each topic (like PetGossip) can have one or more partitions, and each partition is an ordered, immutable sequence of messages.
Partitions are the foundation of Kafka’s scalability and parallelism.

Objective: Partitioning enables parallelism, allowing you to produce and consume from multiple partitions simultaneously.

- Modify the topic to use multiple partitions.

Hint: Update or recreate the topic with --partitions 3+.

## 👯‍♀️ Scenario 2: Consumer Group Chaos

“A group of squirrels are out to quickly find out which dog has been digging holes in the neighborhood park, using the cat's gossip boards. But they’re hearing duplicate stories!”

What is a consumer group?
A consumer group in Kafka is a collection of one or more consumers that work together to read data from a Kafka topic in a coordinated way.
Each consumer in the group is assigned a subset of partitions from the topic, and no two consumers in the same group will read the same partition. This enables parallel processing while ensuring each message is processed only once by one consumer in the group.

Objective:

- Group your consumers into a consumer group to evenly divide work.

Hint: Set the group.id in the consumer code.

## 💥 Scenario 3: Missing out on the latest gossip!

“Squirrel 3's listener crashed! That's so much gossip to go through again!”

Offset Management and Recovery in Kafka Consumers
In Kafka, offsets are crucial for tracking a consumer’s position within a partition. Each message in a Kafka partition is assigned a unique sequential number called an offset. The offset indicates the consumer’s progress by marking which messages have been read.

How Offset Management Works
When a consumer reads messages from a partition, it keeps track of the offset of the last message it successfully processed.
This offset can be committed back to Kafka (usually to a special internal topic called \_\_consumer_offsets) so that Kafka knows up to which message the consumer has processed.
Committing offsets can happen automatically (auto-commit) at regular intervals or manually by the consumer application after processing a message batch.

Why is Offset Management Important?
It ensures exactly-once or at-least-once processing semantics depending on how the application commits offsets.
It allows consumers to resume processing from where they left off after a crash, restart, or failure — rather than reprocessing messages or missing them.
It enables Kafka to track consumer progress across partitions and consumer groups efficiently.

Consumer Recovery Scenario
If a consumer crashes or disconnects unexpectedly:
Upon restart, the consumer reads the last committed offset from Kafka.
The consumer resumes reading from the next offset, ensuring no duplication or message loss if offsets were committed properly.
If offsets were not committed or committed too infrequently, some messages might be reprocessed or missed, depending on the strategy.

Manual vs. Automatic Offset Commit
Automatic commit (enable.auto.commit=true) commits offsets periodically without confirmation that the message was processed. This is easier but risks message duplication or loss if the consumer crashes before processing.

Manual commit (enable.auto.commit=false) gives full control to commit offsets only after messages are safely processed, supporting more reliable processing at the cost of additional code complexity.

Objective:

- Restart a consumer and confirm it resumes from last committed offset.

Hint: Try enable.auto.commit=false and manual commits.

## 💬 Scenario 4: Gossip Replay

“No one's mentioned about the holes in the park today. Can we replay gossip from the beginning?”

How can we replay messages?
Kafka retains messages in a topic for a configurable amount of time (default: 7 days). During this time, any consumer can re-read those messages by:
Resetting its offset to an earlier value (like 0 to go to the very beginning)
Using the --from-beginning flag when running the Kafka console consumer
This works because Kafka stores data in an immutable log, and consumers are free to choose their own read position (offset).

Objective: Use --from-beginning on consumer to reprocess all messages.

Learning Goal: Understand Kafka's immutable log and replayability.

## 🗃️ Scenario 5: Filter the Gossip

“The squirrels may have found their culprit. Let's see where else they've been.”

Objective: Use message keys or metadata to filter messages on the consumer side to find additional evidence on the culprit.

Bonus: Split gossip into multiple topics (e.g., DogGossip, CatGossip) and route accordingly.

Learning Goal: Reinforce Kafka topic design and message filtering.

## 🧶 Cat Gossip Central has brought justice to the squirrel community

You’ve turned chaos into cadence.
Consumers know where they left off. Producers don’t over-share. Everyone knows what happened, who it happened to, and when it was published.
As a result, the squirrels were able to find the dog digging up the acorns in the park.

🏅🎖️ You built Pet Gossip Central into a reliable stream of furry truth. Kafka would be proud.
