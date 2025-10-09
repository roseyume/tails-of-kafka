# 🐾 Tails of Kafka: How Pets Stream the Tea

Welcome to the companion repo for the Grace Hopper Celebration workshop:  
**Tails of Kafka: How Pets Stream the Tea**

This beginner-friendly session uses a lighthearted pet gossip metaphor to teach the fundamentals of Apache Kafka.

---

## 📚 What You'll Learn

- What Apache Kafka is and why it matters
- Core Kafka concepts: producers, topics, consumers, partitions, offsets
- How to stream real-time events using Python and Kafka
- How to scale up with partitions and consumer groups

---

## 🐾 Metaphor Overview

In this workshop, think of Kafka like a neighborhood pet gossip system:

- 🐱 Cats watch and collect neighborhood updates (producers)
- 🗂️ Gossip goes to categories like `YardDrama` or `KitchenCrimes` (topics)
- 🐶 Neighborhood animals read the gossip (consumers)
- 🧠 Kafka organizes and remembers everything

---

## 🛠️ Kafka Setup
   For the purpose of this workshop, a kafka dashboard application has been provisioned on top of the running Apache Kafka to help explore how parts of kafka are configured together for different scenarios. Note that this application is not full fledged and is only intended for learning purposes. The application, its parts and the apache kafka servers are spun up inside the Gitpod workspace through the docker-compose.yml file to serve the kafka dashboard that we will be running the workshop from.

   Prerequisite: a github account is needed and can create created at: https://github.com/
   
   To start, navigate to: https://roseyume-tailsofkafka-ep8vt814m5c.ws-us121.gitpod.io/ and login with your github account

   <img width="932" height="476" alt="image" src="https://github.com/user-attachments/assets/e81d38f9-efd4-48d2-8571-7421d4852bf9" />


---

## Tips and Tricks
 - For any values that are not explicitly mentioned in the lab step, leave as default.
 - Also, at the bottom of both the Producer and the Consumer tabs, there is a Configuration Guide that helps explain some of the configurations that are being set.

## 👉 Scenario 1: Getting Started

“Hey, Pixie. Did you see? There's a new cat next door” ~ Buddy

What Is a Topic in Kafka?
A topic is a category or feed name to which records are sent. Producers write messages to topics, and consumers read from them.

Objective: Understand the basic flow of producing messages in Kafka.

- Create a topic 'NeighborhoodUpdates" with a partition of 1
- Create a producer 'Buddy' configured for the topic 'NeighborhoodUpdates' and use the 💬 button to send messages

Check your work:
 - In the "Topic Management" section of the dashboard, your newly created topic should appear.
 - The "Producer Management" area should display your producer, and the number of messages sent should be greater than 0

## 📦 Scenario 2: Consume Your First Message

What Is a Consumer in Kafka?
A consumer reads messages from a topic. It can start from the earliest or latest message depending on the offset configuration. We'll get more into offset managment later.

Objective: Learn how messages flow from the topic to a consumer.

- Create a consumer 'Pixie' configured for topic 'NeighborhoodUpdates, consumer group 'cat-consumers' and auto offset reset 'earliest'

Check your work:
 - The "Consumers Messages" panel should show incoming messages.

## 📦 Scenario 3: Late to the news!
“Oh! Oh! Me too! I want to get the neighborhood gossip!” ~ Sammy

Objective: Learn how offset configurations can impact initial message consumption

- Create another consumer 'Sammy' configured for topic 'NeighborhoodUpdates in the consumer group 'dog-consumers' and auto offset reset 'latest'

Check your work:
 - In the "Consumers Messages" view, you should notice that Sammy missed all previous messages that were sent.

## 📦 Scenario 4: Scaling the Pet Gossip

“Pet gossip has gone viral. One partition isn’t enough!”

What Are Partitions in Kafka?
In Kafka, a partition is a way to break up a topic into smaller chunks. Each topic (like NeighborhoodUpdates) can have one or more partitions, and each partition is an ordered, immutable sequence of messages.
Partitions are the foundation of Kafka’s scalability and parallelism.

It's good to note that while topics might seem similar to message queues, they are actually logs where each message is appended to it.

Objective: Partitioning enables parallelism, allowing you to produce and consume from multiple partitions simultaneously.

- Recreate the topic 'NeighborhoodUpdates' with 6 partitions instead of 1
- Create 2 producers configured for the topic 'NeighborhoodUpdates' and use the 💬 button to send messages

## 📦 Scenario 5: Bad Server
"There were bugs in the server so I helped take care of them 😊" ~ Buddy

To ensure high availability and fault tolerance, Kafka uses replication. Each partition has:

Leader: the broker responsible for handling all reads and writes for the partition. 

Followers (Replicas): other brokers that replicate the leader’s data.

When a leader broker fails, one of the in-sync followers (ISRs) can be automatically promoted to leader.

🔑 Why Leader Replication Helps Guarantee Resiliency

High Availability: If the leader broker crashes, the system automatically elects a follower as the new leader, ensuring the topic remains available.

Durability: Messages are written to multiple brokers, protecting against data loss if one broker fails.

Scalability with Safety: Producers and consumers always talk to the leader, but followers keep an up-to-date copy, balancing efficiency with reliability.

Consistency: With the min.insync.replicas configuration, Kafka guarantees that writes are acknowledged by a certain number of replicas, reducing risk of data loss during failures.

Objective: Understand how Kafka uses replication to continue processing without downtime.

- Stop one of the brokers
- Produce messages while the broker is offline and verify they are not lost

**Hint:** Ensure a replication factor > 1 on the broker and `acks=all` on the producer.

## 📦 Scenario 6: Topic Design

"I'm not that interested in the yard drama or any indoor crimes but tell me more about just the neighborhood park" ~ Pixie

Separating data streams into topics helps organize messages and manage consumers efficiently.

Objective: Learn topic categorization and multi-topic consumption.

- Delete the 'NeighborhoodUpdates' topic and create 3 new ones 'YardDrama', 'IndoorCrimes' and 'ParkUpdates'.
- Create producers to each and recreate the consumer 'Pixie'


## 👯‍♀️ Scenario 7: Consumer Group Chaos

“A group of squirrels are out to quickly find out which dog has been digging holes in the neighborhood park, using the cat's gossip boards. But they’re hearing duplicate stories!”

What is a consumer group?
A consumer group in Kafka is a collection of one or more consumers that work together to read data from a Kafka topic in a coordinated way.
Each consumer in the group is assigned a subset of partitions from the topic, and no two consumers in the same group will read the same partition. This enables parallel processing while ensuring each message is processed only once by one consumer in the group.

Objective:

- Create 3 squirrel consumers to read the news across all the topics and group them into the same consumer group to evenly divide the work.


## 💥 Scenario 8: Missing out on the latest gossip!

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

Objective: Understand how offset management prevents data duplication and data loss

- Restart one of the squirrel consumers and confirm it resumes from last committed offset.


## 💬 Scenario 9: Gossip Replay (TODO)

“No one's mentioned about the holes in the park today. Can we replay gossip from the beginning?”

How can we replay messages?
Kafka retains messages in a topic for a configurable amount of time (default: 7 days). During this time, any consumer can re-read those messages by:
Resetting its offset to an earlier value (like 0 to go to the very beginning)
This works because Kafka stores data in an immutable log, and consumers are free to choose their own read position (offset).

Objective: Understand Kafka's immutable log and replayability.

- Use the seek command to specify where a new offset position for a specific topic for the consumer to restart from 

## 🗃️ Scenario 10: Filter the Gossip (TODO)

“The squirrels may have found their culprit. Let's see where else they've been.”

Objective: Use message keys or metadata to filter messages on the consumer side to find additional evidence on the culprit.

- Apply a filter to consume only messages matching specific keys or values.

Hint: Kafka Streams or custom consumer logic can be used.

## 🧶 Cat Gossip Central has brought justice to the squirrel community

You’ve turned chaos into cadence.
Consumers know where they left off. Producers don’t over-share. Everyone knows what happened, who it happened to, and when it was published.
As a result, the squirrels were able to find the dog digging up the acorns in the park.

🏅🎖️ You built Pet Gossip Central into a reliable stream of furry truth. Kafka would be proud.
