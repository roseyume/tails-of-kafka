# Tails of Kafka: How Pets Stream the Tea

Welcome to the companion repo for the Grace Hopper Celebration workshop:  
**Tails of Kafka: How Pets Stream the Tea**

This beginner-friendly session uses a lighthearted pet gossip metaphor to teach the fundamentals of Apache Kafka.

---

## Table of Contents
- [What You'll Learn](#what-youll-learn)
- [Metaphor Overview](#metaphor-overview)
- [Kafka Setup](#kafka-setup)
- [Tips and Tricks](#tips-and-tricks)
- [Scenario 1: Getting Started](#scenario-1-getting-started)
- [Scenario 2: Consume Your First Message](#scenario-2-consume-your-first-message)
- [Scenario 3: Late to the news!](#scenario-3-late-to-the-news)
- [Scenario 4: Scaling the Pet Gossip](#scenario-4-scaling-the-pet-gossip)
- [Scenario 5: Bad Server](#scenario-5-bad-server)
- [Scenario 6: Topic Design](#scenario-6-topic-design)
- [Scenario 7: Consumer Crash](#scenario-7-consumer-crash)
- [Scenario 8: Consumer Group Chaos](#scenario-8-consumer-group-chaos)
- [Scenario 9: Missing out on the latest gossip!](#scenario-9-missing-out-on-the-latest-gossip)
- [Additional Key Points for Kafka Beginners](#additional-key-points-for-kafka-beginners)

---

## What You'll Learn

- What Apache Kafka is and why it matters
- Core Kafka concepts: producers, topics, consumers, partitions, offsets
- How to stream real-time events using Python and Kafka
- How to scale up with partitions and consumer groups

---

## Metaphor Overview

In this workshop, think of Kafka like a neighborhood pet gossip system:

- Cats watch and collect neighborhood updates (producers)
- Gossip goes to categories like `YardDrama` or `KitchenCrimes` (topics)
- Neighborhood animals read the gossip (consumers)
- Kafka organizes and remembers everything

---

## Kafka Setup
   For the purpose of this workshop, a kafka dashboard application has been provisioned on top of the running Apache Kafka to help explore how parts of kafka are configured together for different scenarios. Note that this application is not full fledged and is only intended for learning purposes. The application, its parts and the apache kafka servers are spun up inside the Github codespace through the docker-compose.yml file to serve the kafka dashboard that we will be running the workshop from.

   Prerequisite: a github account is needed and can create created at: https://github.com/
   
   To begin, start this repo as a codespace:
   <img width="960" height="540" alt="Desktop - 1" src="https://github.com/user-attachments/assets/9ec1ac4a-66f0-4bd5-bc4d-0b556e6bd090" />
   <img width="960" height="540" alt="Desktop - 4" src="https://github.com/user-attachments/assets/31792812-370e-4bb0-988e-5b07319ce561" />

   When your codespace has loaded, you should see VScode. In the terminal, run 
   ```bash .devcontainer/postStart.sh```
   <img width="960" height="540" alt="Desktop - 2" src="https://github.com/user-attachments/assets/b84a9db8-6112-4689-adf8-84adb8cb6ad2" />

   Once the containers have started successfully, make the kafka dashboard publicly visible and open it in a browser.
   <img width="960" height="540" alt="Desktop - 3" src="https://github.com/user-attachments/assets/9d4e3f64-2677-4634-9fde-0f29993959b2" />

   The kafka dashboard is now running! Follow the lab scenarios below to start exploring.

---

## Tips and Tricks
 - For any values that are not explicitly mentioned in the lab step, use the defaults preset.
 - At the bottom of the producer and consumer tabs, there are Configuration Guide to help explain some of the configurations.
   
---

## Scenario 1: Getting Started

“Hey, Pixie. Did you see? There's a new cat next door” ~ Buddy

What Is a Topic in Kafka?
A topic is a category or feed name to which records are sent. Producers write messages to topics, and consumers read from them.

Objective: Understand the basic flow of producing messages in Kafka.

- Create a topic 'NeighborhoodUpdates" with a partition of 1
- Create a producer 'Buddy' configured for the topic 'NeighborhoodUpdates' and use the 💬 button to send messages

### Check Your Work
 - In the "Topic Management" section of the dashboard, your newly created topic should appear.
 - The "Producer Management" area should display your producer, and the number of messages sent should be greater than 0

## Scenario 2: Consume Your First Message

What Is a Consumer in Kafka?
A consumer reads messages from a topic. It can start from the earliest or latest message depending on the offset configuration. 

In Kafka, offsets are crucial for tracking a consumer’s position within a partition. Each message in a Kafka partition is assigned a unique sequential number called an offset. The offset indicates the consumer’s progress by marking which messages have been read.

Objective: Learn how messages flow from the topic to a consumer.

- Create a consumer 'Pixie' configured for topic 'NeighborhoodUpdates, consumer group 'cat-consumers' and auto offset reset 'earliest'. 

### Check Your Work
 - The "Consumed Messages" panel should show all the messages produced to the topic 'NeighborhoodUpdates. 

## Scenario 3: Late to the news!
“Oh! Oh! Me too! I want to get the neighborhood gossip!” ~ Sammy

Objective: Learn how offset configurations can impact initial message consumption

- Create another consumer 'Sammy' configured for topic 'NeighborhoodUpdates in the consumer group 'dog-consumers' and auto offset reset 'latest'

### Check Your Work
 - In the "Consumed Messages" table, you should notice that Sammy missed previous messages that were sent.

## Scenario 4: Scaling the Pet Gossip

“Pet gossip has gone viral. One partition isn’t enough!”

What Are Partitions in Kafka?
In Kafka, a partition is a way to break up a topic into smaller chunks. Each topic (like NeighborhoodUpdates) can have one or more partitions, and each partition is an ordered, immutable sequence of messages.
Partitions are the foundation of Kafka’s scalability and parallelism.

It's good to note that while topics might seem similar to message queues, topics are actually logs where each message is appended to it.

Objective: Partitioning enables parallelism, allowing you to produce and consume from multiple partitions simultaneously.

- Delete the consumers 'Sammy' and 'Pixie' and the producer 'Buddy' -- **Note: Only delete after producer is done 
- Recreate the topic 'NeighborhoodUpdates' with 6 partitions instead of 1
- Create 2 producers configured for the topic 'NeighborhoodUpdates' and use the 💬 button to send messages
- Recreate the consumer 'Pixie'

### Check Your Work
 - In the "Partition Assignments" table, you should notice that Pixie has been assigned to all 6 partitions. 

## Scenario 5: Bad Server
"There were bugs in the server so I helped take care of them" ~ Buddy

To ensure high availability and fault tolerance, Kafka uses replication. Each partition has:

Leader: the broker responsible for handling all reads and writes for the partition. 
Followers (Replicas): other brokers that replicate the leader’s data.

When a leader broker fails, one of the in-sync followers (ISRs) can be automatically promoted to leader.

Why Leader Replication Helps Guarantee Resiliency

High Availability: If the leader broker crashes, the system automatically elects a follower as the new leader, ensuring the topic remains available.

Durability: Messages are written to multiple brokers, protecting against data loss if one broker fails.

Scalability with Safety: Producers and consumers always talk to the leader, but followers keep an up-to-date copy, balancing efficiency with reliability.

Consistency: With the min.insync.replicas configuration, Kafka guarantees that writes are acknowledged by a certain number of replicas, reducing risk of data loss during failures.

Objective: Understand how Kafka uses replication to continue processing without downtime.

- After the producers have finished sending messages, stop one of the brokers. 
- Produce messages while the broker is offline and verify they are not lost

**Hint:** Ensure a replication factor > 1 on the broker and `acks=all` on the producer.

### Check Your Work
 - In the "Partition Assignments" table, you should notice that the broker that failed is not the Leader Broker for any of the partitions.

## Scenario 6: Topic Design

"I'm not that interested in the yard drama or any indoor crimes but tell me more about just the neighborhood park" ~ Pixie

Separating data streams into topics helps organize messages and manage consumers efficiently.

Objective: Learn topic categorization and multi-topic consumption.

- Once the producer stops sending, start the stopped broker
- Delete the 'NeighborhoodUpdates' topic and create 3 new ones 'YardDrama', 'IndoorCrimes' and 'ParkUpdates'.
- Create producers to each and recreate the consumer 'Pixie'. Subscribe Pixie to just 2 of the topics.

### Check Your Work
 - In the "Consumed Messages" for Pixie, only messages produced to the 2 topics Pixie is subscribed to should appear.

## Scenario 7: Consumer Crash

"I'm going to take a nap for a little while..." ~ Pixie

How Offset Management Works
When a consumer reads messages from a partition, it replies to kafka with an acknowledge.
This offset can then be committed back to Kafka (usually to a special internal topic called \_\_consumer_offsets) so that Kafka knows up to which message the consumer has processed.
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

Objective: Understand how offset management prevents data duplication and data loss while a consumer is down

- Stop running consumer "Pixie"
- Send messages to one of the topics Pixie was subscribed too
- Restart Pixie and confirm that Pixie received all messaged that were sent while she was sleeping

### Check Your Work
 - In the "Consumed Messages" for Pixie, all messages produced while asleep should appear.

## Scenario 8: Consumer Group Chaos

“A group of squirrels are out to quickly find out which dog has been digging holes in the neighborhood park, using the cat's gossip boards. But they’re hearing duplicate stories!”

What is a consumer group?
A consumer group in Kafka is a collection of one or more consumers that work together to read data from a Kafka topic in a coordinated way.
Each consumer in the group is assigned a subset of partitions from the topic, and no two consumers in the same group will read the same partition. This enables parallel processing while ensuring each message is processed only once by one consumer in the group.

Objective:

- Create 3 squirrel consumers to read the news across all the topics and group them into the same consumer group to evenly divide the work.

### Check Your Work
 - In the "Partition Assignments" table, you should notice that the squirrels split up the partitions between themselves. 

## Scenario 9: Missing out on the latest gossip!

“Squirrel 3's listener crashed! That's so much gossip to go through again!”

In a consumer group, if one consumer goes down, its partitions are automatically redistributed among the remaining consumers. Once the crashed consumer comes back online, the group will rebalance again so it can pick up where it left off — starting from the last committed offset.

Objective:

 - Stop Squirrel1 and observe how the partitions are redistributed among the remaining squirrels.
 - Restart Squirrel1 and observe how the partitions are reassigned again.

### Check Your Work
 - In the "Partition Assignments" table, you should notice that the squirrels split up the partitions were adjusted when the squirrel was turned off and once again when it was turned on.

---

## Cat Gossip Central has brought justice to the squirrel community

You’ve turned chaos into cadence.
Consumers know where they left off. Producers don’t over-share. Everyone knows what happened, who it happened to, and when it was published.
As a result, the squirrels were able to find the dog digging up the acorns in the park.

You built Pet Gossip Central into a reliable stream of furry truth. Kafka would be proud.

###🏅🎖️ You built Pet Gossip Central into a reliable stream of furry truth. Kafka would be proud.
---

## Additional Key Points for Kafka Beginners

### Synchronous vs. Asynchronous Messaging
Kafka is designed for asynchronous messaging, where producers send messages without waiting for consumers to process them. This is different from traditional message queues, which often use synchronous messaging. Kafka's asynchronous model allows for high throughput and scalability, making it ideal for real-time data streaming.

### When to Use Kafka
Kafka is a great choice for:
- processing large volumes of data in real-time.
- decoupling producers and consumers for better scalability.
- handling fault tolerance and durability for your data streams.
- handling event-driven architectures or log aggregation.

However, you might consider message queues or pub/sub services for:
- Small-scale applications with low data volume.
- Scenarios requiring strict message ordering across all partitions.
- Use cases where latency is more critical than throughput.


