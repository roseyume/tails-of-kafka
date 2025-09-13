# python -m streamlit run .\kafka_dashboard.py

import streamlit as st
import pandas as pd
import json
import time
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

st.set_page_config(page_title="🐾 Pet Gossip Kafka Dashboard", layout="wide")

st.title("🐾 Pet Gossip Kafka Dashboard 🐾")
st.markdown("Live Kafka playground with schema enforcement, producers, consumers, topics, and monitoring.")

# ---------------- Kafka Configuration ----------------
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Initialize Kafka Admin Client and Schema Registry Client
admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


# ---------------- Cluster Management ----------------
with st.expander("🛠️ Kafka Cluster Management", expanded=True):
    st.subheader("Brokers")
    try:
        cluster_metadata = admin_client.list_topics(timeout=10)
        brokers = cluster_metadata.brokers
        st.write("Active Brokers:", [f"{broker.id}: {broker.host}" for broker in brokers.values()])
    except KafkaException as e:
        st.error(f"Error fetching brokers: {e}")

    st.subheader("Topics")
    try:
        topics = admin_client.list_topics(timeout=10).topics
        st.write("Existing Topics:", list(topics.keys()))
    except KafkaException as e:
        st.error(f"Error fetching topics: {e}")

    new_topic = st.text_input("Create Topic", key="new_topic_input")
    num_partitions = st.number_input("Number of Partitions", min_value=1, value=1, step=1)
    replication_factor = st.number_input("Replication Factor", min_value=1, value=1, step=1)

    if st.button("➕ Create Topic"):
        try:
            admin_client.create_topics([NewTopic(new_topic, num_partitions, replication_factor)])
            st.success(f"Topic `{new_topic}` created!")
        except KafkaException as e:
            st.error(f"Error creating topic: {e}")


# ---------------- Schema Registry ----------------
with st.expander("📑 Schema Registry", expanded=False):
    st.subheader("Register New Schema")
    subject = st.text_input("Schema Subject (usually topic name)", key="schema_subject")
    schema_definition = st.text_area("Schema Definition (JSON)", key="schema_def")

    if st.button("➕ Register Schema"):
        try:
            parsed_schema = json.loads(schema_definition)
            schema_id = schema_registry_client.register_schema(subject, JSONSerializer(parsed_schema, schema_registry_client))
            st.success(f"Registered schema for subject `{subject}` with ID {schema_id}")
        except json.JSONDecodeError:
            st.error("Schema definition must be valid JSON.")
        except Exception as e:
            st.error(f"Error registering schema: {e}")

    st.subheader("Registered Schemas")
    try:
        subjects = schema_registry_client.get_subjects()
        for subj in subjects:
            st.write(f"📌 **{subj}**")
            versions = schema_registry_client.get_versions(subj)
            for version in versions:
                schema = schema_registry_client.get_schema(subj, version)
                st.json(schema.schema_str)
    except Exception as e:
        st.error(f"Error fetching schemas: {e}")


# ---------------- Monitoring & Visualization ----------------
with st.expander("📊 Monitoring & Visualization", expanded=False):
    st.subheader("Message Throughput per Topic")
    try:
        topics = admin_client.list_topics(timeout=10).topics
        topic_counts = {topic: len(admin_client.list_consumer_groups().result(timeout=10).consumer_groups) for topic in topics.keys()}
        st.bar_chart(pd.DataFrame(topic_counts.values(), index=topic_counts.keys(), columns=["Messages"]))
    except KafkaException as e:
        st.error(f"Error fetching topic metadata: {e}")

    st.subheader("Simulated Consumer Lag")
    try:
        consumer_groups = admin_client.list_consumer_groups(timeout=10)
        lag_data = []
        for group in consumer_groups:
            group_metadata = admin_client.list_consumer_group_offsets(group.group_id)
            for topic, partition in group_metadata.items():
                lag = partition.high - partition.low
                lag_data.append({"consumer": group.group_id, "topic": topic, "lag": lag})
        if lag_data:
            lag_df = pd.DataFrame(lag_data)
            st.table(lag_df)
            st.line_chart(lag_df.set_index("consumer")["lag"])
        else:
            st.info("No consumers yet.")
    except KafkaException as e:
        st.error(f"Error fetching consumer lag: {e}")

# ---------------- Kafka Producer ----------------
def create_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKER})

def send_message(producer, topic, message, schema=None):
    if schema:
        avro_serializer = AvroSerializer(schema_registry_client, schema, lambda obj, ctx: obj)
        serialized_message = avro_serializer(message, None)
        producer.produce(topic=topic, value=serialized_message)
    else:
        producer.produce(topic=topic, value=json.dumps(message))
    producer.flush()

# ---------------- Kafka Consumer ----------------
def create_consumer(group_id, topic):
    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    return consumer

def consume_messages(consumer, max_messages=10):
    messages = []
    for _ in range(max_messages):
        msg = consumer.poll(1.0)
        if msg is None:
            break
        if msg.error():
            raise KafkaException(msg.error())
        messages.append(json.loads(msg.value()))
    return messages

# ---------------- Streamlit UI ----------------
# Kafka Producer UI
with st.expander("✏️ Kafka Producer", expanded=True):
    st.subheader("Send Messages")
    topic = st.text_input("Topic", key="producer_topic")
    pet = st.selectbox("Pet", ["Cat 🐱", "Dog 🐶", "Parrot 🦜"], key="producer_pet")
    event = st.selectbox("Event", ["slept 💤", "ate 🍖", "gossiped 🗣️"], key="producer_event")
    location = st.selectbox("Location", ["Garden 🌳", "Living Room 🛋️", "Roof 🏠"], key="producer_location")
    message = {
        "pet": pet,
        "event": event,
        "location": location,
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    st.json(message)

    if st.button("📤 Send Message"):
        producer = create_producer()
        send_message(producer, topic, message)
        st.success(f"Message sent to topic `{topic}`")

# Kafka Consumer UI
with st.expander("👀 Kafka Consumer", expanded=True):
    st.subheader("Consume Messages")
    group_id = st.text_input("Consumer Group ID", key="consumer_group_id")
    consume_topic = st.text_input("Topic to Subscribe", key="consumer_topic")
    if st.button("▶️ Start Consuming"):
        consumer = create_consumer(group_id, consume_topic)
        messages = consume_messages(consumer)
        if messages:
            st.dataframe(pd.DataFrame(messages))
        else:
            st.info("No messages available.")