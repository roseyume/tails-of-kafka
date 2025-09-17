# python -m streamlit run .\kafka_dashboard.py

import streamlit as st
import streamlit.components.v1 as components
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
admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

if st.button("⟳ Refresh Kafka Info"):
    st.rerun() 

# Native Metric Box Defined
def native_metric_box(metric_name, metric_value, button_key):
    # Container simulates a card
    with st.container():
        # Top padding
        st.write("")  

        # Metric and plus button in one row
        m_col, b_col = st.columns([5,1])
        with m_col:
            st.metric(metric_name, metric_value)
        with b_col:
            if st.button("+", key=button_key):
                st.info(f"{metric_name} add button clicked")
        
        # Bottom padding
        st.write("")

# ---------------- Cluster Management ----------------
with st.expander("🛠️ Kafka Cluster Management", expanded=True):
    # Extract details
    cluster_name = "Cluster-1"  # you can label it however you like
    # --- Fetch cluster metadata ---
    cluster_metadata = admin.list_topics(timeout=10)
    brokers = len(cluster_metadata.brokers)
    topics = list(cluster_metadata.topics.keys())
    partitions = sum(len(t.partitions) for t in cluster_metadata.topics.values())


    # Count partitions across all topics
    num_partitions = sum(len(t.partitions) for t in cluster_metadata.topics.values())

    # --- Streamlit UI ---
    st.subheader(cluster_name)

    c1, c2, c3 = st.columns(3)

    # --- Brokers metric + plus button ---
    with c1:
        label_col, button_col = st.columns([5,1])
        with label_col:
            st.metric("Brokers", brokers)
        with button_col:
            if st.button("➕", key="add_broker"):
                # Kafka doesn’t support adding brokers via AdminClient
                st.warning("Cannot add brokers programmatically. Add a broker manually to the cluster.")

    # --- Partitions metric + plus button ---
    with c2:
        label_col, button_col = st.columns([5,1])
        with label_col:
            st.metric("Partitions", partitions)
        with button_col:
            if st.button("➕", key="add_partition"):
                topic_to_expand = st.selectbox("Select topic", topics, key="partition_topic")
                current_count = len(cluster_metadata.topics[topic_to_expand].partitions)
                new_count = current_count + 1
                # Create new partitions
                fs = admin.create_partitions({topic_to_expand: NewPartitions(new_count)}, request_timeout=15)
                for topic, f in fs.items():
                    try:
                        f.result()
                        st.success(f"Added 1 partition to topic {topic}")
                    except Exception as e:
                        st.error(f"Failed to add partition to {topic}: {e}")

    # --- Topics metric + plus button ---
    with c3:
        label_col, button_col = st.columns([5,1])
        with label_col:
            st.metric("Topics", len(topics))
        with button_col:
            if st.button("➕", key="add_topic"):
                new_topic_name = st.text_input("New topic name", key="new_topic_input")
                if new_topic_name:
                    new_topic = NewTopic(new_topic_name, num_partitions=1, replication_factor=1)
                    st.write(new_topic_name)
                    fs = admin.create_topics([new_topic], request_timeout=15)
                    for topic, f in fs.items():
                        try:
                            f.result()
                            st.success(f"Created topic {topic}")
                        except Exception as e:
                            st.error(f"Failed to create topic {topic}: {e}")


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
        topics = admin.list_topics(timeout=10).topics
        groups_result = admin.list_consumer_groups(request_timeout=10)
        consumer_groups = groups_result.result()  # This is a dict: {group_id: ConsumerGroupListing}
        st.write("REsults")
        st.write(consumer_groups.valid)
        topic_counts = {topic: len(consumer_groups.valid) for topic in topics.keys()}
        st.bar_chart(pd.DataFrame(topic_counts.values(), index=topic_counts.keys(), columns=["Messages"]))
    except KafkaException as e:
        st.error(f"Error fetching topic metadata: {e}")

    st.subheader("Simulated Consumer Lag")
    try:
        consumer_groups = admin.list_consumer_groups(request_timeout=10).result().valid
        lag_data = []
        for group in consumer_groups:
            group_metadata = admin.list_consumer_group_offsets(group.group_id)
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