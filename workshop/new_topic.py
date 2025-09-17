import streamlit as st
from confluent_kafka.admin import AdminClient, NewTopic

# Initialize Kafka admin client
admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# --- Input field ---
new_topic_name = st.text_input("New topic name", key="new_topic_input")

# --- Button to add topic ---
if st.button("➕ Add topic"):
    name = new_topic_name.strip()
    if name:
        # Create NewTopic object
        new_topic = NewTopic(name, num_partitions=1, replication_factor=1)
        fs = admin.create_topics([new_topic], request_timeout=15)

        # Process result
        for topic, f in fs.items():
            try:
                f.result()  # Wait until broker confirms creation

                # Verify topic exists by re-querying Kafka
                topics = admin.list_topics(timeout=10).topics.keys()
                if topic in topics:
                    st.success(f"✅ Topic '{topic}' successfully created!")
                else:
                    st.error(f"❌ Topic '{topic}' creation failed!")

            except Exception as e:
                st.error(f"❌ Failed to create topic '{topic}': {e}")
    else:
        st.warning("⚠️ Please enter a topic name.")
