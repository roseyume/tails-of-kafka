from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "PetGossip",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cat-watchers',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to PetGossip topic...\n")
for message in consumer:
    data = message.value
    print(f"🐾 {data['pet']} - {data['event']} in the {data['location']}")
