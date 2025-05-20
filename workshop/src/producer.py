
from kafka import KafkaProducer
import json
import time
import random

pets = ["Fluffy", "Sir Barksalot", "Mittens", "Whiskerella"]
events = ["knocked over vase", "barked at mail carrier", "stole food", "chased tail"]
locations = ["living room", "yard", "kitchen", "roof"]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {
        "pet": random.choice(pets),
        "event": random.choice(events),
        "location": random.choice(locations),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    print(f"Sending: {message}")
    producer.send("PetGossip", message)
    time.sleep(2)
