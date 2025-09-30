import time, threading, asyncio
from datetime import datetime
from confluent_kafka import Consumer
from backend.database.init import database
from backend.database.tables import messages

# Shared stop flag
stop_event = threading.Event()

def consume_single_consumer(consumer_name: str, topics: list[str], consumer_obj: Consumer, stop_event: threading.Event, loop):
    buffer = []
    last_flush = time.time()
    FLUSH_INTERVAL = 2  # seconds
    BATCH_SIZE = 50

    consumer_obj.subscribe(topics) 
    print(f"[{consumer_name}] Started consuming {topics}", flush=True)

    try:
        while not stop_event.is_set():
            print(f"Before subscribe", flush=True)
            msg = consumer_obj.poll(0.1)  # still blocking, but short
            print(f"After poll", flush=True)
            if msg is None:
                time.sleep(0.1) 
                print(f"After subscribe", flush=True)
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error ({consumer_name}): {msg.error()}", flush=True)
            else:
                print(f"Found message", flush=True)
                row = {
                    "consumer_name": consumer_name,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode() if msg.key() else None,
                    "value": msg.value().decode() if msg.value() else None,
                    "timestamp": datetime.utcfromtimestamp(msg.timestamp()[1]/1000)
                }
                buffer.append(row)

            # Flush batch if full or interval passed
            if buffer and (len(buffer) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_INTERVAL):
                print(f"DB Execute", flush=True)
                asyncio.run_coroutine_threadsafe(
                    database.execute_many(messages.insert(), buffer),
                    loop
                )
                buffer.clear()
                last_flush = time.time()

    finally:
        print(f"Flushing", flush=True)
        # Flush remaining messages on shutdown
        if buffer:
            asyncio.run_coroutine_threadsafe(
                database.execute_many(messages.insert(), buffer),
                loop
            )
        consumer_obj.close()
        print(f"[{consumer_name}] Stopped gracefully", flush=True)
