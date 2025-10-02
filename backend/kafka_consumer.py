import time, threading, asyncio
from datetime import datetime
from confluent_kafka import Consumer
from backend.database.init import database
from backend.database.tables import messages
# from queue import Queue

# Shared stop flag
stop_event = threading.Event()
# message_queue = Queue()

def consume_single_consumer(consumer_name: str, topics: list[str], consumer_obj: Consumer, stop_event: threading.Event, loop):
    buffer = []
    last_flush = time.time()
    FLUSH_INTERVAL = 2  # seconds
    BATCH_SIZE = 50

    consumer_obj.subscribe(topics) 
    print(f"[{consumer_name}] Started consuming {topics}", flush=True)

    try:
        while not stop_event.is_set():
            msg = consumer_obj.poll(1.0)  # still blocking, but short
            # print(f"[{consumer_name}] polling {topics}", flush=True)
            if msg is None:
                time.sleep(1.0) 
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error ({consumer_name}): {msg.error()}", flush=True)
            else:
                #print(f"[{consumer_name}] found a message", flush=True)
                row = {
                    "consumer_name": consumer_name,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "message_offset": msg.offset(),
                    "key": msg.key().decode() if msg.key() else None,
                    "value": msg.value().decode() if msg.value() else None,
                    "timestamp": datetime.utcfromtimestamp(msg.timestamp()[1]/1000)
                }
                buffer.append(row)
                # message_queue.put(row)

            # Flush batch if full or interval passed
            if buffer and (len(buffer) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_INTERVAL):
                future = asyncio.run_coroutine_threadsafe(
                    database.execute_many(messages.insert(), buffer),
                    loop
                )

                try:
                    # Wait for result or catch errors
                    future.result(timeout=5)
                    #print(f"DB Execute completed", flush=True)
                    print(f"{future}", flush=True)
                except Exception as e:
                    print(f"Database insert failed: {e}")
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


# async def db_writer():
#     buffer = []
#     while True:
#         try:
#             row = message_queue.get(timeout=1)
#             buffer.append(row)
#         except:
#             pass

#         if buffer:
#             await database.execute_many(messages.insert(), buffer)
#             print(f"Inserted {len(buffer)} messages")
#             buffer.clear()
#         await asyncio.sleep(1)