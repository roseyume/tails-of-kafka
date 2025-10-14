from confluent_kafka import Producer
import threading

class ProducerTracker:
    def __init__(self, name, conf):
        self.name = name
        self.producer = Producer(conf)
        self.sent_count = 0
        self.lock = threading.Lock()

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"❌ [{self.name}] Delivery failed: {err}")
        else:
            print(f"✅ [{self.name}] Gossip delivered to {msg.topic()} [{msg.partition()}]")
            with self.lock:
                self.sent_count += 1

    def send_message(self, topic, value):
        try:
            self.producer.produce(
                topic,
                value=value.encode("utf-8"),
                callback=self.delivery_report
            )
            self.producer.poll(1)
        except BufferError:
            print(f"⚠️ [{self.name}] Buffer full, flushing…")
            self.producer.flush()
            self.producer.produce(
                topic,
                value=value.encode("utf-8"),
                callback=self.delivery_report
            )

    def flush(self):
        self.producer.flush()
        print(f"💤 [{self.name}] Finished gossiping. Total messages delivered: {self.sent_count}")
