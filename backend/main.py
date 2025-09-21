from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from confluent_kafka import Consumer, Producer, TopicPartition
from typing import List, Optional, Dict
from fastapi import HTTPException
import logging

logger = logging.getLogger("myapp")
logging.basicConfig(level=logging.ERROR)
# Add a console handler (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

app = FastAPI()

# Allow your React app to call this backend
origins = [
    "https://3000-roseyume-tailsofkafka-md4yrcdut1c.ws-us121.gitpod.io/",
    "http://localhost:3000",   # React dev server
    "http://127.0.0.1:3000",   # sometimes React uses this
    "*"                        # (optional, allow all origins - use carefully!)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,        # list of allowed origins
    allow_credentials=True,
    allow_methods=["*"],          # allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],          # allow all headers
)

# Kafka Admin client
admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# Consumers
consumers = {}

# Producers
producers = {}

# -------------------------------
# Request models
# -------------------------------
class ClusterNode(BaseModel):
    broker_id: int
    hostname: str
    port: int
    role: str  # 'controller' | 'follower'
    status: str  # 'running' | 'stopped' | 'error'
    rack: str | None = None
    num_partitions_as_leader: int = 0
    num_partitions_as_follower: int = 0

class TopicRequest(BaseModel):
    name: str
    partitions: int = 1
    replication_factor: int = 1

class PartitionsRequest(BaseModel):
    name: str
    additional_partitions: int

class ProducerRequest(BaseModel):
    producer_id: str
    topic: str
    message: str

class ConsumerConfigRequest(BaseModel):
    consumer_id: str                # unique internal key
    name: Optional[str] = None      # human-readable name
    group_id: str
    topics: List[str]
    enable_auto_commit: bool = True
    auto_offset_reset: str = "latest"
    max_poll_records: Optional[int] = 500
    isolation_level: Optional[str] = "read_uncommitted"
    fetch_min_bytes: Optional[int] = 1_000
    offsets: Optional[Dict[str, int]] = None  # {topic: offset}

class ProducerConfigRequest(BaseModel):
    producer_id: str
    acks: str = "all"           # all, 1, 0
    linger_ms: int = 0
    compression_type: str = "none"

logger.info("This will always appear if flush is enabled by default")

# -------------------------------
# Admin endpoints
# -------------------------------
@app.get("/cluster", response_model=List[ClusterNode])
async def get_cluster_info():
    logger.info("Fetching Kafka cluster metadata...")
    try:
        client = AdminClient(KAFKA_CONFIG)
        metadata = client.list_topics(timeout=5)
        logger.info("Stuff")
        logger.error("Something went wrong")
        logger.info(metadata)

        nodes = []
        controller_id = metadata.controller_id

        # Initialize broker stats
        broker_stats = {broker_id: {"leader_count": 0, "follower_count": 0} 
                        for broker_id in metadata.brokers.keys()}

        # Count leader/follower partitions per broker
        for topic in metadata.topics.values():
            for partition in topic.partitions.values():
                leader_id = partition.leader
                replicas = partition.replicas
                for broker_id in replicas:
                    if broker_id == leader_id:
                        broker_stats[broker_id]["leader_count"] += 1
                    else:
                        broker_stats[broker_id]["follower_count"] += 1

        # Build cluster node info
        for broker_id, broker in metadata.brokers.items():
            node = ClusterNode(
                broker_id=broker_id,
                hostname=broker.host,
                port=broker.port,
                rack=broker.rack,
                status='running',  # if metadata returned, assume running
                role='controller' if broker_id == controller_id else 'follower',
                num_partitions_as_leader=broker_stats[broker_id]["leader_count"],
                num_partitions_as_follower=broker_stats[broker_id]["follower_count"],
            )
            nodes.append(node)

        return nodes

    except Exception as e:
        # Return a generic error node if unable to connect
        return [ClusterNode(
            broker_id=-1,
            hostname="unknown",
            port=0,
            role="unknown",
            status="error",
            rack=None
        )]

@app.get("/topics")
def list_topics():
    metadata = admin.list_topics(timeout=10)
    return list(metadata.topics.keys())

@app.post("/topics")
def create_topic(req: TopicRequest):
    new_topic = NewTopic(req.name, num_partitions=req.partitions, replication_factor=req.replication_factor)
    fs = admin.create_topics([new_topic], request_timeout=15)
    for topic, f in fs.items():
        try:
            f.result()
            return {"success": True, "topic": topic}
        except Exception as e:
            return {"success": False, "error": str(e)}

@app.post("/partitions")
def add_partitions(req: PartitionsRequest):
    fs = admin.create_partitions({req.name: NewPartitions(total_count=req.additional_partitions)}, request_timeout=15)
    for topic, f in fs.items():
        try:
            f.result()
            return {"success": True, "topic": topic, "added_partitions": req.additional_partitions}
        except Exception as e:
            return {"success": False, "error": str(e)}

@app.get("/partitions")
def list_partitions():
    metadata = admin.list_topics(timeout=10)
    partitions = sum(len(topic.partitions) for topic in metadata.topics.values())
    return {"partition_count": partitions}

# -------------------------------
# Consumers
# -------------------------------
@app.put("/consumer/{consumer_id}")
def update_consumer(consumer_id: str, req: ConsumerConfigRequest):
    if consumer_id not in consumers:
        print("Consumer " + str(consumer_id) + " found")
    else:
        # Close existing consumer safely
        old_consumer = consumers[consumer_id]
        old_consumer.close()
    
    # Re-create consumer with new config
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": req.group_id,
        "enable.auto.commit": req.enable_auto_commit,
        "auto.offset.reset": req.auto_offset_reset,
        "max.poll.records": req.max_poll_records,
        "isolation.level": req.isolation_level,
        "fetch.min.bytes": req.fetch_min_bytes
    }
    
    consumer = Consumer(consumer_config)
    
    if req.offsets:
        assignments = [TopicPartition(topic, 0, offset) for topic, offset in req.offsets.items()]
        consumer.assign(assignments)
    else:
        consumer.subscribe(req.topics)
    
    consumers[consumer_id] = consumer
    return {
        "success": True,
        "consumer_id": consumer_id,
        "group_id": req.group_id,
        "topics": req.topics,
        "manual_offsets": req.offsets is not None
    }

# -------------------------------
# Endpoint: Consume latest messages from a configured consumer
# -------------------------------
@app.get("/consume/{consumer_id}")
def consume_from_consumer(consumer_id: str, count: int = 10):
    if consumer_id not in consumers:
        return {"success": False, "error": f"Consumer {consumer_id} not found"}
    consumer = consumers[consumer_id]
    messages = []
    for _ in range(count):
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        messages.append(msg.value().decode())
    return {"success": True, "messages": messages}

# -------------------------------
# Producers
# -------------------------------
@app.post("/producer")
def update_producer(producer_id: str, req: ProducerConfigRequest):
    if producer_id not in producers:
        raise HTTPException(status_code=404, detail="Producer not found")
    else:
        producer = producers[producer_id]
        old_producer.flush()
    # Confluent Kafka producers are stateless, so just recreate
    from confluent_kafka import Producer
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": req.acks,
        "linger.ms": req.linger_ms,
        "compression.type": req.compression_type
    })
    producers[producer_id] = producer
    return {"success": True, "producer_id": producer_id}

@app.post("/produce")
def produce_message(req: ProducerRequest):
    try:
        producer = Producer({'bootstrap.servers': 'localhost:9092'})
        producer.produce(req.topic, req.message.encode())
        producer.flush()
        return {"success": True, "topic": req.topic, "message": req.message}
    except Exception as e:
        return {"success": False, "error": str(e)}
