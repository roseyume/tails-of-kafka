from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from confluent_kafka import Consumer, Producer, TopicPartition
from typing import List, Optional, Dict
from fastapi import HTTPException
import logging
import yaml
import subprocess
import os
import sys
import time

# ----------------------------
# Configure root logger
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,   # ensure logs go to stdout (Gitpod picks this up)
)

logger = logging.getLogger(__name__)

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
class Broker(BaseModel):
    broker_id: int
    hostname: str
    port: int
    role: str  # 'controller' | 'follower'
    status: str  # 'running' | 'stopped' | 'error'
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
@app.get("/brokers", response_model=List[Broker])
async def get_brokers():
    logger.info("Fetching Kafka cluster metadata...")
    try:
        metadata = admin.list_topics(timeout=5)

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

        # Build broker node info
        for broker_id, broker in metadata.brokers.items():
            node = Broker(
                broker_id=broker_id,
                hostname=broker.host,
                port=broker.port,
                status='running',  # if metadata returned, assume running
                role='controller' if broker_id == controller_id else 'follower',
                num_partitions_as_leader=broker_stats[broker_id]["leader_count"],
                num_partitions_as_follower=broker_stats[broker_id]["follower_count"],
            )
            nodes.append(node)

        return nodes

    except Exception as e:
        # Return a generic error node if unable to connect
        return [Broker(
            broker_id=-1,
            hostname="unknown",
            port=0,
            role="unknown",
            status="error"
        )]


script_dir = os.path.dirname(os.path.abspath(__file__))
DOCKER_COMPOSE_FILE = os.path.join(script_dir, "..", "docker-compose.yml")

def find_service_name(broker_id: int) -> str:
    """Find the docker-compose service name for a given broker_id."""
    if not os.path.exists(DOCKER_COMPOSE_FILE):
        raise HTTPException(status_code=500, detail="docker-compose.yml not found")

    with open(DOCKER_COMPOSE_FILE, "r") as f:
        compose_data = yaml.safe_load(f)

    for name, service in compose_data.get("services", {}).items():
        env = service.get("environment", {})
        if int(env.get("KAFKA_BROKER_ID", -1)) == broker_id:
            return name
    return None


@app.get("/brokers/create")
async def add_broker():
    try:
        if not os.path.exists(DOCKER_COMPOSE_FILE):
            raise HTTPException(status_code=500, detail="docker-compose.yml not found")
        with open(DOCKER_COMPOSE_FILE, "r") as f:
            compose_data = yaml.safe_load(f)

        existing_ids = set()
        for service_name, service in compose_data.get("services", {}).items():
            if service_name.startswith("kafka-"):
                env = service.get("environment", {})
                bid = env.get("KAFKA_BROKER_ID")
                if bid is not None:
                    existing_ids.add(int(bid))

        broker_id=max(existing_ids, default=0) + 1
        service_name = f"kafka-{broker_id}"

        # Create new broker definition
        new_service = {
            "image": "confluentinc/cp-kafka:6.0.1",
            "container_name": service_name,
            "hostname": service_name,
            "depends_on": ["zookeeper-1"],
            "ports": [
                f"{9092+broker_id}:{9093+broker_id}",
                f"{29092+broker_id}:{29093+broker_id}",
                f"{9992+broker_id}:{9993+broker_id}"
            ],
            "environment": {
                "KAFKA_BROKER_ID": f"{broker_id}",
                "KAFKA_BROKER_RACK": f"{"r" + str(broker_id)}",
                "KAFKA_ZOOKEEPER_CONNECT": "zookeeper-1:2181",
                "KAFKA_LISTENERS": f"LISTENER_INTERNAL://{service_name}:{19093+broker_id},LISTENER_DOCKERHOST://{service_name}:{29093+broker_id},LISTENER_EXTERNAL://{service_name}:{9093+broker_id}",
                "KAFKA_ADVERTISED_LISTENERS": f"LISTENER_INTERNAL://{service_name}:{19093+broker_id},LISTENER_DOCKERHOST://localhost:{29093+broker_id},LISTENER_EXTERNAL://${{PUBLIC_IP:-127.0.0.1}}:{9093+broker_id}",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "LISTENER_INTERNAL:PLAINTEXT,LISTENER_DOCKERHOST:PLAINTEXT,LISTENER_EXTERNAL:PLAINTEXT",
                "KAFKA_INTER_BROKER_LISTENER_NAME": "LISTENER_INTERNAL",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 3,
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 1,
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 3,
                "KAFKA_MESSAGE_TIMESTAMP_TYPE": "CreateTime",
                "KAFKA_MIN_INSYNC_REPLICAS": 1,
                "KAFKA_DELETE_TOPIC_ENABLE": "True",
                "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "False",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": 100,
                "KAFKA_JMX_PORT": f"{broker_id+9993}",
                "KAFKA_JMX_OPTS": "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.rmi.port={}".format(broker_id+9993),
                "KAFKA_JMX_HOSTNAME": "${PUBLIC_IP:-127.0.0.1}"
            },
            "volumes": ["./data-transfer:/data-transfer"],
            "restart": "unless-stopped",
        }

        compose_data["services"][service_name] = new_service

        with open(DOCKER_COMPOSE_FILE, "w") as f:
            yaml.dump(compose_data, f, default_flow_style=False)

        try:
            subprocess.run(["docker-compose", "up", "-d", service_name], check=True)
        except subprocess.CalledProcessError as e:
            raise HTTPException(status_code=500, detail=f"Failed to start broker: {e}")

        time.sleep(1)
        return {"status": "success", "broker": await get_brokers()}
    except Exception as e:
        print(e)


def run_compose_command(service_name: str, command: str):
    """Run docker-compose command on the given service."""
    try:
        subprocess.run(
            ["docker-compose", "-f", DOCKER_COMPOSE_FILE, command, service_name],
            check=True
        )
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Failed to {command} {service_name}: {e}")


@app.post("/brokers/stop/{broker_id}")
async def stop_broker(broker_id: int):
    service_name = find_service_name(broker_id)
    if not service_name:
        raise HTTPException(status_code=404, detail=f"Broker {broker_id} not found")
    run_compose_command(service_name, "stop")
    time.sleep(1)
    return {"status": "stopped", "service_name": service_name, "broker": await get_brokers()}


@app.post("/brokers/restart/{broker_id}")
async def restart_broker(broker_id: int):
    service_name = find_service_name(broker_id)
    if not service_name:
        raise HTTPException(status_code=404, detail=f"Broker {broker_id} not found")
    run_compose_command(service_name, "restart")
    time.sleep(1)
    return {"status": "restarted", "broker_id": broker_id, "service_name": service_name, "broker": await get_brokers()}


@app.delete("/brokers/delete/{broker_id}")
async def delete_broker(broker_id: int):
    service_name = find_service_name(broker_id)
    if not service_name:
        raise HTTPException(status_code=404, detail=f"Broker {broker_id} not found")

    # Stop the container first
    run_compose_command(service_name, "stop")

    # Remove the container
    run_compose_command(service_name, "rm -f")

    # Optionally: remove the service from docker-compose.yml
    with open(DOCKER_COMPOSE_FILE, "r") as f:
        compose_data = yaml.safe_load(f)

    compose_data["services"].pop(service_name, None)

    with open(DOCKER_COMPOSE_FILE, "w") as f:
        yaml.dump(compose_data, f, default_flow_style=False)

    time.sleep(1)
    return {"status": "deleted", "broker_id": broker_id, "service_name": service_name, "broker": await get_brokers()}

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
