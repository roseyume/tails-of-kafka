from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic, ConfigResource
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException, KafkaError
from fastapi import FastAPI, HTTPException
from backend.models.schemas import *
from backend.database.init import database
from backend.database.tables import messages
from backend.kafka_consumer import consume_single_consumer
import logging
import yaml
import subprocess
import os
import sys
import time
import threading, queue
import random
import asyncio
from sqlalchemy import func

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

@app.on_event("startup")
async def startup():
    logger.info("Before")
    await database.connect()
    logger.info("After")

@app.on_event("shutdown")
async def shutdown():
    for consumer_name in consumer_stop_events.keys():
        consumer_stop_events[consumer_name].set()  # signal to stop
        consumer_threads[consumer_name].join()     # wait for thread to finish
        consumers[consumer_name].close()
        print(f"Consumer {consumer_name} stopped gracefully")
    await database.disconnect()

# Allow your React app to call this backend
origins = [
    "https://3000-roseyume-tailsofkafka-md4yrcdut1c.ws-us121.gitpod.io/",
    "http://localhost:3000",   # React dev server
    "http://127.0.0.1:3000",   # sometimes React uses this
    "*"                        # (optional, allow all origins - use carefully!)
]

# Cat gossip messages for continuous messaging feature
cat_gossip_messages = [
" saw a suspicious squirrel in the oak tree. Investigation ongoing.",
" reports that the mailman arrived 3 minutes early today. Concerning.",
" observed the neighbors getting a new cat carrier. Possible escape plan needed.",
" confirms that the red dot is still at large. All units on high alert.",
" spotted unknown cat in backyard at 0300 hours. Territory breach!",
" notes that dinner was served 2.5 minutes late. Unacceptable service levels.",
" reports successful counter-surfing mission. Tuna sandwich acquired.",
" witnessed the humans moving furniture. Possible fortress reconstruction.",
" confirms that the laser pointer has been relocated to top shelf. Access denied.",
" reports strange noises from the washing machine. Possible monster habitat.",
" observed the vacuum cleaner in closet. Threat level: Orange.",
" successfully infiltrated the forbidden bathroom counter. Mission accomplished.",
" reports that new scratching post has been delivered. Quality testing required.",
" witnessed delivery truck. Possible invasion. Recommend increased vigilance.",
" confirms that catnip stash remains hidden from human detection.",
" reports successful nap completion. Duration: 14.7 hours. Highly satisfactory.",
" observed bird activity outside window increasing by 23%. Hunting opportunities abound.",
" confirms that favorite cardboard box has been moved. Emergency protocols activated.",
" reports that water bowl is now 78% full instead of usual 80%. Concerning trend.",
" witnessed treat jar opening. All units converged within 0.3 seconds."
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,        # list of allowed origins
    allow_credentials=True,
    allow_methods=["*"],          # allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],          # allow all headers
) 

logger.info("This will always appear if flush is enabled by default")

# Kafka Admin client
admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# Consumers
consumers: Dict[str, Consumer] = {}
consumer_metadata: Dict[str, ConsumerInfo] = {}
consumer_threads = {}       
consumer_stop_events = {}   

# Producers
producers: Dict[str, Producer] = {}
producer_metadata: Dict[str, ProducerInfo] = {}

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

        # Collect broker IDs from replicas (alive + dead)
        replica_brokers = set()
        for topic in metadata.topics.values():
            for partition in topic.partitions.values():
                replica_brokers.update(partition.replicas)

        # Live brokers (from metadata)
        live_brokers = set(metadata.brokers.keys())

        # Initialize broker stats for all brokers (live + dead)
        broker_stats = {broker_id: {"leader_count": 0, "follower_count": 0}
                        for broker_id in replica_brokers}

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

        # Build broker node info (live + dead)
        for broker_id in replica_brokers:
            if broker_id in live_brokers:
                broker = metadata.brokers[broker_id]
                node = Broker(
                    broker_id=broker_id,
                    hostname=broker.host,
                    port=broker.port,
                    status="running",
                    role="controller" if broker_id == controller_id else "follower",
                    num_partitions_as_leader=broker_stats[broker_id]["leader_count"],
                    num_partitions_as_follower=broker_stats[broker_id]["follower_count"],
                )
            else:
                # Dead broker (no metadata.host/port available)
                node = Broker(
                    broker_id=broker_id,
                    hostname="unknown",
                    port=0,
                    status="down",
                    role="unknown",
                    num_partitions_as_leader=broker_stats[broker_id]["leader_count"],
                    num_partitions_as_follower=broker_stats[broker_id]["follower_count"],
                )
            nodes.append(node)

        return nodes

    except Exception as e:
        logger.error("Failed to fetch metadata: %s", e)
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
async def create_broker():
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

def get_topic_config(topic_name: str):
    """Fetch topic config like retention.ms"""
    resource = ConfigResource("topic", topic_name)
    try:
        configs = admin.describe_configs([resource])
        cfg = configs[resource].result()
        retention_entry = cfg.get("retention.ms")
        if retention_entry is not None and retention_entry.value is not None:
            retention_ms = int(retention_entry.value)
        else:
            retention_ms = 7 * 24 * 60 * 60 * 1000  # default 7 days
        return retention_ms
    except KafkaException as e:
        logger.error("Failed to get config for %s: %s", topic_name, e)
        return 7 * 24 * 60 * 60 * 1000  # default 7 days

def format_bytes(size_in_bytes: int) -> str:
    """Convert bytes to a human-readable string (KB, MB, GB, etc.)"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.2f} PB"

@app.get("/topics", response_model=list[Topic])
def list_topics():
    try:
        metadata = admin.list_topics(timeout=10)
        topics = []

        for topic_name, topic in metadata.topics.items():
            partitions = len(topic.partitions)
            replication_factor = len(next(iter(topic.partitions.values())).replicas)

            retention_ms = get_topic_config(topic_name)
            retention_hours = retention_ms // (1000 * 60 * 60)

            topics.append(Topic(
                name=topic_name,
                partitions=partitions,
                replicationFactor=replication_factor,
                retentionHours=retention_hours,
            ))

        return topics

    except KafkaException as e:
        logger.error("Failed to list topics: %s", e)
        return []


@app.post("/topics/create")
def create_topic(req: TopicRequest):
    retention_ms = req.retentionHours * 60 * 60 * 1000  # convert hours → milliseconds

    new_topic = NewTopic(
        topic=req.name,
        num_partitions=req.partitions,
        replication_factor=req.replicationFactor,
        config={"retention.ms": str(retention_ms)}
    )

    fs = admin.create_topics([new_topic])

    for topic_name, f in fs.items():
        try:
            f.result()
            return {"success": True, "topic": topic_name}
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
@app.get("/consumers")
def get_consumers():
    return list(consumer_metadata.values())

@app.post("/consumers/create")
async def create_consumer(req: ConsumerInfo):
    if req.name in consumers:
        raise HTTPException(status_code=400, detail="Consumer ID already exists")

    # Kafka consumer configuration
    conf = {
        "bootstrap.servers": "localhost:9092",  # default, could be extended to frontend
        "group.id": req.groupId,
        "auto.offset.reset": req.autoOffsetReset,
        "enable.auto.commit": req.enableAutoCommit,
        "auto.commit.interval.ms": req.autoCommitInterval
    }

    try:
        kafka_consumer = Consumer(conf)
        metadata = admin.list_topics(timeout=5)
        topic_meta = metadata.topics[req.topics[0]]
        total_messages = 0
        for partition_id, partition_meta in topic_meta.partitions.items():
            low, high = kafka_consumer.get_watermark_offsets(TopicPartition(req.topics[0], partition_id))
            messages_in_partition = high - low
            total_messages += messages_in_partition
        logger.info("Messages found")
        logger.info(total_messages)
    except KafkaException as e:
        raise HTTPException(status_code=500, detail=f"Failed to create consumer: {e}")


    stop_event = threading.Event()
    consumer_stop_events[req.name] = stop_event
    t = threading.Thread(target=consume_single_consumer, args=(req.name, req.topics, kafka_consumer, stop_event,  asyncio.get_running_loop()))
    t.start()
    consumer_threads[req.name] = t

    # Store the consumer in the global dict
    consumers[req.name] = kafka_consumer
    consumer_metadata[req.name] = ConsumerInfo(
        name=req.name,
        groupId=req.groupId,
        topics=req.topics,
        status='running',
        messagesConsumed=0,
        autoOffsetRest=req.autoOffsetReset,
        enableAutoCommit=req.enableAutoCommit,
        autoCommitInterval=req.autoCommitInterval
    )

    return {
        "message": f"Consumer {req.name} added successfully",
        "consumer": req.dict(),
        "consumers": get_consumers()
    }

@app.put("/consumers/{consumer_name}")
def update_consumer(consumer_name: str, req: ConsumerConfigRequest):
    if consumer_name not in consumers:
        print("Consumer " + str(consumer_name) + " found")
    else:
        # Close existing consumer safely
        consumer_stop_events[consumer_name].set()  # signal to stop
        consumer_threads[consumer_name].join()     # wait for thread to finish
        consumers[consumer_name].close()
    
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
    
    consumers[consumer_name] = consumer
    consumer_metadata[req.name] = ConsumerInfo(
        name=req.name,
        groupId=req.groupId,
        topics=req.topics,
        status='running',
        messagesConsumed=0,
        autoOffsetRest=req.autoOffsetReset,
        enableAutoCommit=req.enableAutoCommit,
        autoCommitInterval=req.autoCommitInterval
    )

    return {
        "success": True,
        "consumer_name": consumer_name,
        "group_id": req.group_id,
        "topics": req.topics,
        "manual_offsets": req.offsets is not None,
        "consumers": get_consumers()
    }

@app.delete("/consumers/{consumer_name}")
def delete_consumer(consumer_name: str):
    if consumer_name not in consumers:
        raise HTTPException(status_code=404, detail="Consumer not found")

    try:
        consumer_stop_events[consumer_name].set()  # signal to stop
        consumer_threads[consumer_name].join()     # wait for thread to finish           
        print(f"Consumer {consumer_name} stopped gracefully")
        
        # Remove from dictionaries
        del consumer_stop_events[consumer_name]
        del consumer_threads[consumer_name]
        consumer = consumers.pop(consumer_name)   # remove from active consumers
        consumer_metadata.pop(consumer_name, None)  # clean up metadata if present

        return {"success": True, "message": f"Consumer {consumer_name} deleted", "consumers": get_consumers()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete consumer: {str(e)}")

# -------------------------------
# Endpoint: Consume latest messages from a configured consumer
# -------------------------------
@app.post("/consume")
async def get_consumer_messages(req: ConsumerRequest):
    logger.info(req.consumerName)
    logger.info(consumers)
    if req.consumerName and req.consumerName not in consumers:
        raise HTTPException(status_code=404, detail="Consumer not found")
    
    query = messages.select()
    count_query = messages.select().with_only_columns(func.count())


    # Filter by consumer name
    if req.consumerName:
        query = query.where(messages.c.consumer_name == req.consumerName)
        count_query = count_query.where(messages.c.consumer_name == req.consumerName)

    # Filter by topic
    if req.topic:
        query = query.where(messages.c.topic == req.topic)
        count_query = count_query.where(messages.c.topic == req.topic)
    # Filter by searchTerm
    if req.searchTerm:
        query = query.where(messages.c.value.ilike(f"%{searchTerm}%"))
        count_query = count_query.where(messages.c.value.ilike(f"%{searchTerm}%"))

    # Order newest first by timestamp
    query = query.order_by(messages.c.timestamp.desc())
    
    # Pagination
    query = query.limit(req.limit).offset(req.offset)
    results = await database.fetch_all(query)

    # Total count for remaining offsets
    total_count = await database.fetch_val(count_query)
    
    return {
        "consumerName": req.consumerName or "all",
        "topic": req.topic or "all",
        "limit": req.limit,
        "messageOffset": req.offset,
        "messages": results,
        "totalCount": total_count,
    }

# -------------------------------
# Producers
# -------------------------------
@app.get("/producers")
def get_producers():
    return list(producer_metadata.values())

@app.post("/producers/create")
def create_producer(req: ProducerConfigRequest):
    if req.name in producers:
        raise HTTPException(
            status_code=400, 
            detail=f"Producer '{req.name}' already exists"
        )

    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": req.acks,
        "batch.size": req.batchSize,  
        "linger.ms": req.lingerMs,
        "compression.type": req.compressionType,
        "retries": req.retries,
    })

    producers[req.name] = producer

    producer_metadata[req.name] = ProducerInfo(
        name=req.name,
        topic=req.topic,
        messagesSent=0,
        acks = req.acks,
        batchSize = req.batchSize,
        lingerMs = req.lingerMs,
        retries = req.retries,
        compressionType = req.compressionType
    )
    
    return {
        "success": True,
        "producer": producer_metadata[req.name],
        "producers": get_producers()
    }

@app.post("/producers")
def update_producer(req: ProducerConfigRequest):
    logger.info(producers)
    logger.info(producer_metadata)
    if req.name in producers:
        # Flush and discard old producer
        producers[req.name].flush()

    logger.info(req)
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": req.acks,
        "batch.size": req.batchSize,  
        "linger.ms": req.lingerMs,
        "compression.type": req.compressionType,
        "retries": req.retries,
    })

    producers[req.name] = producer
    producer_metadata[req.name] = ProducerInfo(
        name=req.name,
        topic=req.topic,
        messagesSent=0,
        acks = req.acks,
        batchSize = req.batchSize,
        lingerMs = req.lingerMs,
        retries = req.retries,
        compressionType = req.compressionType
    )
    return {"success": True, "producer_name": req.name, "producers": get_producers()}

@app.post("/produce")
def produce_message(req: ProducerRequest):
    if req.name not in producers:
        raise HTTPException(status_code=404, detail="Producer not found")

    producer = producers[req.name]

    try:
        producer.produce(req.topic, req.message.encode())
        producer.flush()

        # update stats
        meta = producer_metadata[req.name]
        meta.messagesSent += 1

        return {
            "success": True,
            "topic": req.topic,
            "message": req.message,
            "producer_name": req.name,
            "producers": get_producers()
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.delete("/producers/{name}")
def delete_producer(name: str):
    logger.info(producers)
    if name not in producers:
        raise HTTPException(
            status_code=404,
            detail=f"Producer '{name}' not found"
        )

    try:
        producer = producers.pop(name)
        producer.flush(timeout=5)  # flush pending messages
        producer_metadata.pop(name)

        return {
            "success": True,
            "message": f"Producer '{name}' stopped and removed",
            "producers": get_producers()
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error stopping producer '{name}': {str(e)}"
        )

def send_cat_gossip(name: str, topic: str, duration: int):
    """Background thread to send gossip messages for duration"""
    if name not in producers:
        return
    
    producer = producers[name]
    meta = producer_metadata[name]
    end_time = time.time() + duration

    while time.time() < end_time:
        random_message = random.choice(cat_gossip_messages)
        cat_name = random.choice(['Whiskers', 'Mr. Mittens', 'Luna', 'Shadow', 'Princess Fluffy', 'Garfield'])
 
        msg = cat_name + random_message
        try:
            producer.produce(topic, msg.encode("utf-8"))
            meta.messagesSent += 1
            logger.info(f"[CAT GOSSIP] {name} -> {topic}: {msg}")
        except Exception as e:
            logger.error(f"Producer {name} error: {e}")
            break
        time.sleep(1)  # send roughly 1 message per second
    
    producer.flush()

@app.post("/producers/cat-gossip/{name}")
def start_cat_gossip(name: str, req: GossipRequest):
    logger.info(producers)
    logger.info(producer_metadata)
    if name not in producers:
        raise HTTPException(status_code=404, detail="Producer not found")

    # Start gossiping in background thread
    thread = threading.Thread(
        target=send_cat_gossip,
        args=(name, req.topic, req.durationSeconds),
        daemon=True
    )
    thread.start()

    return {
        "success": True,
        "producer_id": name,
        "topic": req.topic,
        "duration_seconds": req.durationSeconds,
        "status": "cat gossip mission started"
    }

