from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic, ConfigResource
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException, KafkaError, ConsumerGroupTopicPartitions
from fastapi import FastAPI, HTTPException
from models.schemas import *
from database.init import database
from database.tables import messages
from kafka_consumer import consume_single_consumer
from kafka_producer import ProducerTracker
import logging, re
import yaml
import subprocess
import os
import sys
import time
import threading, queue
import random
import asyncio
from sqlalchemy import func, select, delete
from concurrent.futures import ThreadPoolExecutor

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
executor = ThreadPoolExecutor(max_workers=10)
script_dir = os.path.dirname(os.path.abspath(__file__))
DOCKER_COMPOSE_FILE = os.path.join(script_dir, "..", "docker-compose.yml")

@app.on_event("startup")
async def startup():
    await database.connect()
    # asyncio.create_task(db_writer())

@app.on_event("shutdown")
async def shutdown():
    for consumer_name in consumer_stop_events.keys():
        consumer_stop_events[consumer_name].set()  # signal to stop
        consumer_threads[consumer_name].join()     # wait for thread to finish
        consumers[consumer_name].close()
        print(f"Consumer {consumer_name} stopped gracefully")
    await database.execute(str("TRUNCATE TABLE messages RESTART IDENTITY"))
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
BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094"
admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

# Consumers
consumers: Dict[str, Consumer] = {}
consumer_metadata: Dict[str, ConsumerInfo] = {}
consumer_threads = {}       
consumer_stop_events = {}   

# Producers
producers: Dict[str, Producer] = {}
producer_metadata: Dict[str, ProducerInfo] = {}

# -------------------------------
# Cluster endpoints
# -------------------------------
@app.get("/cluster")
async def get_cluster_configs():
    try:
        # --- Get cluster metadata to discover brokers ---
        metadata = admin.list_topics(timeout=10)

        # Collect broker IDs from replicas (alive + dead)
        broker_ids = [broker.id for broker in metadata.brokers.values()]

        if not broker_ids:
            raise HTTPException(status_code=500, detail="No brokers found in cluster")

        # --- TODO: Fix this so it accurately gives the global configs ---
        cluster_resource = ConfigResource("BROKER", str(broker_ids[0]))
        cluster_future = admin.describe_configs([cluster_resource])
        global_configs = {}
        try:
            cluster_result = cluster_future[cluster_resource].result()
            for name, entry in cluster_result.items():
                global_configs[str(name)] = {
                    "value": entry.value,
                    "is_default": entry.is_default,
                    "is_read_only": entry.is_read_only,
                    "is_sensitive": entry.is_sensitive,
                    "source": str(entry.source),
                }
        except Exception as e:
            global_configs["error"] = str(e)

        # --- Get per-broker configs (loop each broker) ---
        broker_configs = {}

        for broker_id in broker_ids:
            res = ConfigResource("BROKER", str(broker_id))
            future = admin.describe_configs([res])

            configs = {}
            try:
                result = future[res].result()
                for name, entry in result.items():
                    configs[name] = {
                        "value": entry.value,
                        "is_default": entry.is_default,
                        "is_read_only": entry.is_read_only,
                    }
            except Exception as e:
                configs["error"] = str(e)

            broker_configs[broker_id] = configs

        return {
            "bootstrap_servers": BOOTSTRAP_SERVERS,
            "global_configs": global_configs,  # from broker 0
            "brokers": broker_configs,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch cluster configs: {e}")

@app.post("/cluster/update")
async def update_cluster_config(req: ClusterConfigRequest):
    try:
        config = {
            "default.replication.factor": req.defaultReplicationFactor,
            "log.retention.hours": req.logRetentionHours,
            "min.insync.replicas": req.minInsyncReplicas,
            "segment.bytes": req.segmentSizeMb
        }
        # Apply configs to the cluster (broker resource type = BROKER = 4)
        # Passing `broker_id=0` means "apply to the default broker config for all brokers"
        resource = ConfigResource("BROKER", "0", config)
        futures = admin.alter_configs([resource])

        # Wait for completion
        for r, f in futures.items():
            f.result()  # raises exception on failure

        return {"success": True, "updated": new_config}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update cluster config: {str(e)}")
# -------------------------------
# Broker endpoints
# -------------------------------
@app.get("/brokers", response_model=List[Broker])
async def get_brokers():
    try:
        metadata = admin.list_topics(timeout=5)
        nodes = []
        controller_id = metadata.controller_id

        # Live brokers (from metadata)
        live_brokers = set(metadata.brokers.keys())
    
        # Build broker node info (live + dead)
        # Initialize broker stats for all brokers (live + dead)
        broker_stats = {broker_id: {"leader_count": 0, "follower_count": 0}
                        for broker_id in live_brokers}

        # Count leader/follower partitions per broker
        for topic in metadata.topics.values():

            for partition_id, partition in topic.partitions.items():
                leader_id = partition.leader
                replicas = partition.replicas
                if replicas:
                    for broker_id in replicas:
                        if broker_id == leader_id:
                            broker_stats[broker_id]["leader_count"] += 1
                        else:
                            if(broker_stats.get(broker_id)):
                                broker_stats[broker_id]["follower_count"] += 1
                            else:
                                broker_stats.setdefault(broker_id, {"leader_count": 0, "follower_count": 0})
                                broker_stats[broker_id]["follower_count"] += 1

        for broker_id in live_brokers:
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
            nodes.append(node)
        
        stopped_broker = get_stopped_brokers()
        
        for broker in stopped_broker:
            if broker.broker_id not in live_brokers:
                nodes.append(broker)

        return nodes

    except Exception as e:
        return get_stopped_brokers()

def get_stopped_brokers(file_path=DOCKER_COMPOSE_FILE):
    """
    Reads docker-compose.yml, extracts Kafka brokers, and returns a list of Broker objects
    with status='stopped' for brokers assumed stopped.
    
    """
    with open(file_path, "r") as f:
        compose = yaml.safe_load(f)

    brokers_list = []
    services = compose.get("services", {})
    for service_name, service_def in services.items():
        image = service_def.get("image", "")
        if "kafka" not in image.lower() and not service_name.startswith("kafka"):
            continue

        broker_id = int(service_def.get("container_name").split("-")[1])

        external_port = 0

        # Parse KAFKA_LISTENERS
        env_vars = service_def.get("environment", {})
        kafka_listeners = env_vars.get("KAFKA_LISTENERS", "")
        for listener in kafka_listeners.split(","):
            if listener.startswith("LISTENER_EXTERNAL"):
                match = re.search(r":(\d+)$", listener.strip())
                if match:
                    external_port = int(match.group(1))
                break

        broker_obj = Broker(
                    broker_id=broker_id,
                    # hostname="localhost" if external_port != 0 else "unknown",
                    # port=external_port,
                    hostname="unknown",
                    port=0,
                    status="stopped",
                    role="unknown",
                    num_partitions_as_leader=0,
                    num_partitions_as_follower=0,
                )
                
        brokers_list.append(broker_obj)

    return brokers_list

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

        BOOTSTRAP_SERVERS.append("localhost:" + str(29093+broker_id))
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

        return {"status": "success", "broker": await get_brokers()}
    except Exception as e:
        print(e)

# Continuously read stdout and stderr
async def read_stream(stream, name):
    while True:
        line = await stream.readline()
        if not line:
            break
        print(f"[{name}] {line.decode().rstrip()}")

async def run_compose_command(sleep_time, *args):
    """Run docker-compose command on the given service."""
    try:
        process = await asyncio.create_subprocess_exec(
           "docker-compose",
            "-f", DOCKER_COMPOSE_FILE,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        await asyncio.sleep(sleep_time)
        await asyncio.gather(
            read_stream(process.stdout, "stdout"),
            read_stream(process.stderr, "stderr")
        )

        return_code = await process.wait()
        print(f"Process exited with code {return_code}", flush=True)
        return return_code
    except Exception as e:
        print(f"Error running docker-compose: {e}")
        raise

    #     stdout, stderr = await process.communicate()


    #     if process.returncode != 0:
    #         raise RuntimeError(
    #             f"docker-compose failed with code {process.returncode}: {stderr.decode()}"
    #         )

    #     time.sleep(5)

    # except Exception as e:
    #     print(f"Error running docker-compose: {e}")
    #     raise

    # return stdout.decode().strip()

@app.post("/brokers/stop/{broker_id}")
async def stop_broker(broker_id: int):
    service_name = find_service_name(broker_id)
    if not service_name:
        raise HTTPException(status_code=404, detail=f"Broker {broker_id} not found")
    await run_compose_command(2, "stop", service_name)

    return {"status": "stopped", "service_name": service_name, "broker": await get_brokers()}


@app.post("/brokers/restart/{broker_id}")
async def restart_broker(broker_id: int):
    service_name = find_service_name(broker_id)
    if not service_name:
        raise HTTPException(status_code=404, detail=f"Broker {broker_id} not found")

    await run_compose_command(10, "restart", service_name)
    
    return {"status": "restarted", "broker_id": broker_id, "service_name": service_name, "broker": await get_brokers()}


@app.delete("/brokers/delete/{broker_id}")
async def delete_broker(broker_id: int):
    service_name = find_service_name(broker_id)
    if not service_name:
        raise HTTPException(status_code=404, detail=f"Broker {broker_id} not found")

    # Stop the container first
    await run_compose_command(1, "stop", service_name)
    # Remove the container
    await run_compose_command(1, "rm", "-f", service_name)

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

def topic_has_active_consumers(topic_name: str) -> bool:
    """Return True if any consumer group is actively consuming from the topic."""
    groups = admin.list_consumer_groups(request_timeout=10).result()
    group_ids = [group.group_id for group in groups.valid]
    if len(group_ids) == 0:
        return False

    group_metadata = admin.describe_consumer_groups(group_ids, request_timeout=10)
    for group_id, meta in group_metadata.items():
        try:
            group_desc = meta.result()
            for member in group_desc.members:
                assignment = member.assignment
                if topic_name in assignment.topic_partitions:
                    return True
        except Exception:
            continue
    return False

@app.delete("/topics/{topic_name}")
async def delete_topic(topic_name: str):
    """Delete a Kafka topic after checking for active consumers."""
    try:
        metadata = admin.list_topics(timeout=10)
  
        if not (topic_name in metadata.topics.keys()):
            return list_topics()

        if topic_has_active_consumers(topic_name):
            raise HTTPException(
                status_code=409,
                detail=f"Topic '{topic_name}' has active consumers. Stop them before deleting.",
            )

        futures = admin.delete_topics([topic_name])
        futures[topic_name].result()

        return list_topics()

    except KafkaException as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
async def list_partitions():
    assigned = []
    unassigned = []
    consumer_groups = []
    try:
        groups = admin.list_consumer_groups(request_timeout=10).result()
        group_ids = [group.group_id for group in groups.valid]

        if len(group_ids) == 0:
            return {
                "assignedConsumers": assigned,
                "unassignedConsumers": unassigned,
                "consumerGroups": consumer_groups
            }

        group_descriptions = admin.describe_consumer_groups(group_ids, request_timeout = 10)
        for group_id in group_ids:
            curr_assigned = []
            # Get committed offsets for group
            group = group_descriptions[group_id].result()
            group_spec = ConsumerGroupTopicPartitions(group_id, None) 
            group_offsets = admin.list_consumer_group_offsets([group_spec])[group_id].result()
 
            # Get all topics this group is consuming
            topics = list({topic_partition.topic for topic_partition in group_offsets.topic_partitions})
            topic_metadata = admin.list_topics(timeout=10)
            group_members = group.members

            for member in group_members:
                consumer_name = member.client_id
                assignment = member.assignment  # list of TopicPartitions
                assigned_topics = {tp.topic for tp in assignment.topic_partitions}

                # Check each assigned partition
                for tp in assignment.topic_partitions:
                    committed = None

                    for offset_tp in group_offsets.topic_partitions:
                        if offset_tp.topic == tp.topic and offset_tp.partition == tp.partition:
                            committed = offset_tp.offset
                            break

                    if consumer_name in consumers:
                        latest = consumers[consumer_name].get_watermark_offsets(tp)[1]
                        lag = latest - committed if committed is not None and committed >= 0 else None
                    else:
                        latest = None
                        lag = None

                    partition_metadata = topic_metadata.topics.get(tp.topic).partitions.get(tp.partition)

                    assigned_partition = {
                        "consumerName": consumer_name,
                        "consumerGroup": group_id,
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "committedOffset": committed,
                        "latestOffset": latest,
                        "leader": partition_metadata.leader,
                        "replicas": partition_metadata.replicas,
                        "lag": lag
                    }
                    curr_assigned.append(assigned_partition)
                    assigned.append(assigned_partition)

                # Detect unassigned topics for this consumer
                for topic in topics:
                    if topic not in assigned_topics:
                        unassigned.append({
                            "groupId": group_id,
                            "consumer": consumer_name,
                            "unassignedTopic": topic
                        })

            consumer_groups.append({
                "id": group_id,
                "members": len([member.client_id for member in group_members]),
                "topics": topics,
                "lag": sum([a['lag'] if a['lag'] else 0 for a in curr_assigned]),
                "status": 'empty' if len([member.client_id for member in group_members]) == 0 else 'running'

            })

        return {
            "assignedConsumers": assigned,
            "unassignedConsumers": unassigned,
            "consumerGroups": consumer_groups
        }

    except KafkaException as e:
        return {"error": str(e)}

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
        "client.id" : req.name,
        "bootstrap.servers": BOOTSTRAP_SERVERS,  # default, could be extended to frontend
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
        autoOffsetReset=req.autoOffsetReset,
        enableAutoCommit=req.enableAutoCommit,
        autoCommitInterval=req.autoCommitInterval
    )

    return {
        "message": f"Consumer {req.name} added successfully",
        "consumer": req.dict(),
        "consumers": get_consumers()
    }

@app.put("/consumers/{consumer_name}")
async def update_consumer(consumer_name: str, req: ConsumerConfigRequest):
    if consumer_name in consumers: 
        stop_consumer(consumer_name)
    
    # Re-create consumer with new config
    # conf = {
    #     "bootstrap.servers": "localhost:9092",
    #     "group.id": req.group_id,
    #     "enable.auto.commit": req.enable_auto_commit,
    #     "auto.offset.reset": req.auto_offset_reset,
    #     # "max.poll.records": req.max_poll_records,
    #     # "isolation.level": req.isolation_level,
    #     # "fetch.min.bytes": req.fetch_min_bytes
    # }

    consumerInfo = ConsumerInfo(
        name=consumer_name,
        groupId=req.groupId,
        topics=req.topics,
        status='running',
        messagesConsumed=0,
        autoOffsetRest=req.autoOffsetReset,
        enableAutoCommit=req.enableAutoCommit,
        autoCommitInterval=req.autoCommitInterval
    )
    
    # if req.offsets:
    #     assignments = [TopicPartition(topic, 0, offset) for topic, offset in req.offsets.items()]
    #     consumer.assign(assignments)
    # else:
    #     consumer.subscribe(req.topics)
    
    try:
        result = await create_consumer(consumer_metadata[consumer_name])
        return {"success": True, "message": f"Consumer {consumer_name} updated", "consumers": result['consumers']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete consumer: {str(e)}")


@app.get("/consumers/resume/{consumer_name}")
async def resume_consumer(consumer_name: str):
    if consumer_name not in consumer_metadata:
        raise HTTPException(status_code=404, detail="Consumer not found")

    try:
        result = await create_consumer(consumer_metadata[consumer_name])
        return {"success": True, "message": f"Consumer {consumer_name} started", "consumers": result['consumers']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete consumer: {str(e)}")


@app.get("/consumers/stop/{consumer_name}")
def stop_consumer(consumer_name: str):
    if consumer_name not in consumers:
        raise HTTPException(status_code=404, detail="Consumer not found")

    try:
        consumer_stop_events[consumer_name].set()  # signal to stop
        del consumer_stop_events[consumer_name]
        consumer_threads[consumer_name].join()     # wait for thread to finish  
        del consumer_threads[consumer_name]
        consumer = consumers.pop(consumer_name)   # remove from active consumers
        consumer.close()       
        consumer_metadata[consumer_name].status = 'stopped' 
        logger.info("Consumer {consumer_name} stopped gracefully")

        return {"success": True, "message": f"Consumer {consumer_name} deleted", "consumers": get_consumers()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete consumer: {str(e)}")


@app.delete("/consumers/{consumer_name}")
async def delete_consumer(consumer_name: str):
    if consumer_name not in consumer_metadata:
        raise HTTPException(status_code=404, detail="Consumer not found")

    try:
        if consumer_name in consumers:
            stop_consumer(consumer_name)

        # Remove from dictionaries
        consumer_metadata.pop(consumer_name, None)  # clean up metadata if present
        query = delete(messages).where(messages.c.consumer_name == consumer_name)
        result = await database.execute(query)

        return {"success": True, "message": f"Consumer {consumer_name} deleted", "consumers": get_consumers()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete consumer: {str(e)}")

# -------------------------------
# Endpoint: Consume latest messages from a configured consumer
# -------------------------------
@app.post("/consume")
async def get_consumer_messages(req: ConsumerRequest):
    if req.consumerName and req.consumerName not in consumer_metadata:
        raise HTTPException(status_code=404, detail="Consumer not found")
    
    query = messages.select()
    count_query = select(func.count()).select_from(messages)


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
        query = query.where(messages.c.value.ilike(f"%{req.searchTerm}%"))
        count_query = count_query.where(messages.c.value.ilike(f"%{req.searchTerm}%"))

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

    producers[req.name] = ProducerTracker(req.name, {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": req.acks,
        "batch.size": req.batchSize,  
        "linger.ms": req.lingerMs,
        "compression.type": req.compressionType,
        "retries": req.retries,
    })

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
    if req.name in producers:
        # Flush and discard old producer
        producers[req.name].flush()

    producers[req.name] = ProducerTracker(req.name, {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": req.acks,
        "batch.size": req.batchSize,  
        "linger.ms": req.lingerMs,
        "compression.type": req.compressionType,
        "retries": req.retries,
    })

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
        producer.send_message(req.topic, req.message.encode())
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
    
    start_time = time.time()
    producer = producers[name]
    meta = producer_metadata[name]
    end_time = time.time() + duration

    while time.time() < end_time:
        random_message = random.choice(cat_gossip_messages)
        cat_name = random.choice(['Whiskers', 'Mr. Mittens', 'Luna', 'Shadow', 'Princess Fluffy', 'Garfield'])
 
        msg = cat_name + random_message
        try:
            producer.send_message(topic, msg)
            logger.info(f"[CAT GOSSIP] {name} -> {topic}: {msg}")
            time.sleep(1)  # small delay between gossips

        except Exception as e:
            logger.error(f"Producer {name} error: {e}")
            break  
    
    producer.flush()

@app.post("/producers/cat-gossip/{name}")
def start_cat_gossip(name: str, req: GossipRequest):
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

