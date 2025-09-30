from pydantic import BaseModel
from typing import List, Optional, Dict, Literal

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

class Topic(BaseModel):
    name: str
    partitions: int
    replicationFactor: int
    retentionHours: int

class ProducerInfo(BaseModel):
    name: str
    topic: str
    messagesSent: int
    batchSize: int 
    lingerMs: int
    acks: str
    retries: int
    compressionType: str

class ConsumerInfo(BaseModel):
    name: str
    groupId: str
    topics: List[str]
    status: Literal['running', 'stopped', 'error'] = 'stopped'
    messagesConsumed: int = 0
    rate: float = 0.0  # messages per second
    autoOffsetReset: Literal['earliest', 'latest', 'none'] = 'earliest'
    enableAutoCommit: bool = True
    autoCommitInterval: int = 5000  # in milliseconds

class Message(BaseModel):
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: str
    timestamp: str

class TopicRequest(BaseModel):
    name: str
    partitions: int = 1
    replicationFactor: int = 1
    retentionHours: int = 168

class PartitionsRequest(BaseModel):
    name: str
    additional_partitions: int

class ConsumerRequest(BaseModel):
    consumerName: Optional[str] = None  # filter by consumer name
    topic: Optional[str] = None          # filter by topic
    searchTerm: Optional[str] = None
    limit: int = 50
    offset: int = 0

class ProducerRequest(BaseModel):
    name: str
    topic: str
    message: str
    partition: Optional[int] = 0   

class ConsumerConfigRequest(BaseModel):
    name: str       # human-readable name and unique internal key
    group_id: str
    topics: List[str]
    enable_auto_commit: bool = True
    auto_offset_reset: str = "latest"
    max_poll_records: Optional[int] = 500
    isolation_level: Optional[str] = "read_uncommitted"
    fetch_min_bytes: Optional[int] = 1_000
    offsets: Optional[Dict[str, int]] = None  # {topic: offset}

class ProducerConfigRequest(BaseModel):
    name: str
    topic: str
    acks: str = "1"
    batchSize: int = 100
    lingerMs: int = 0
    retries: int = 0
    compressionType: str = "none"

class GossipRequest(BaseModel):
    durationSeconds: int
    topic: str
