
export interface Broker {
  broker_id: number;
  hostname: string;
  port: number;
  role: 'controller' | 'follower';
  status: 'running' | 'stopped' | 'error';
  num_partitions_as_leader: number;
  num_partitions_as_follower: number;
}

export interface Topic {
  name: string;
  partitions: number;
  replicationFactor: number;
  retentionHours: number;
}

export interface Partition {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  offset: number;
}

export interface Producer {
  name: string;
  topic: string;
  messagesSent: number;
  batchSize: number;
  lingerMs: number;
  acks: 'all' | '1' | '0';
  retries: number;
  compressionType: 'none' | 'gzip' | 'snappy' | 'lz4';
  isContinuousMessaging?: boolean;
  continuousMessagingEnd?: number;
}

export interface ProducerRequest {
  name: string;
  topic: string;
  message: string;
  partition?: number; // optional, defaults to 0 if not provided
}

export interface GossipRequest {
  duration_seconds: number;
  topic: string;
}

export interface Consumer {
  name: string;
  groupId: string;
  topics: string[];
  status: 'running' | 'stopped' | 'error';
  messagesConsumed: number;
  rate: number; // messages per second
  autoOffsetReset: 'earliest' | 'latest' | 'none';
  enableAutoCommit: boolean;
  autoCommitInterval: number;
}

export interface ConsumerGroup {
  id: string;
  members: number;
  topics: string[];
  status: 'stable' | 'empty' | 'dead';
  lag: number;
}

export interface ConsumerRequest {
  consumerName?: string;
  topic?: string;
  searchTerm?: string;
  limit: number;
  offset: number;
}

export interface Message {
  key: string;
  value: string;
  partition: number;
  message_offset: number;
  timestamp: string;
  topic: string;
  consumer_name?: string;
}

export interface PartitionAssignment {
  partition: number;
  consumerName: string;
  topic: string;
  committedOffset: number;
  latestOffset: number;
  leader: number;
  replicas: number[];
  lag: number;
}

export interface PartitionReassignment {
  id: string;
  topic: string;
  partition: number;
  fromConsumer: string;
  toConsumer: string;
  timestamp: string;
  status: 'pending' | 'completed' | 'failed';
}


