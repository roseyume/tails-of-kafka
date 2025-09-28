import React, { useState, useEffect, useMemo } from 'react';
import axios from "axios";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card';
import { Button } from './ui/button';
import { Badge } from './ui/badge';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { Textarea } from './ui/textarea';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from './ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from './ui/alert-dialog';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { ScrollArea } from './ui/scroll-area';
import { Pagination, PaginationContent, PaginationItem, PaginationLink, PaginationNext, PaginationPrevious } from './ui/pagination';
import { MessageSquare, Plus, Trash2, Play, Square, Users, Eye, ArrowLeftRight, Filter, Search, Settings, Edit, Send } from 'lucide-react';
import { toast } from 'sonner@2.0.3';

interface Consumer {
  id: string;
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

interface ConsumerGroup {
  id: string;
  members: number;
  topics: string[];
  state: 'stable' | 'rebalancing' | 'dead';
  lag: number;
}

interface Message {
  key: string;
  value: string;
  partition: number;
  offset: number;
  timestamp: string;
  topic: string;
  consumerId?: string;
  consumerName?: string;
}

interface PartitionAssignment {
  partition: number;
  consumerId: string;
  consumerName: string;
  offset: number;
  lag: number;
}

interface PartitionReassignment {
  id: string;
  topic: string;
  partition: number;
  fromConsumer: string;
  toConsumer: string;
  timestamp: string;
  status: 'pending' | 'completed' | 'failed';
}

export function ConsumersManagement({topics}) {
  const apiURL = "https://8000-roseyume-tailsofkafka-md4yrcdut1c.ws-us121.gitpod.io";

  const [consumers, setConsumers] = useState<Consumer[]>([
    {
      id: 'cons-1',
      name: 'User Events Consumer',
      groupId: 'user-analytics',
      topics: ['user-events'],
      status: 'running',
      messagesConsumed: 1203,
      rate: 15,
      autoOffsetReset: 'latest',
      enableAutoCommit: true,
      autoCommitInterval: 5000,
    },
    {
      id: 'cons-2',
      name: 'Order Processing Consumer',
      groupId: 'order-processing',
      topics: ['order-events'],
      status: 'running',
      messagesConsumed: 742,
      rate: 8,
      autoOffsetReset: 'earliest',
      enableAutoCommit: true,
      autoCommitInterval: 1000,
    },
    {
      id: 'cons-3',
      name: 'Notification Consumer',
      groupId: 'notifications',
      topics: ['notifications'],
      status: 'stopped',
      messagesConsumed: 0,
      rate: 0,
      autoOffsetReset: 'latest',
      enableAutoCommit: false,
      autoCommitInterval: 5000,
    },
  ]);

  const [consumerGroups] = useState<ConsumerGroup[]>([
    {
      id: 'user-analytics',
      members: 2,
      topics: ['user-events'],
      state: 'stable',
      lag: 45,
    },
    {
      id: 'order-processing',
      members: 1,
      topics: ['order-events'],
      state: 'stable',
      lag: 12,
    },
    {
      id: 'notifications',
      members: 0,
      topics: ['notifications'],
      state: 'dead',
      lag: 0,
    },
  ]);


  const [partitionAssignments] = useState<PartitionAssignment[]>([
    { partition: 0, consumerId: 'cons-1', consumerName: 'User Events Consumer', offset: 1542, lag: 15 },
    { partition: 1, consumerId: 'cons-1', consumerName: 'User Events Consumer', offset: 1541, lag: 8 },
    { partition: 2, consumerId: 'cons-2', consumerName: 'Order Processing Consumer', offset: 893, lag: 23 },
    { partition: 0, consumerId: 'cons-2', consumerName: 'Order Processing Consumer', offset: 445, lag: 12 },
  ]);

  const [partitionReassignments, setPartitionReassignments] = useState<PartitionReassignment[]>([
    {
      id: 'rebalance-1',
      topic: 'user-events',
      partition: 1,
      fromConsumer: 'cons-2',
      toConsumer: 'cons-1',
      timestamp: '10:25:30',
      status: 'completed',
    },
    {
      id: 'rebalance-2',
      topic: 'order-events',
      partition: 0,
      fromConsumer: 'cons-1',
      toConsumer: 'cons-2',
      timestamp: '10:20:15',
      status: 'pending',
    },
  ]);

  const [newConsumer, setNewConsumer] = useState({
    name: '',
    groupId: '',
    topics: [] as string[],
    autoOffsetReset: 'latest' as const,
    enableAutoCommit: true,
    autoCommitInterval: 5000,
  });

  const [consumedMessages, setConsumedMessages] = useState([]);
  const [isCreatingConsumer, setIsCreatingConsumer] = useState(false);
  const [selectedTopicForMessages, setSelectedTopicForMessages] = useState('user-events');
  const [selectedConsumerForMessages, setSelectedConsumerForMessages] = useState<string>('all');
  const [messageViewMode, setMessageViewMode] = useState<'topic' | 'consumer'>('topic');
  const [currentPage, setCurrentPage] = useState(1);
  const [searchTerm, setSearchTerm] = useState('');
  const [editingConsumer, setEditingConsumer] = useState<Consumer | null>(null);
  const [isEditingConfig, setIsEditingConfig] = useState(false);
  const [isSendingMessage, setIsSendingMessage] = useState(false);
  const [selectedConsumerForSending, setSelectedConsumerForSending] = useState<Consumer | null>(null);
  const [messageToSend, setMessageToSend] = useState({
    key: '',
    value: '{"catId": "whiskers-01", "observation": "Suspicious squirrel spotted near bird feeder at 0900 hours. Recommend increased surveillance.", "priority": "high", "reporter": "Agent Whiskers"}',
    partition: undefined as number | undefined,
  });
  const messagesPerPage = 50;

  const createConsumer = async() => {
    if (!newConsumer.name.trim() || !newConsumer.groupId.trim() || newConsumer.topics.length === 0) {
      toast.error('Consumer name, group ID, and at least one topic are required');
      return;
    }

    const consumer: Consumer = {
      id: `cons-${Date.now()}`,
      ...newConsumer,
      status: 'stopped',
      messagesConsumed: 0,
      rate: 0,
    };

    const [consumerResponse] = await Promise.all([
      axios.post(`${apiURL}/consumers/create`, newConsumer)
    ]);
    setConsumers(consumerResponse.data.consumers);

    setNewConsumer({
      name: '',
      groupId: '',
      topics: [],
      autoOffsetReset: 'latest',
      enableAutoCommit: true,
      autoCommitInterval: 5000,
    });
    setIsCreatingConsumer(false);
    toast.success(`Consumer "${consumer.name}" created successfully`);
  };

  const startConsumer = (id: string) => {
    setConsumers(prev => prev.map(consumer => 
      consumer.id === id ? { 
        ...consumer, 
        status: 'running' as const,
        rate: Math.floor(Math.random() * 30) + 5 
      } : consumer
    ));
    toast.success('Consumer started successfully');
  };

  const stopConsumer = (id: string) => {
    setConsumers(prev => prev.map(consumer => 
      consumer.id === id ? { 
        ...consumer, 
        status: 'stopped' as const,
        rate: 0 
      } : consumer
    ));
    toast.success('Consumer stopped successfully');
  };

  const deleteConsumer = (id: string) => {
    setConsumers(prev => prev.filter(consumer => consumer.id !== id));
    toast.success('Consumer deleted successfully');
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'running': return 'bg-green-100 text-green-800';
      case 'stopped': return 'bg-gray-100 text-gray-800';
      case 'error': return 'bg-red-100 text-red-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getGroupStateColor = (state: string) => {
    switch (state) {
      case 'stable': return 'bg-green-100 text-green-800';
      case 'rebalancing': return 'bg-yellow-100 text-yellow-800';
      case 'dead': return 'bg-red-100 text-red-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const runningConsumers = consumers.filter(c => c.status === 'running').length;
  const totalMessagesConsumed = consumers.reduce((sum, c) => sum + c.messagesConsumed, 0);
  const totalGroups = new Set(consumers.map(c => c.groupId)).size;

  // Filter and paginate messages
  const filteredMessages = useMemo(() => {
    let filtered = consumedMessages;

    // Filter by search term
    if (searchTerm) {
      filtered = filtered.filter(msg => 
        msg.value.toLowerCase().includes(searchTerm.toLowerCase()) ||
        msg.key.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }

    // Filter by view mode
    if (messageViewMode === 'topic') {
      filtered = filtered.filter(msg => msg.topic === selectedTopicForMessages);
    } else {
      if (selectedConsumerForMessages !== 'all') {
        filtered = filtered.filter(msg => msg.consumerId === selectedConsumerForMessages);
      }
    }

    return filtered;
  }, [consumedMessages, messageViewMode, selectedTopicForMessages, selectedConsumerForMessages, searchTerm]);

  const paginatedMessages = useMemo(() => {
    const start = (currentPage - 1) * messagesPerPage;
    const end = start + messagesPerPage;
    return filteredMessages.slice(start, end);
  }, [filteredMessages, currentPage, messagesPerPage]);

  const totalPages = Math.ceil(filteredMessages.length / messagesPerPage);

  const updateConsumerConfig = () => {
    if (!editingConsumer) return;

    setConsumers(prev => prev.map(c => 
      c.id === editingConsumer.id ? editingConsumer : c
    ));
    setEditingConsumer(null);
    setIsEditingConfig(false);
    toast.success('Consumer configuration updated successfully');
  };

  const sendMessage = () => {
    if (!messageToSend.value.trim() || !selectedConsumerForSending) {
      toast.error('Message value and consumer are required');
      return;
    }

    // Simulate sending message via the selected consumer
    setConsumers(prev => prev.map(c => 
      c.id === selectedConsumerForSending.id ? { ...c, messagesConsumed: c.messagesConsumed + 1 } : c
    ));

    setMessageToSend({ 
      key: '', 
      value: '{"catId": "whiskers-01", "observation": "Suspicious squirrel spotted near bird feeder at 0900 hours. Recommend increased surveillance.", "priority": "high", "reporter": "Agent Whiskers"}', 
      partition: undefined 
    });
    setIsSendingMessage(false);
    setSelectedConsumerForSending(null);
    toast.success(`Message sent via consumer "${selectedConsumerForSending.name}"`);
  };

  const triggerPartitionReassignment = () => {
    const newReassignment: PartitionReassignment = {
      id: `rebalance-${Date.now()}`,
      topic: 'user-events',
      partition: Math.floor(Math.random() * 3),
      fromConsumer: 'cons-1',
      toConsumer: 'cons-2',
      timestamp: new Date().toLocaleTimeString(),
      status: 'pending',
    };

    setPartitionReassignments(prev => [newReassignment, ...prev]);
    toast.success('Partition reassignment triggered');

    // Simulate completion after 2 seconds
    setTimeout(() => {
      setPartitionReassignments(prev => 
        prev.map(r => r.id === newReassignment.id ? { ...r, status: 'completed' as const } : r)
      );
    }, 2000);
  };

  return (
    <div className="space-y-6">
      {/* Consumers Overview */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Consumers Overview</CardTitle>
              <CardDescription>Manage Kafka message consumers and consumer groups</CardDescription>
            </div>
            <Dialog open={isCreatingConsumer} onOpenChange={setIsCreatingConsumer}>
              <DialogTrigger asChild>
                <Button>
                  <Plus className="w-4 h-4 mr-2" />
                  Create Consumer
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Create New Consumer</DialogTitle>
                  <DialogDescription>
                    Configure a new Kafka message consumer
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label htmlFor="consumer-name">Consumer Name</Label>
                    <Input
                      id="consumer-name"
                      value={newConsumer.name}
                      onChange={(e) => setNewConsumer(prev => ({ ...prev, name: e.target.value }))}
                      placeholder="My Consumer"
                    />
                  </div>
                  
                  <div className="space-y-2">
                    <Label htmlFor="group-id">Consumer Group ID</Label>
                    <Input
                      id="group-id"
                      value={newConsumer.groupId}
                      onChange={(e) => setNewConsumer(prev => ({ ...prev, groupId: e.target.value }))}
                      placeholder="my-consumer-group"
                    />
                  </div>
                  
                  <div className="space-y-2">
                    <Label>Topics to Subscribe</Label>
                    <div className="space-y-2">
                      {topics.map(topic => (
                        <label key={topic.name} className="flex items-center space-x-2">
                          <input
                            type="checkbox"
                            checked={newConsumer.topics.includes(topic)}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setNewConsumer(prev => ({ 
                                  ...prev, 
                                  topics: [...prev.topics, topic.name] 
                                }));
                              } else {
                                setNewConsumer(prev => ({ 
                                  ...prev, 
                                  topics: prev.topics.filter(t => t !== topic.name) 
                                }));
                              }
                            }}
                            className="rounded"
                          />
                          <span className="text-sm">{topic.name}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                  
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="auto-offset-reset">Auto Offset Reset</Label>
                      <Select
                        value={newConsumer.autoOffsetReset}
                        onValueChange={(value: any) => setNewConsumer(prev => ({ ...prev, autoOffsetReset: value }))}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="earliest">earliest</SelectItem>
                          <SelectItem value="latest">latest</SelectItem>
                          <SelectItem value="none">none</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    
                    <div className="space-y-2">
                      <Label htmlFor="auto-commit-interval">Auto Commit Interval (ms)</Label>
                      <Input
                        id="auto-commit-interval"
                        type="number"
                        value={newConsumer.autoCommitInterval}
                        onChange={(e) => setNewConsumer(prev => ({ 
                          ...prev, 
                          autoCommitInterval: parseInt(e.target.value) 
                        }))}
                        disabled={!newConsumer.enableAutoCommit}
                      />
                    </div>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      id="enable-auto-commit"
                      checked={newConsumer.enableAutoCommit}
                      onChange={(e) => setNewConsumer(prev => ({ 
                        ...prev, 
                        enableAutoCommit: e.target.checked 
                      }))}
                      className="rounded"
                    />
                    <Label htmlFor="enable-auto-commit">Enable Auto Commit</Label>
                  </div>
                  
                  <div className="flex justify-end space-x-2">
                    <Button variant="outline" onClick={() => setIsCreatingConsumer(false)}>
                      Cancel
                    </Button>
                    <Button onClick={createConsumer}>Create Consumer</Button>
                  </div>
                </div>
              </DialogContent>
            </Dialog>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="space-y-2">
              <p className="text-sm font-medium">Total Consumers</p>
              <p className="text-2xl font-bold">{consumers.length}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Running Consumers</p>
              <p className="text-2xl font-bold text-green-600">{runningConsumers}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Consumer Groups</p>
              <p className="text-2xl font-bold">{totalGroups}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Messages Consumed</p>
              <p className="text-2xl font-bold">{totalMessagesConsumed.toLocaleString()}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Consumers Table */}
      <Card>
        <CardHeader>
          <CardTitle>Consumer Management</CardTitle>
          <CardDescription>Control and monitor your Kafka consumers</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Consumer Name</TableHead>
                <TableHead>Group</TableHead>
                <TableHead>Topics</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Messages Consumed</TableHead>
                <TableHead>Rate (msg/s)</TableHead>
                <TableHead>Configuration</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {consumers.map((consumer) => (
                <TableRow key={consumer.id}>
                  <TableCell className="font-medium">{consumer.name}</TableCell>
                  <TableCell>{consumer.groupId}</TableCell>
                  <TableCell>{consumer.topics.join(', ')}</TableCell>
                  <TableCell>
                    <Badge className={getStatusColor(consumer.status)}>
                      {consumer.status}
                    </Badge>
                  </TableCell>
                  <TableCell>{consumer.messagesConsumed.toLocaleString()}</TableCell>
                  <TableCell>{consumer.rate}</TableCell>
                  <TableCell>
                    <div className="text-sm text-muted-foreground">
                      Offset: {consumer.autoOffsetReset}, Auto-commit: {consumer.enableAutoCommit ? 'On' : 'Off'}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex space-x-1">
                      {consumer.status === 'running' ? (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => stopConsumer(consumer.id)}
                        >
                          <Square className="w-3 h-3 mr-1" />
                          Stop
                        </Button>
                      ) : (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => startConsumer(consumer.id)}
                        >
                          <Play className="w-3 h-3 mr-1" />
                          Start
                        </Button>
                      )}
                      
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setEditingConsumer(consumer);
                          setIsEditingConfig(true);
                        }}
                      >
                        <Edit className="w-3 h-3" />
                      </Button>

                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setSelectedConsumerForSending(consumer);
                          setIsSendingMessage(true);
                        }}
                      >
                        <Send className="w-3 h-3" />
                      </Button>
                      
                      <AlertDialog>
                        <AlertDialogTrigger asChild>
                          <Button variant="outline" size="sm">
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </AlertDialogTrigger>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>Delete Consumer</AlertDialogTitle>
                            <AlertDialogDescription>
                              Are you sure you want to delete the consumer "{consumer.name}"? 
                              This action cannot be undone.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction onClick={() => deleteConsumer(consumer.id)}>
                              Delete
                            </AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Consumer Groups */}
      <Card>
        <CardHeader>
          <CardTitle>Consumer Groups</CardTitle>
          <CardDescription>Monitor consumer group status and lag</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {consumerGroups.map((group) => (
              <div key={group.id} className="flex items-center justify-between p-3 border rounded-lg">
                <div className="space-y-1">
                  <h4 className="font-medium">{group.id}</h4>
                  <p className="text-sm text-muted-foreground">
                    {group.topics.join(', ')}
                  </p>
                </div>
                <div className="flex items-center space-x-4">
                  <div className="text-center">
                    <p className="text-sm font-medium">{group.members}</p>
                    <p className="text-xs text-muted-foreground">members</p>
                  </div>
                  <div className="text-center">
                    <p className="text-sm font-medium">{group.lag}</p>
                    <p className="text-xs text-muted-foreground">lag</p>
                  </div>
                  <Badge className={getGroupStateColor(group.state)}>
                    {group.state}
                  </Badge>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Partition Assignments and Reassignments */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Partition Assignments</CardTitle>
                <CardDescription>Current partition assignments per consumer</CardDescription>
              </div>
              <Button onClick={triggerPartitionReassignment} variant="outline">
                <ArrowLeftRight className="w-4 h-4 mr-2" />
                Trigger Rebalance
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Partition</TableHead>
                  <TableHead>Consumer</TableHead>
                  <TableHead>Offset</TableHead>
                  <TableHead>Lag</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {partitionAssignments.map((assignment, index) => (
                  <TableRow key={index}>
                    <TableCell>{assignment.partition}</TableCell>
                    <TableCell className="font-medium">{assignment.consumerName}</TableCell>
                    <TableCell>{assignment.offset.toLocaleString()}</TableCell>
                    <TableCell>
                      <Badge variant={assignment.lag > 20 ? "destructive" : "secondary"}>
                        {assignment.lag}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Partition Reassignments</CardTitle>
            <CardDescription>Recent partition reassignment activity</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {partitionReassignments.map((reassignment) => (
                <div key={reassignment.id} className="p-3 border rounded-lg">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <Badge variant="outline">{reassignment.topic}</Badge>
                      <Badge variant="outline">Partition {reassignment.partition}</Badge>
                    </div>
                    <Badge className={
                      reassignment.status === 'completed' ? 'bg-green-100 text-green-800' :
                      reassignment.status === 'pending' ? 'bg-yellow-100 text-yellow-800' :
                      'bg-red-100 text-red-800'
                    }>
                      {reassignment.status}
                    </Badge>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    <p>From: {reassignment.fromConsumer} → To: {reassignment.toConsumer}</p>
                    <p>Time: {reassignment.timestamp}</p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Consumed Messages */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Consumed Messages</CardTitle>
              <CardDescription>
                {filteredMessages.length.toLocaleString()} messages 
                {messageViewMode === 'consumer' && selectedConsumerForMessages !== 'all' 
                  ? ` from ${consumers.find(c => c.id === selectedConsumerForMessages)?.name || 'Unknown Consumer'}` 
                  : ` from topic ${selectedTopicForMessages}`
                }
              </CardDescription>
            </div>
            <div className="flex items-center space-x-2">
              <div className="relative">
                <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search messages..."
                  value={searchTerm}
                  onChange={(e) => {
                    setSearchTerm(e.target.value);
                    setCurrentPage(1);
                  }}
                  className="pl-8 w-48"
                />
              </div>
              <Select value={messageViewMode} onValueChange={(value: any) => {
                setMessageViewMode(value);
                setCurrentPage(1);
              }}>
                <SelectTrigger className="w-32">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="topic">By Topic</SelectItem>
                  <SelectItem value="consumer">By Consumer</SelectItem>
                </SelectContent>
              </Select>
              {messageViewMode === 'topic' ? (
                <Select value={selectedTopicForMessages} onValueChange={(value) => {
                  setSelectedTopicForMessages(value);
                  setCurrentPage(1);
                }}>
                  <SelectTrigger className="w-48">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {topics.map(topic => (
                      <SelectItem key={topic.name} value={topic.name}>{topic.name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <Select value={selectedConsumerForMessages} onValueChange={(value) => {
                  setSelectedConsumerForMessages(value);
                  setCurrentPage(1);
                }}>
                  <SelectTrigger className="w-64">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Consumers</SelectItem>
                    {consumers.map(consumer => (
                      <SelectItem key={consumer.id} value={consumer.id}>{consumer.name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <ScrollArea className="h-96">
            <div className="space-y-3">
              {paginatedMessages.map((message, index) => (
                <div key={`${message.topic}-${message.offset}-${index}`} className="p-3 border rounded-lg bg-muted/30">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <Badge variant="outline">Topic: {message.topic}</Badge>
                      <Badge variant="outline">Partition {message.partition}</Badge>
                      <Badge variant="outline">Offset {message.offset}</Badge>
                      {message.key && (
                        <Badge variant="outline">Key: {message.key}</Badge>
                      )}
                      {message.consumerName && (
                        <Badge variant="secondary">{message.consumerName}</Badge>
                      )}
                    </div>
                    <span className="text-sm text-muted-foreground">{message.timestamp}</span>
                  </div>
                  <pre className="text-sm bg-background p-2 rounded border overflow-x-auto">
                    {message.value}
                  </pre>
                </div>
              ))}
            </div>
          </ScrollArea>
          
          {totalPages > 1 && (
            <div className="mt-4 flex justify-center">
              <Pagination>
                <PaginationContent>
                  <PaginationItem>
                    <PaginationPrevious 
                      onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                      className={currentPage === 1 ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                    />
                  </PaginationItem>
                  
                  {Array.from({ length: Math.min(totalPages, 5) }, (_, i) => {
                    let pageNum;
                    if (totalPages <= 5) {
                      pageNum = i + 1;
                    } else {
                      const start = Math.max(1, currentPage - 2);
                      const end = Math.min(totalPages, start + 4);
                      pageNum = start + i;
                      if (pageNum > end) return null;
                    }
                    
                    return (
                      <PaginationItem key={pageNum}>
                        <PaginationLink
                          onClick={() => setCurrentPage(pageNum)}
                          isActive={currentPage === pageNum}
                          className="cursor-pointer"
                        >
                          {pageNum}
                        </PaginationLink>
                      </PaginationItem>
                    );
                  })}
                  
                  <PaginationItem>
                    <PaginationNext 
                      onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                      className={currentPage === totalPages ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                    />
                  </PaginationItem>
                </PaginationContent>
              </Pagination>
            </div>
          )}
          
          <div className="mt-2 text-center text-sm text-muted-foreground">
            Showing {((currentPage - 1) * messagesPerPage) + 1} - {Math.min(currentPage * messagesPerPage, filteredMessages.length)} of {filteredMessages.length.toLocaleString()} messages
          </div>
        </CardContent>
      </Card>

      {/* Send Message Dialog */}
      <Dialog open={isSendingMessage} onOpenChange={setIsSendingMessage}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Send Message via Consumer 🐱</DialogTitle>
            <DialogDescription>
              Send a cat surveillance message via "{selectedConsumerForSending?.name}"
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="message-key">Message Key (optional)</Label>
              <Input
                id="message-key"
                value={messageToSend.key}
                onChange={(e) => setMessageToSend(prev => ({ ...prev, key: e.target.value }))}
                placeholder="cat-agent-01"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="message-value">Message Value</Label>
              <Textarea
                id="message-value"
                value={messageToSend.value}
                onChange={(e) => setMessageToSend(prev => ({ ...prev, value: e.target.value }))}
                placeholder='{"catId": "whiskers-01", "observation": "Suspicious activity detected"}'
                rows={6}
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="message-partition">Partition (optional)</Label>
              <Input
                id="message-partition"
                type="number"
                value={messageToSend.partition || ''}
                onChange={(e) => setMessageToSend(prev => ({ 
                  ...prev, 
                  partition: e.target.value ? parseInt(e.target.value) : undefined 
                }))}
                placeholder="0"
              />
            </div>
            
            <div className="flex justify-end space-x-2">
              <Button variant="outline" onClick={() => setIsSendingMessage(false)}>
                Cancel
              </Button>
              <Button onClick={sendMessage}>
                <Send className="w-4 h-4 mr-2" />
                Send Cat Message
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Consumer Configuration Edit Dialog */}
      <Dialog open={isEditingConfig} onOpenChange={setIsEditingConfig}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Consumer Configuration</DialogTitle>
            <DialogDescription>
              Update the configuration for "{editingConsumer?.name}"
            </DialogDescription>
          </DialogHeader>
          {editingConsumer && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="edit-consumer-name">Consumer Name</Label>
                <Input
                  id="edit-consumer-name"
                  value={editingConsumer.name}
                  onChange={(e) => setEditingConsumer(prev => prev ? { ...prev, name: e.target.value } : null)}
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="edit-group-id">Consumer Group ID</Label>
                <Input
                  id="edit-group-id"
                  value={editingConsumer.groupId}
                  onChange={(e) => setEditingConsumer(prev => prev ? { ...prev, groupId: e.target.value } : null)}
                />
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="edit-auto-offset-reset">Auto Offset Reset</Label>
                  <Select
                    value={editingConsumer.autoOffsetReset}
                    onValueChange={(value: any) => setEditingConsumer(prev => prev ? { ...prev, autoOffsetReset: value } : null)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="earliest">earliest</SelectItem>
                      <SelectItem value="latest">latest</SelectItem>
                      <SelectItem value="none">none</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="edit-auto-commit-interval">Auto Commit Interval (ms)</Label>
                  <Input
                    id="edit-auto-commit-interval"
                    type="number"
                    value={editingConsumer.autoCommitInterval}
                    onChange={(e) => setEditingConsumer(prev => prev ? { 
                      ...prev, 
                      autoCommitInterval: parseInt(e.target.value) 
                    } : null)}
                    disabled={!editingConsumer.enableAutoCommit}
                  />
                </div>
              </div>
              
              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id="edit-enable-auto-commit"
                  checked={editingConsumer.enableAutoCommit}
                  onChange={(e) => setEditingConsumer(prev => prev ? { 
                    ...prev, 
                    enableAutoCommit: e.target.checked 
                  } : null)}
                  className="rounded"
                />
                <Label htmlFor="edit-enable-auto-commit">Enable Auto Commit</Label>
              </div>
              
              <div className="flex justify-end space-x-2">
                <Button variant="outline" onClick={() => setIsEditingConfig(false)}>
                  Cancel
                </Button>
                <Button onClick={updateConsumerConfig}>Update Configuration</Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Consumer Configuration Guide */}
      <Card>
        <CardHeader>
          <CardTitle>Consumer Configuration Guide</CardTitle>
          <CardDescription>Understanding consumer configuration options</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <h4 className="font-medium mb-2">Auto Offset Reset</h4>
                <ul className="text-sm text-muted-foreground space-y-1">
                  <li><strong>earliest:</strong> Start from the beginning of the topic</li>
                  <li><strong>latest:</strong> Start from the latest messages</li>
                  <li><strong>none:</strong> Throw exception if no offset found</li>
                </ul>
              </div>
              
              <div>
                <h4 className="font-medium mb-2">Consumer Groups</h4>
                <p className="text-sm text-muted-foreground">
                  Consumers with the same group ID work together to consume a topic. 
                  Each partition is consumed by only one consumer in the group.
                </p>
              </div>
            </div>
            
            <div className="space-y-4">
              <div>
                <h4 className="font-medium mb-2">Auto Commit</h4>
                <p className="text-sm text-muted-foreground">
                  When enabled, offsets are automatically committed at regular intervals. 
                  When disabled, you must manually commit offsets.
                </p>
              </div>
              
              <div>
                <h4 className="font-medium mb-2">Consumer Lag</h4>
                <p className="text-sm text-muted-foreground">
                  The difference between the latest offset and the consumer's current offset. 
                  High lag indicates the consumer is falling behind.
                </p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Send Message Dialog */}
      <Dialog open={isSendingMessage} onOpenChange={setIsSendingMessage}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Send Message via Consumer 🐱</DialogTitle>
            <DialogDescription>
              Send a cat surveillance message via "{selectedConsumerForSending?.name}"
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="message-key">Message Key (optional)</Label>
              <Input
                id="message-key"
                value={messageToSend.key}
                onChange={(e) => setMessageToSend(prev => ({ ...prev, key: e.target.value }))}
                placeholder="cat-agent-01"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="message-value">Message Value</Label>
              <Textarea
                id="message-value"
                value={messageToSend.value}
                onChange={(e) => setMessageToSend(prev => ({ ...prev, value: e.target.value }))}
                placeholder='{"catId": "whiskers-01", "observation": "Suspicious activity detected"}'
                rows={6}
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="message-partition">Partition (optional)</Label>
              <Input
                id="message-partition"
                type="number"
                value={messageToSend.partition || ''}
                onChange={(e) => setMessageToSend(prev => ({ 
                  ...prev, 
                  partition: e.target.value ? parseInt(e.target.value) : undefined 
                }))}
                placeholder="0"
              />
            </div>
            
            <div className="flex justify-end space-x-2">
              <Button variant="outline" onClick={() => setIsSendingMessage(false)}>
                Cancel
              </Button>
              <Button onClick={sendMessage}>
                <Send className="w-4 h-4 mr-2" />
                Send Cat Message
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Consumer Configuration Edit Dialog */}
      <Dialog open={isEditingConfig} onOpenChange={setIsEditingConfig}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Consumer Configuration</DialogTitle>
            <DialogDescription>
              Update the configuration for "{editingConsumer?.name}"
            </DialogDescription>
          </DialogHeader>
          {editingConsumer && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="edit-consumer-name">Consumer Name</Label>
                <Input
                  id="edit-consumer-name"
                  value={editingConsumer.name}
                  onChange={(e) => setEditingConsumer(prev => prev ? { ...prev, name: e.target.value } : null)}
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="edit-group-id">Consumer Group ID</Label>
                <Input
                  id="edit-group-id"
                  value={editingConsumer.groupId}
                  onChange={(e) => setEditingConsumer(prev => prev ? { ...prev, groupId: e.target.value } : null)}
                />
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="edit-auto-offset-reset">Auto Offset Reset</Label>
                  <Select
                    value={editingConsumer.autoOffsetReset}
                    onValueChange={(value: any) => setEditingConsumer(prev => prev ? { ...prev, autoOffsetReset: value } : null)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="earliest">earliest</SelectItem>
                      <SelectItem value="latest">latest</SelectItem>
                      <SelectItem value="none">none</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="edit-auto-commit-interval">Auto Commit Interval (ms)</Label>
                  <Input
                    id="edit-auto-commit-interval"
                    type="number"
                    value={editingConsumer.autoCommitInterval}
                    onChange={(e) => setEditingConsumer(prev => prev ? { 
                      ...prev, 
                      autoCommitInterval: parseInt(e.target.value) 
                    } : null)}
                    disabled={!editingConsumer.enableAutoCommit}
                  />
                </div>
              </div>
              
              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id="edit-enable-auto-commit"
                  checked={editingConsumer.enableAutoCommit}
                  onChange={(e) => setEditingConsumer(prev => prev ? { 
                    ...prev, 
                    enableAutoCommit: e.target.checked 
                  } : null)}
                  className="rounded"
                />
                <Label htmlFor="edit-enable-auto-commit">Enable Auto Commit</Label>
              </div>
              
              <div className="flex justify-end space-x-2">
                <Button variant="outline" onClick={() => setIsEditingConfig(false)}>
                  Cancel
                </Button>
                <Button onClick={updateConsumerConfig}>Update Configuration</Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}