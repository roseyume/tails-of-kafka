import React, { useState } from 'react';
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
import { Send, Plus, Trash2, Play, Square, Settings, MessageCircle, Clock } from 'lucide-react';
import { toast } from 'sonner@2.0.3';

interface Producer {
  id: string;
  name: string;
  topic: string;
  status: 'running' | 'stopped' | 'error';
  messagesSent: number;
  rate: number; // messages per second
  batchSize: number;
  acks: 'all' | '1' | '0';
  retries: number;
  compressionType: 'none' | 'gzip' | 'snappy' | 'lz4';
  isContinuousMessaging?: boolean;
  continuousMessagingEnd?: number;
}

interface Message {
  key?: string;
  value: string;
  headers?: { [key: string]: string };
  partition?: number;
}

export function ProducersManagement() {
  const [producers, setProducers] = useState<Producer[]>([
    {
      id: 'prod-1',
      name: 'User Events Producer',
      topic: 'user-events',
      status: 'running',
      messagesSent: 1542,
      rate: 23,
      batchSize: 100,
      acks: 'all',
      retries: 3,
      compressionType: 'none',
    },
    {
      id: 'prod-2',
      name: 'Order Events Producer',
      topic: 'order-events',
      status: 'stopped',
      messagesSent: 893,
      rate: 0,
      batchSize: 50,
      acks: '1',
      retries: 2,
      compressionType: 'gzip',
    },
  ]);

  const [newProducer, setNewProducer] = useState({
    name: '',
    topic: '',
    batchSize: 100,
    acks: 'all' as const,
    retries: 3,
    compressionType: 'none' as const,
  });

  const [messageToSend, setMessageToSend] = useState<Message>({
    key: '',
    value: '',
    headers: {},
    partition: undefined,
  });

  const [isCreatingProducer, setIsCreatingProducer] = useState(false);
  const [isSendingMessage, setIsSendingMessage] = useState(false);
  const [selectedProducerTopic, setSelectedProducerTopic] = useState('user-events');
  const [editingProducer, setEditingProducer] = useState<Producer | null>(null);
  const [isEditingConfig, setIsEditingConfig] = useState(false);
  const [isContinuousMessaging, setIsContinuousMessaging] = useState(false);
  const [continuousProducer, setContinuousProducer] = useState<Producer | null>(null);
  const [continuousDuration, setContinuousDuration] = useState(30); // seconds

  const topics = ['user-events', 'order-events', 'notifications'];

  // Cat gossip messages for continuous messaging feature
  const catGossipMessages = [
    "Whiskers saw a suspicious squirrel in the oak tree. Investigation ongoing.",
    "Mr. Mittens reports that the mailman arrived 3 minutes early today. Concerning.",
    "Luna observed the neighbors getting a new cat carrier. Possible escape plan needed.",
    "Shadow confirms that the red dot is still at large. All units on high alert.",
    "Princess Fluffy spotted unknown cat in backyard at 0300 hours. Territory breach!",
    "Garfield notes that dinner was served 2.5 minutes late. Unacceptable service levels.",
    "Pepper reports successful counter-surfing mission. Tuna sandwich acquired.",
    "Smokey witnessed the humans moving furniture. Possible fortress reconstruction.",
    "Bella confirms that the laser pointer has been relocated to top shelf. Access denied.",
    "Oscar reports strange noises from the washing machine. Possible monster habitat.",
    "Mimi observed the vacuum cleaner in closet. Threat level: Orange.",
    "Felix successfully infiltrated the forbidden bathroom counter. Mission accomplished.",
    "Chloe reports that new scratching post has been delivered. Quality testing required.",
    "Max witnessed delivery truck. Possible invasion. Recommend increased vigilance.",
    "Nala confirms that catnip stash remains hidden from human detection.",
    "Tiger reports successful nap completion. Duration: 14.7 hours. Highly satisfactory.",
    "Zoe observed bird activity outside window increasing by 23%. Hunting opportunities abound.",
    "Charlie confirms that favorite cardboard box has been moved. Emergency protocols activated.",
    "Lily reports that water bowl is now 78% full instead of usual 80%. Concerning trend.",
    "Oreo witnessed treat jar opening. All units converged within 0.3 seconds."
  ];

  const createProducer = () => {
    if (!newProducer.name.trim() || !newProducer.topic) {
      toast.error('Producer name and topic are required');
      return;
    }

    const producer: Producer = {
      id: `prod-${Date.now()}`,
      ...newProducer,
      status: 'stopped',
      messagesSent: 0,
      rate: 0,
    };

    setProducers(prev => [...prev, producer]);
    setNewProducer({
      name: '',
      topic: '',
      batchSize: 100,
      acks: 'all',
      retries: 3,
      compressionType: 'none',
    });
    setIsCreatingProducer(false);
    toast.success(`Producer "${producer.name}" created successfully`);
  };

  const startProducer = (id: string) => {
    setProducers(prev => prev.map(producer => 
      producer.id === id ? { 
        ...producer, 
        status: 'running' as const,
        rate: Math.floor(Math.random() * 50) + 10 
      } : producer
    ));
    toast.success('Producer started successfully');
  };

  const stopProducer = (id: string) => {
    setProducers(prev => prev.map(producer => 
      producer.id === id ? { 
        ...producer, 
        status: 'stopped' as const,
        rate: 0 
      } : producer
    ));
    toast.success('Producer stopped successfully');
  };

  const deleteProducer = (id: string) => {
    setProducers(prev => prev.filter(producer => producer.id !== id));
    toast.success('Producer deleted successfully');
  };

  const sendMessage = () => {
    if (!messageToSend.value.trim()) {
      toast.error('Message value is required');
      return;
    }

    // Simulate sending message
    const producer = producers.find(p => p.topic === selectedProducerTopic);
    if (producer) {
      setProducers(prev => prev.map(p => 
        p.id === producer.id ? { ...p, messagesSent: p.messagesSent + 1 } : p
      ));
    }

    setMessageToSend({ key: '', value: '', headers: {}, partition: undefined });
    setIsSendingMessage(false);
    toast.success(`Message sent to topic "${selectedProducerTopic}"`);
  };

  const updateProducerConfig = () => {
    if (!editingProducer) return;

    setProducers(prev => prev.map(p => 
      p.id === editingProducer.id ? editingProducer : p
    ));
    setEditingProducer(null);
    setIsEditingConfig(false);
    toast.success('Producer configuration updated successfully');
  };

  const startContinuousMessaging = () => {
    if (!continuousProducer) return;

    const endTime = Date.now() + (continuousDuration * 1000);
    
    setProducers(prev => prev.map(p => 
      p.id === continuousProducer.id ? { 
        ...p, 
        status: 'running' as const,
        isContinuousMessaging: true,
        continuousMessagingEnd: endTime,
        rate: Math.floor(Math.random() * 20) + 10
      } : p
    ));

    // Send cat gossip messages
    const messageInterval = setInterval(() => {
      if (Date.now() >= endTime) {
        clearInterval(messageInterval);
        setProducers(prev => prev.map(p => 
          p.id === continuousProducer.id ? { 
            ...p, 
            isContinuousMessaging: false,
            continuousMessagingEnd: undefined
          } : p
        ));
        toast.success('Continuous messaging completed! The cats have finished their gossip session.');
        return;
      }

      const randomMessage = catGossipMessages[Math.floor(Math.random() * catGossipMessages.length)];
      const catName = ['Whiskers', 'Mr. Mittens', 'Luna', 'Shadow', 'Princess Fluffy', 'Garfield'][Math.floor(Math.random() * 6)];
      
      setProducers(prev => prev.map(p => 
        p.id === continuousProducer.id ? { 
          ...p, 
          messagesSent: p.messagesSent + 1 
        } : p
      ));
    }, 2000); // Send message every 2 seconds

    setIsContinuousMessaging(false);
    setContinuousProducer(null);
    toast.success(`Started continuous cat gossip messaging for ${continuousDuration} seconds!`);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'running': return 'bg-green-100 text-green-800';
      case 'stopped': return 'bg-gray-100 text-gray-800';
      case 'error': return 'bg-red-100 text-red-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const runningProducers = producers.filter(p => p.status === 'running').length;
  const totalMessagesSent = producers.reduce((sum, p) => sum + p.messagesSent, 0);

  return (
    <div className="space-y-6">
      {/* Producers Overview */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Producers Overview</CardTitle>
              <CardDescription>Manage Kafka message producers</CardDescription>
            </div>
            <div className="flex space-x-2">
              <Dialog open={isSendingMessage} onOpenChange={setIsSendingMessage}>
                <DialogTrigger asChild>
                  <Button variant="outline">
                    <Send className="w-4 h-4 mr-2" />
                    Send Message
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Send Message</DialogTitle>
                    <DialogDescription>
                      Send a message to a Kafka topic
                    </DialogDescription>
                  </DialogHeader>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label htmlFor="message-topic">Topic</Label>
                      <Select value={selectedProducerTopic} onValueChange={setSelectedProducerTopic}>
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {topics.map(topic => (
                            <SelectItem key={topic} value={topic}>{topic}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    
                    <div className="space-y-2">
                      <Label htmlFor="message-key">Message Key (optional)</Label>
                      <Input
                        id="message-key"
                        value={messageToSend.key}
                        onChange={(e) => setMessageToSend(prev => ({ ...prev, key: e.target.value }))}
                        placeholder="user-123"
                      />
                    </div>
                    
                    <div className="space-y-2">
                      <Label htmlFor="message-value">Message Value</Label>
                      <Textarea
                        id="message-value"
                        value={messageToSend.value}
                        onChange={(e) => setMessageToSend(prev => ({ ...prev, value: e.target.value }))}
                        placeholder='{"userId": "123", "action": "login"}'
                        rows={4}
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
                      <Button onClick={sendMessage}>Send Message</Button>
                    </div>
                  </div>
                </DialogContent>
              </Dialog>
              
              <Dialog open={isCreatingProducer} onOpenChange={setIsCreatingProducer}>
                <DialogTrigger asChild>
                  <Button>
                    <Plus className="w-4 h-4 mr-2" />
                    Create Producer
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Create New Producer</DialogTitle>
                    <DialogDescription>
                      Configure a new Kafka message producer
                    </DialogDescription>
                  </DialogHeader>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label htmlFor="producer-name">Producer Name</Label>
                      <Input
                        id="producer-name"
                        value={newProducer.name}
                        onChange={(e) => setNewProducer(prev => ({ ...prev, name: e.target.value }))}
                        placeholder="My Producer"
                      />
                    </div>
                    
                    <div className="space-y-2">
                      <Label htmlFor="producer-topic">Topic</Label>
                      <Select
                        value={newProducer.topic}
                        onValueChange={(value) => setNewProducer(prev => ({ ...prev, topic: value }))}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select a topic" />
                        </SelectTrigger>
                        <SelectContent>
                          {topics.map(topic => (
                            <SelectItem key={topic} value={topic}>{topic}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label htmlFor="batch-size">Batch Size</Label>
                        <Input
                          id="batch-size"
                          type="number"
                          value={newProducer.batchSize}
                          onChange={(e) => setNewProducer(prev => ({ ...prev, batchSize: parseInt(e.target.value) }))}
                        />
                      </div>
                      
                      <div className="space-y-2">
                        <Label htmlFor="retries">Retries</Label>
                        <Input
                          id="retries"
                          type="number"
                          value={newProducer.retries}
                          onChange={(e) => setNewProducer(prev => ({ ...prev, retries: parseInt(e.target.value) }))}
                        />
                      </div>
                    </div>
                    
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label htmlFor="acks">Acknowledgments</Label>
                        <Select
                          value={newProducer.acks}
                          onValueChange={(value: any) => setNewProducer(prev => ({ ...prev, acks: value }))}
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="0">0 - No acknowledgment</SelectItem>
                            <SelectItem value="1">1 - Leader acknowledgment</SelectItem>
                            <SelectItem value="all">all - Full ISR acknowledgment</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      
                      <div className="space-y-2">
                        <Label htmlFor="compression">Compression</Label>
                        <Select
                          value={newProducer.compressionType}
                          onValueChange={(value: any) => setNewProducer(prev => ({ ...prev, compressionType: value }))}
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="none">None</SelectItem>
                            <SelectItem value="gzip">GZIP</SelectItem>
                            <SelectItem value="snappy">Snappy</SelectItem>
                            <SelectItem value="lz4">LZ4</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    
                    <div className="flex justify-end space-x-2">
                      <Button variant="outline" onClick={() => setIsCreatingProducer(false)}>
                        Cancel
                      </Button>
                      <Button onClick={createProducer}>Create Producer</Button>
                    </div>
                  </div>
                </DialogContent>
              </Dialog>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="space-y-2">
              <p className="text-sm font-medium">Total Producers</p>
              <p className="text-2xl font-bold">{producers.length}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Running Producers</p>
              <p className="text-2xl font-bold text-green-600">{runningProducers}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Messages Sent</p>
              <p className="text-2xl font-bold">{totalMessagesSent.toLocaleString()}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Avg. Rate</p>
              <p className="text-2xl font-bold">
                {runningProducers > 0 
                  ? Math.round(producers.filter(p => p.status === 'running').reduce((sum, p) => sum + p.rate, 0) / runningProducers)
                  : 0
                } msg/s
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Producers Table */}
      <Card>
        <CardHeader>
          <CardTitle>Producer Management</CardTitle>
          <CardDescription>Control and monitor your Kafka producers</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Producer Name</TableHead>
                <TableHead>Topic</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Messages Sent</TableHead>
                <TableHead>Rate (msg/s)</TableHead>
                <TableHead>Configuration</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {producers.map((producer) => (
                <TableRow key={producer.id}>
                  <TableCell className="font-medium">{producer.name}</TableCell>
                  <TableCell>{producer.topic}</TableCell>
                  <TableCell>
                    <Badge className={getStatusColor(producer.status)}>
                      {producer.status}
                    </Badge>
                  </TableCell>
                  <TableCell>{producer.messagesSent.toLocaleString()}</TableCell>
                  <TableCell>{producer.rate}</TableCell>
                  <TableCell>
                    <div className="text-sm text-muted-foreground">
                      Batch: {producer.batchSize}, Acks: {producer.acks}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex space-x-1">
                      {producer.status === 'running' ? (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => stopProducer(producer.id)}
                          disabled={producer.isContinuousMessaging}
                        >
                          <Square className="w-3 h-3 mr-1" />
                          {producer.isContinuousMessaging ? 'Gossiping...' : 'Stop'}
                        </Button>
                      ) : (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => startProducer(producer.id)}
                        >
                          <Play className="w-3 h-3 mr-1" />
                          Start
                        </Button>
                      )}
                      
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setEditingProducer(producer);
                          setIsEditingConfig(true);
                        }}
                      >
                        <Settings className="w-3 h-3" />
                      </Button>

                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setContinuousProducer(producer);
                          setIsContinuousMessaging(true);
                        }}
                        disabled={producer.isContinuousMessaging}
                      >
                        <MessageCircle className="w-3 h-3" />
                      </Button>
                      
                      <AlertDialog>
                        <AlertDialogTrigger asChild>
                          <Button variant="outline" size="sm">
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </AlertDialogTrigger>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>Delete Producer</AlertDialogTitle>
                            <AlertDialogDescription>
                              Are you sure you want to delete the producer "{producer.name}"? 
                              This action cannot be undone.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction onClick={() => deleteProducer(producer.id)}>
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

      {/* Producer Configuration Guide */}
      <Card>
        <CardHeader>
          <CardTitle>Producer Configuration Guide</CardTitle>
          <CardDescription>Understanding producer configuration options</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <h4 className="font-medium mb-2">Acknowledgments (acks)</h4>
                <ul className="text-sm text-muted-foreground space-y-1">
                  <li><strong>0:</strong> No acknowledgment (fastest, least reliable)</li>
                  <li><strong>1:</strong> Leader acknowledgment (balanced)</li>
                  <li><strong>all:</strong> Full ISR acknowledgment (slowest, most reliable)</li>
                </ul>
              </div>
              
              <div>
                <h4 className="font-medium mb-2">Batch Size</h4>
                <p className="text-sm text-muted-foreground">
                  Number of messages to batch together before sending. Higher values increase throughput but add latency.
                </p>
              </div>
            </div>
            
            <div className="space-y-4">
              <div>
                <h4 className="font-medium mb-2">Compression Types</h4>
                <ul className="text-sm text-muted-foreground space-y-1">
                  <li><strong>none:</strong> No compression (fastest)</li>
                  <li><strong>gzip:</strong> Good compression ratio, more CPU</li>
                  <li><strong>snappy:</strong> Fast compression, less CPU</li>
                  <li><strong>lz4:</strong> Very fast, good for high throughput</li>
                </ul>
              </div>
              
              <div>
                <h4 className="font-medium mb-2">Retries</h4>
                <p className="text-sm text-muted-foreground">
                  Number of times to retry failed sends. Higher values improve reliability but may impact ordering.
                </p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Producer Configuration Edit Dialog */}
      <Dialog open={isEditingConfig} onOpenChange={setIsEditingConfig}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Producer Configuration</DialogTitle>
            <DialogDescription>
              Update the configuration for "{editingProducer?.name}"
            </DialogDescription>
          </DialogHeader>
          {editingProducer && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="edit-name">Producer Name</Label>
                <Input
                  id="edit-name"
                  value={editingProducer.name}
                  onChange={(e) => setEditingProducer(prev => prev ? { ...prev, name: e.target.value } : null)}
                />
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="edit-batch-size">Batch Size</Label>
                  <Input
                    id="edit-batch-size"
                    type="number"
                    value={editingProducer.batchSize}
                    onChange={(e) => setEditingProducer(prev => prev ? { ...prev, batchSize: parseInt(e.target.value) } : null)}
                  />
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="edit-retries">Retries</Label>
                  <Input
                    id="edit-retries"
                    type="number"
                    value={editingProducer.retries}
                    onChange={(e) => setEditingProducer(prev => prev ? { ...prev, retries: parseInt(e.target.value) } : null)}
                  />
                </div>
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="edit-acks">Acknowledgments</Label>
                  <Select
                    value={editingProducer.acks}
                    onValueChange={(value: any) => setEditingProducer(prev => prev ? { ...prev, acks: value } : null)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0">0 - No acknowledgment</SelectItem>
                      <SelectItem value="1">1 - Leader acknowledgment</SelectItem>
                      <SelectItem value="all">all - Full ISR acknowledgment</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="edit-compression">Compression</Label>
                  <Select
                    value={editingProducer.compressionType}
                    onValueChange={(value: any) => setEditingProducer(prev => prev ? { ...prev, compressionType: value } : null)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="none">None</SelectItem>
                      <SelectItem value="gzip">GZIP</SelectItem>
                      <SelectItem value="snappy">Snappy</SelectItem>
                      <SelectItem value="lz4">LZ4</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              
              <div className="flex justify-end space-x-2">
                <Button variant="outline" onClick={() => setIsEditingConfig(false)}>
                  Cancel
                </Button>
                <Button onClick={updateProducerConfig}>Update Configuration</Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Continuous Cat Gossip Messaging Dialog */}
      <Dialog open={isContinuousMessaging} onOpenChange={setIsContinuousMessaging}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start Cat Gossip Session 🐱</DialogTitle>
            <DialogDescription>
              Send continuous cat gossip messages from "{continuousProducer?.name}" for a specified duration
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="p-4 bg-muted rounded-lg">
              <p className="text-sm">
                <strong>What happens:</strong> Your producer will send authentic cat surveillance reports 
                and window observations for the specified duration. Messages include sightings of suspicious 
                squirrels, mailman schedule updates, and other critical feline intelligence.
              </p>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="duration">Duration (seconds)</Label>
              <Select
                value={continuousDuration.toString()}
                onValueChange={(value) => setContinuousDuration(parseInt(value))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="15">15 seconds</SelectItem>
                  <SelectItem value="30">30 seconds</SelectItem>
                  <SelectItem value="60">1 minute</SelectItem>
                  <SelectItem value="120">2 minutes</SelectItem>
                  <SelectItem value="300">5 minutes</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label>Sample Cat Gossip Messages:</Label>
              <div className="text-sm text-muted-foreground space-y-1 max-h-32 overflow-y-auto">
                {catGossipMessages.slice(0, 3).map((message, index) => (
                  <p key={index}>• {message}</p>
                ))}
                <p className="italic">...and {catGossipMessages.length - 3} more surveillance reports</p>
              </div>
            </div>
            
            <div className="flex justify-end space-x-2">
              <Button variant="outline" onClick={() => setIsContinuousMessaging(false)}>
                Cancel
              </Button>
              <Button onClick={startContinuousMessaging}>
                <MessageCircle className="w-4 h-4 mr-2" />
                Start Cat Gossip Session
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}