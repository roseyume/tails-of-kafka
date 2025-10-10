import React, { useState, useEffect } from 'react';
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
import { Send, Plus, Trash2, Play, Square, Settings, MessageCircle, Clock } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Producer, ProducerRequest, GossipRequest } from '@/types';

// Cat gossip messages for continuous messaging feature
const catGossipMessages = [
  "Whiskers saw a suspicious squirrel in the oak tree. Investigation ongoing.",
  "Mr. Mittens reports that the mailman arrived 3 minutes early today. Concerning.",
  "Luna observed the neighbors getting a new cat carrier. Possible escape plan needed.",
]

export function ProducersManagement({topics, apiURL, producers, setProducers}) {

  const [newProducer, setNewProducer] = useState({
    name: '',
    topic: '',
    batchSize: 100,
    lingerMs: 0,
    acks: 'all' as const,
    retries: 3,
    compressionType: 'none' as const
  });

  const [messageToSend, setMessageToSend] = useState<ProducerRequest>({
    name: '',
    topic: '',
    message: undefined,
    partition: undefined,
  });

  const [gossipRequest, setGossipRequest] = useState<GossipRequest>({
    durationSeconds: 0,
    topic: ""
  });

  const [isCreatingProducer, setIsCreatingProducer] = useState(false);
  const [isSendingMessage, setIsSendingMessage] = useState(false);
  const [selectedProducerTopic, setSelectedProducerTopic] = useState('user-events');
  const [editingProducer, setEditingProducer] = useState<Producer | null>(null);
  const [isEditingConfig, setIsEditingConfig] = useState(false);
  const [isContinuousMessaging, setIsContinuousMessaging] = useState(false);
  const [continuousProducer, setContinuousProducer] = useState<Producer | null>(null);
  const [messagingProducer, setMessagingProducer] = useState<Producer | null>(null);
  

  const createProducer = async() => {
    if (!newProducer.name.trim() || !newProducer.topic) {
      toast.error('Producer name and topic are required');
      return;
    }

    const producer:Producer = {
      ...newProducer,
      messagesSent: 0
    }
    const [producerResponse] = await Promise.all([
      axios.post(`${apiURL}/producers/create`, newProducer)
    ]);
    setProducers(producerResponse.data.producers)

    setIsCreatingProducer(false);
    toast.success(`Producer "${producer.name}" created successfully`);
    
    setNewProducer({
      name: '',
      topic: '',
      batchSize: 100,
      acks: 'all',
      retries: 3,
      compressionType: 'none',
      messagesSent: 0
    });

  };


  const deleteProducer = async(name: string) => {
    const [producerResponse] = await Promise.all([
      axios.delete(`${apiURL}/producers/${name}`)
    ]);

    setProducers(producerResponse.data.producers)
    toast.success('Producer deleted successfully');
  };

  const sendMessage = async() => {
    if (!messageToSend.message.trim()) {
      toast.error('Message value is required');
      return;
    }

    const [messageResponse] = await Promise.all([
      axios.post(`${apiURL}/produce`, messageToSend)
    ]);

    setProducers(messageResponse.data.producers)
    setMessageToSend({ name: '', topic: '', message: undefined, partition: undefined });
    setIsSendingMessage(false);
    toast.success(`Message sent to topic "${selectedProducerTopic}"`);
  };

  const updateProducerConfig = async() => {
    if (!editingProducer) return;

    const [producerResponse] = await Promise.all([
      axios.post(`${apiURL}/producers`, editingProducer)
    ]);

    setProducers(producerResponse.data.producers)
    setEditingProducer(null);
    setIsEditingConfig(false);
    toast.success('Producer configuration updated successfully');
  };

  const startContinuousMessaging = async() => {
    if (!continuousProducer) return;

    if (continuousProducer.isContinuousMessaging) {
      toast.error('Producer is already sending continuous messages until ' + continuousProducer.continuousMessagingEnd);
      return;
    }

    const endTime = Date.now() + (gossipRequest.durationSeconds * 1000);
    
    setProducers(prev => prev.map(p => 
      p.name === continuousProducer.name ? { 
        ...p, 
        isContinuousMessaging: true,
        continuousMessagingEnd: endTime
      } : p
    ));

    // Send cat gossip messages
    const [producerResponse] = await Promise.all([
      axios.post(`${apiURL}/producers/cat-gossip/${continuousProducer.name}`, gossipRequest)
    ]);

    setTimeout(() => {
      setProducers(prev =>
        prev.map(p =>
          p.name === continuousProducer.name ? { ...p, isContinuousMessaging: false , continuousMessagingEnd: null} : p
        )
      );
      getProducers();
    }, gossipRequest.durationSeconds * 1000);

    setIsContinuousMessaging(false);
    setContinuousProducer(null);
    setGossipRequest({
      durationSeconds: 0,
      topic: ""
    })
    toast.success(`Started continuous cat gossip messaging for ${gossipRequest.durationSeconds} seconds!`);
  };


  const totalMessagesSent = producers.reduce((sum, p) => sum + p.messagesSent, 0);

  const getProducers = async () => {
    try {
      const [producerResponse] = await Promise.all([
        axios.get(`${apiURL}/producers`)
      ]);
      setProducers(producerResponse.data)
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    getProducers();

    const interval = setInterval(async () => {
        getProducers();
    }, 3000); // 5s
    return () => clearInterval(interval); // cleanup
  }, []);

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
                            <SelectItem key={topic.name} value={topic.name}>{topic.name}</SelectItem>
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
                        <Label htmlFor="linger-ms">Linger Ms</Label>
                        <Input
                          id="linger-ms"
                          type="number"
                          value={newProducer.lingerMs}
                          onChange={(e) => setNewProducer(prev => ({ ...prev, lingerMs: parseInt(e.target.value) }))}
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label htmlFor="retries">Retries</Label>
                        <Input
                          id="retries"
                          type="number"
                          value={newProducer.retries}
                          onChange={(e) => setNewProducer(prev => ({ ...prev, retries: parseInt(e.target.value) }))}
                        />
                      </div>
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
                    </div>
                    
                    <div className="grid grid-cols-2 gap-4">
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
              <p className="text-sm font-medium">Messages Sent</p>
              <p className="text-2xl font-bold">{totalMessagesSent.toLocaleString()}</p>
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
                <TableHead>Messages Sent</TableHead>
                <TableHead>Configuration</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {producers.map((producer) => (
                <TableRow key={producer.id}>
                  <TableCell className="font-medium">{producer.name}</TableCell>
                  <TableCell>{producer.topic}</TableCell>
                  <TableCell>{producer.messagesSent.toLocaleString()}</TableCell>
                  <TableCell>
                    <div className="text-sm text-muted-foreground">
                      Batch: {producer.batchSize}, Linger.Ms: {producer.lingerMs}, Acks: {producer.acks}, Compression Type: {producer.compressionType}, Retries: {producer.retries}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex space-x-1">
                      
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

                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setMessagingProducer(producer);
                          setMessageToSend(prev => prev ? { ...prev, name: producer.name, topic: producer.topic } : prev)
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
                            <AlertDialogTitle>Delete Producer</AlertDialogTitle>
                            <AlertDialogDescription>
                              Are you sure you want to delete the producer "{producer.name}"? 
                              This action cannot be undone.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction onClick={() => deleteProducer(producer.name)}>
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
                <Label htmlFor="edit-topic">Topic</Label>
                <Input
                  id="edit-topic"
                  value={editingProducer.topic}
                  onChange={(e) => setEditingProducer(prev => prev ? { ...prev, topic: e.target.value } : null)}
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
                  <Label htmlFor="edit-linger-ms">Linger Ms</Label>
                  <Input
                    id="edit-linger-ms"
                    type="number"
                    value={editingProducer.lingerMs}
                    onChange={(e) => setEditingProducer(prev => prev ? { ...prev, lingerMs: parseInt(e.target.value) } : null)}
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="edit-retries">Retries</Label>
                  <Input
                    id="edit-retries"
                    type="number"
                    value={editingProducer.retries}
                    onChange={(e) => setEditingProducer(prev => prev ? { ...prev, retries: parseInt(e.target.value) } : null)}
                  />
                </div>
              
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
              </div>
                
              <div className="grid grid-cols-2 gap-4">
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
      {continuousProducer && (
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
                <Label htmlFor="topic">Topic</Label>
                <Select
                  value={gossipRequest.topic || ""}
                  onValueChange={(e) => setGossipRequest(prev => prev ? { ...prev, topic: e } : null)}
                >
                  <SelectTrigger id="topic">
                    <SelectValue placeholder="Select a topic" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={continuousProducer.topic}>{continuousProducer.topic}</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="duration">Duration (seconds)</Label>
                <Select
                  value={gossipRequest.durationSeconds.toString() || ""}
                  onValueChange={(e) => setGossipRequest(prev => prev ? { ...prev, durationSeconds: parseInt(e) } : null)}
                >
                  <SelectTrigger id="duration">
                    <SelectValue placeholder="Select a duration" />
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
                  {catGossipMessages.map((message, index) => (
                    <p key={index}>• {message}</p>
                  ))}
                  <p className="italic">...and more surveillance reports</p>
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
      )}

      {/* Continuous Cat Gossip Messaging Dialog */}
      {messagingProducer && (
        <Dialog open={isSendingMessage} onOpenChange={setIsSendingMessage}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Send Message</DialogTitle>
              <DialogDescription>
                Send a message to a Kafka topic
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4">
              
              <div className="space-y-2">
                <Label htmlFor="message-producer-name">Producer Name: </Label>
                <Input
                  id="message-producer-name"
                  value={messageToSend.name}
                  class="disabled:cursor-not-allowed disabled:bg-gray-200 disabled:text-gray-500 disabled:border-gray-300 disabled:opacity-50"
                  readOnly
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="message-topic">Topic: </Label>
                <Input
                  id="message-topic"
                  value={messageToSend.topic}
                  class="disabled:cursor-not-allowed disabled:bg-gray-200 disabled:text-gray-500 disabled:border-gray-300 disabled:opacity-50"
                  readOnly
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="message-value">Message</Label>
                <Textarea
                  id="message-value"
                  value={messageToSend.message}
                  onChange={(e) => setMessageToSend(prev => ({ ...prev, message: e.target.value }))}
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
      )}
    </div>
  );
}

