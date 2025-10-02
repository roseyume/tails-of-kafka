import React, { useState, useEffect } from 'react';
import axios from "axios";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card';
import { Button } from './ui/button';
import { Badge } from './ui/badge';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from './ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from './ui/alert-dialog';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { Database, Plus, Trash2, Eye, Settings } from 'lucide-react';
import { toast } from 'sonner@2.0.3';

interface Topic {
  name: string;
  partitions: number;
  replicationFactor: number;
  retentionHours: number;
}

interface Partition {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  offset: number;
}

export function TopicsManagement({topics, setTopics, apiURL}) {

  const [newTopic, setNewTopic] = useState({
    name: '',
    partitions: 3,
    replicationFactor: 2,
    retentionHours: 168,
  });

  const [isCreatingTopic, setIsCreatingTopic] = useState(false);
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);

  const createTopic = async () => {
    if (!newTopic.name.trim()) {
      toast.error('Topic name is required');
      return;
    }

    if (topics.find(t => t.name === newTopic.name)) {
      toast.error('Topic with this name already exists');
      return;
    }

    try {
      const [brokerResponse] = await Promise.all([
        axios.post(`${apiURL}/topics/create`, newTopic)
      ]);

      setTopics(prev => [...prev, newTopic]);
      setIsCreatingTopic(false);
      toast.success(`Topic "${newTopic.name}" created successfully`);
      setNewTopic({ name: '', partitions: 3, replicationFactor: 2, retentionHours: 168 });
    } catch (err) {
      console.error(err);
    }
  };

  const deleteTopic = (name: string) => {
    setTopics(prev => prev.filter(topic => topic.name !== name));
    toast.success(`Topic "${name}" deleted successfully`);
  };

  const getPartitionDetails = (topicName: string): Partition[] => {
    // Mock partition data
    const topic = topics.find(t => t.name === topicName);
    if (!topic) return [];
    
    return Array.from({ length: topic.partitions }, (_, i) => ({
      id: i,
      leader: (i % 3) + 1,
      replicas: [1, 2, 3].slice(0, topic.replicationFactor),
      isr: [1, 2, 3].slice(0, topic.replicationFactor),
      offset: Math.floor(Math.random() * 10000),
    }));
  };

  return (
    <div className="space-y-6">
      {/* Topics Table */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Topic Management</CardTitle>
              <CardDescription>View and manage your Kafka topics</CardDescription>
            </div>
            <Dialog open={isCreatingTopic} onOpenChange={setIsCreatingTopic}>
              <DialogTrigger asChild>
                <Button>
                  <Plus className="w-4 h-4 mr-2" />
                  Create Topic
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Create New Topic</DialogTitle>
                  <DialogDescription>
                    Configure a new Kafka topic with partitions and replication
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label htmlFor="topic-name">Topic Name</Label>
                    <Input
                      id="topic-name"
                      value={newTopic.name}
                      onChange={(e) => setNewTopic(prev => ({ ...prev, name: e.target.value }))}
                      placeholder="my-topic"
                    />
                  </div>
                  
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="partitions">Partitions</Label>
                      <Select
                        value={newTopic.partitions.toString()}
                        onValueChange={(value) => setNewTopic(prev => ({ ...prev, partitions: parseInt(value) }))}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="1">1</SelectItem>
                          <SelectItem value="3">3</SelectItem>
                          <SelectItem value="6">6</SelectItem>
                          <SelectItem value="12">12</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    
                    <div className="space-y-2">
                      <Label htmlFor="replication">Replication Factor</Label>
                      <Select
                        value={newTopic.replicationFactor.toString()}
                        onValueChange={(value) => setNewTopic(prev => ({ ...prev, replicationFactor: parseInt(value) }))}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="1">1</SelectItem>
                          <SelectItem value="2">2</SelectItem>
                          <SelectItem value="3">3</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  
                  <div className="space-y-2">
                    <Label htmlFor="retention">Retention (hours)</Label>
                    <Input
                      id="retention"
                      type="number"
                      value={newTopic.retentionHours}
                      onChange={(e) => setNewTopic(prev => ({ ...prev, retentionHours: parseInt(e.target.value) }))}
                    />
                  </div>
                  
                  <div className="flex justify-end space-x-2">
                    <Button variant="outline" onClick={() => setIsCreatingTopic(false)}>
                      Cancel
                    </Button>
                    <Button onClick={createTopic}>Create Topic</Button>
                  </div>
                </div>
              </DialogContent>
            </Dialog>
          </div>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Topic Name</TableHead>
                <TableHead>Partitions</TableHead>
                <TableHead>Replication</TableHead>
                <TableHead>Retention Hours</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {topics.map((topic) => (
                <TableRow key={topic.name}>
                  <TableCell className="font-medium">{topic.name}</TableCell>
                  <TableCell>{topic.partitions}</TableCell>
                  <TableCell>{topic.replicationFactor}</TableCell>
                  <TableCell>{topic.retentionHours}</TableCell>
                  <TableCell>
                    <div className="flex space-x-2">
                      <Dialog>
                        <DialogTrigger asChild>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setSelectedTopic(topic.name)}
                          >
                            <Eye className="w-4 h-4" />
                          </Button>
                        </DialogTrigger>
                        <DialogContent className="max-w-4xl">
                          <DialogHeader>
                            <DialogTitle>Topic Details: {topic.name}</DialogTitle>
                            <DialogDescription>
                              Detailed view of topic partitions and configuration
                            </DialogDescription>
                          </DialogHeader>
                          
                          <Tabs defaultValue="partitions" className="w-full">
                            <TabsList>
                              <TabsTrigger value="partitions">Partitions</TabsTrigger>
                              <TabsTrigger value="config">Configuration</TabsTrigger>
                            </TabsList>
                            
                            <TabsContent value="partitions" className="space-y-4">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>Partition</TableHead>
                                    <TableHead>Leader</TableHead>
                                    <TableHead>Replicas</TableHead>
                                    <TableHead>In-Sync Replicas</TableHead>
                                    <TableHead>Offset</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {getPartitionDetails(topic.name).map((partition) => (
                                    <TableRow key={partition.id}>
                                      <TableCell>{partition.id}</TableCell>
                                      <TableCell>Broker {partition.leader}</TableCell>
                                      <TableCell>{partition.replicas.join(', ')}</TableCell>
                                      <TableCell>{partition.isr.join(', ')}</TableCell>
                                      <TableCell>{partition.offset.toLocaleString()}</TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </TabsContent>
                            
                            <TabsContent value="config" className="space-y-4">
                              <div className="grid grid-cols-2 gap-4">
                                <div>
                                  <Label>Retention Time</Label>
                                  <p className="text-sm text-muted-foreground">{topic.retentionHours} hours</p>
                                </div>
                                <div>
                                  <Label>Cleanup Policy</Label>
                                  <p className="text-sm text-muted-foreground">delete</p>
                                </div>
                                <div>
                                  <Label>Compression Type</Label>
                                  <p className="text-sm text-muted-foreground">none</p>
                                </div>
                                <div>
                                  <Label>Min In-Sync Replicas</Label>
                                  <p className="text-sm text-muted-foreground">1</p>
                                </div>
                              </div>
                            </TabsContent>
                          </Tabs>
                        </DialogContent>
                      </Dialog>
                      
                      <AlertDialog>
                        <AlertDialogTrigger asChild>
                          <Button variant="outline" size="sm">
                            <Trash2 className="w-4 h-4" />
                          </Button>
                        </AlertDialogTrigger>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>Delete Topic</AlertDialogTitle>
                            <AlertDialogDescription>
                              Are you sure you want to delete the topic "{topic.name}"? 
                              This will permanently delete all messages and cannot be undone.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction onClick={() => deleteTopic(topic.name)}>
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
    </div>
  );
}