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
import { Server, Play, Square, Trash2, Plus, Settings } from 'lucide-react';
import { toast } from 'sonner@2.0.3';

interface Broker {
  broker_id: number;
  hostname: string;
  port: number;
  role: 'controller' | 'follower';
  status: 'running' | 'stopped' | 'error';
  rack?: string; // optional, equivalent to `str | None` in Python
  num_partitions_as_leader: number;
  num_partitions_as_follower: number;
}

export function ClusterManagement() {
const apiURL = "https://8000-roseyume-tailsofkafka-md4yrcdut1c.ws-us121.gitpod.io";

  const [brokers, setBrokers] = useState<Broker[]>([
    { id: 1, host: 'localhost', port: 9092, status: 'running', role: 'controller' },
    { id: 2, host: 'localhost', port: 9093, status: 'running', role: 'follower' },
    { id: 3, host: 'localhost', port: 9094, status: 'stopped', role: 'follower' },
  ]);

  const [newBroker, setNewBroker] = useState({
    host: 'localhost',
    port: 9095,
  });

  const [isAddingBroker, setIsAddingBroker] = useState(false);

  const startBroker = (id: number) => {
    setBrokers(prev => prev.map(broker => 
      broker.id === id ? { ...broker, status: 'running' as const } : broker
    ));
    toast.success(`Broker ${id} started successfully`);
  };

  const stopBroker = (id: number) => {
    setBrokers(prev => prev.map(broker => 
      broker.id === id ? { ...broker, status: 'stopped' as const } : broker
    ));
    toast.success(`Broker ${id} stopped successfully`);
  };

  const removeBroker = (id: number) => {
    setBrokers(prev => prev.filter(broker => broker.id !== id));
    toast.success(`Broker ${id} removed from cluster`);
  };

  const addBroker = () => {
    const id = Math.max(...brokers.map(b => b.id)) + 1;
    const broker: Broker = {
      id,
      host: newBroker.host,
      port: newBroker.port,
      status: 'stopped',
      role: 'follower',
    };
    setBrokers(prev => [...prev, broker]);
    setNewBroker({ host: 'localhost', port: newBroker.port + 1 });
    setIsAddingBroker(false);
    toast.success(`Broker ${id} added to cluster`);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'running': return 'bg-green-100 text-green-800';
      case 'stopped': return 'bg-red-100 text-red-800';
      case 'error': return 'bg-yellow-100 text-yellow-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getRoleColor = (role: string) => {
    return role === 'controller' ? 'bg-blue-100 text-blue-800' : 'bg-gray-100 text-gray-800';
  };

  const runningBrokers = brokers.filter(b => b.status === 'running').length;

  const fetchMetrics = async () => {
    try {
      const [brokerResponse] = await Promise.all([
        axios.get(`${apiURL}/brokers`)
      ]);
      setBrokers(brokerResponse)
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    fetchMetrics();
  }, []);

  return (
    <div className="space-y-6">
      {/* Cluster Overview */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Cluster Overview</CardTitle>
              <CardDescription>Manage your Kafka cluster brokers</CardDescription>
            </div>
            <Dialog open={isAddingBroker} onOpenChange={setIsAddingBroker}>
              <DialogTrigger asChild>
                <Button>
                  <Plus className="w-4 h-4 mr-2" />
                  Add Broker
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Add New Broker</DialogTitle>
                  <DialogDescription>
                    Configure a new broker to add to the cluster
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label htmlFor="host">Host</Label>
                    <Input
                      id="host"
                      value={newBroker.host}
                      onChange={(e) => setNewBroker(prev => ({ ...prev, host: e.target.value }))}
                      placeholder="localhost"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="port">Port</Label>
                    <Input
                      id="port"
                      type="number"
                      value={newBroker.port}
                      onChange={(e) => setNewBroker(prev => ({ ...prev, port: parseInt(e.target.value) }))}
                      placeholder="9095"
                    />
                  </div>
                  <div className="flex justify-end space-x-2">
                    <Button variant="outline" onClick={() => setIsAddingBroker(false)}>
                      Cancel
                    </Button>
                    <Button onClick={addBroker}>Add Broker</Button>
                  </div>
                </div>
              </DialogContent>
            </Dialog>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            <div className="space-y-2">
              <p className="text-sm font-medium">Total Brokers</p>
              <p className="text-2xl font-bold">{brokers.length}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Running Brokers</p>
              <p className="text-2xl font-bold text-green-600">{runningBrokers}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Cluster Health</p>
              <Badge className={runningBrokers > 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}>
                {runningBrokers > 0 ? 'Healthy' : 'Down'}
              </Badge>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Brokers List */}
      <Card>
        <CardHeader>
          <CardTitle>Broker Management</CardTitle>
          <CardDescription>Control individual brokers in your cluster</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {brokers.map((broker) => (
              <div key={broker.id} className="flex items-center justify-between p-4 border rounded-lg">
                <div className="flex items-center space-x-4">
                  <div className="flex items-center justify-center w-10 h-10 bg-muted rounded-lg">
                    <Server className="w-5 h-5" />
                  </div>
                  <div>
                    <h4 className="font-medium">Broker {broker.id}</h4>
                    <p className="text-sm text-muted-foreground">
                      {broker.host}:{broker.port}
                    </p>
                  </div>
                </div>
                
                <div className="flex items-center space-x-4">
                  <Badge className={getStatusColor(broker.status)}>
                    {broker.status}
                  </Badge>
                  <Badge variant="outline" className={getRoleColor(broker.role)}>
                    {broker.role}
                  </Badge>
                  
                  <div className="flex space-x-2">
                    {broker.status === 'running' ? (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => stopBroker(broker.id)}
                      >
                        <Square className="w-4 h-4 mr-1" />
                        Stop
                      </Button>
                    ) : (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => startBroker(broker.id)}
                      >
                        <Play className="w-4 h-4 mr-1" />
                        Start
                      </Button>
                    )}
                    
                    <AlertDialog>
                      <AlertDialogTrigger asChild>
                        <Button variant="outline" size="sm">
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </AlertDialogTrigger>
                      <AlertDialogContent>
                        <AlertDialogHeader>
                          <AlertDialogTitle>Remove Broker</AlertDialogTitle>
                          <AlertDialogDescription>
                            Are you sure you want to remove Broker {broker.id}? This action cannot be undone.
                          </AlertDialogDescription>
                        </AlertDialogHeader>
                        <AlertDialogFooter>
                          <AlertDialogCancel>Cancel</AlertDialogCancel>
                          <AlertDialogAction onClick={() => removeBroker(broker.id)}>
                            Remove
                          </AlertDialogAction>
                        </AlertDialogFooter>
                      </AlertDialogContent>
                    </AlertDialog>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Cluster Configuration */}
      <Card>
        <CardHeader>
          <CardTitle>Cluster Configuration</CardTitle>
          <CardDescription>Global cluster settings and properties</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="replication">Default Replication Factor</Label>
                <Select defaultValue="3">
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
              
              <div className="space-y-2">
                <Label htmlFor="min-insync">Min In-Sync Replicas</Label>
                <Select defaultValue="2">
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
            
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="log-retention">Log Retention (hours)</Label>
                <Input id="log-retention" type="number" defaultValue="168" />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="segment-size">Segment Size (MB)</Label>
                <Input id="segment-size" type="number" defaultValue="1024" />
              </div>
            </div>
          </div>
          
          <div className="flex justify-end mt-6">
            <Button>
              <Settings className="w-4 h-4 mr-2" />
              Update Configuration
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}