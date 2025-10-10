import React, { useState, useEffect } from 'react';
import axios from "axios";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card';
import { Button } from './ui/button';
import { Badge } from './ui/badge';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from './ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from './ui/alert-dialog';
import { Server, Play, Square, Trash2, Plus, Settings, Eye, Copy, Check, Info  } from 'lucide-react';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from './ui/tooltip';
import { toast } from 'sonner@2.0.3';
import { CONFIG_DESCRIPTIONS } from './configurations';
import { Broker } from '@/types';

export function ClusterManagement({apiURL}) {

  const [viewingConfigBrokerId, setViewingConfigBrokerId] = useState<number | null>(null);

  const [brokerConfig, setBrokerConfig] = useState<{}>({});

  const [brokers, setBrokers] = useState<Broker[]>([]);

  const [isAddingBroker, setIsAddingBroker] = useState(false);

  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const startBroker = async (id: number) => {
    if(!updateBrokerState(id, 'restarting')){
      return;
    }

    const [brokerResponse] = await Promise.all([
      axios.post(`${apiURL}/brokers/restart/${id}`)
    ]);
    setBrokers(brokerResponse.data.broker);

    toast.success(`Broker ${id} started successfully`);
  };

  const updateBrokerState = (id: number, newStatus: string) => {
    if (brokers.find(b => b.broker_id === id && b.status === newStatus)){
      toast.success(`Broker ${id} is already ${newStatus}`);
      return false;
    } else {
      toast.success(`Broker ${id} is ${newStatus}`);
    }

    setBrokers((prevBrokers) =>
      prevBrokers.map((broker) =>
        broker.broker_id === id
          ? { ...broker, status: newStatus } // update only the matching broker
          : broker
      )
    );
    return true;
  }

  const stopBroker = async (id: number) => {

    if(!updateBrokerState(id, 'stopping')){
      return;
    }

    const [brokerResponse] = await Promise.all([
      axios.post(`${apiURL}/brokers/stop/${id}`)
    ]);
    setBrokers(brokerResponse.data.broker);
  
    toast.success(`Broker ${id} stopped successfully`);
  };

  //TODO: Need to resolve scenario: removed broker is still assigned as replicas. Force partition reassignment?
  const removeBroker = async (id: number) => {
    if(!updateBrokerState(id, 'removing')){
      return;
    }

    const [brokerResponse] = await Promise.all([
      axios.delete(`${apiURL}/brokers/delete/${id}`)
    ]);
    setBrokers(brokerResponse.data.broker);

    toast.success(`Broker ${id} removed from cluster`);
  };

  const createBroker = async () => {
    toast.success(`Creating broker. Please wait...`);
    setIsAddingBroker(false);

    const [brokerResponse] = await Promise.all([
      axios.get(`${apiURL}/brokers/create`)
    ]);
    setBrokers(brokerResponse.data.broker);
    toast.success(`Broker added to cluster`);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'running': return 'bg-green-100 text-green-800';
      case 'stopped': return 'bg-red-100 text-red-800';
      case 'stopping': case 'restarting': return 'bg-yellow-100 text-yellow-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getRoleColor = (role: string) => {
    return role === 'controller' ? 'bg-blue-100 text-blue-800' : 'bg-gray-100 text-gray-800';
  };

  const copyToClipboard = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(key);
    toast.success('Copied to clipboard');
    setTimeout(() => setCopiedKey(null), 2000);
  };

  const runningBrokers = brokers.filter(b => b.status === 'running').length;

  
  const getBrokers = async () => {
    try {
      const [brokerResponse] = await Promise.all([
        axios.get(`${apiURL}/brokers`)
      ]);
      setBrokers(brokerResponse.data)
    } catch (err) {
      console.error(err);
    }
  };

  const getBrokerConfig = async () => {
    try {
      const [brokerResponse] = await Promise.all([
        axios.get(`${apiURL}/cluster`)
      ]);
      setBrokerConfig(brokerResponse.data.brokers)
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    getBrokers();
    getBrokerConfig();
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
                    This will register a new broker in your Kafka cluster using the default configuration.
No additional setup is required. Please confirm to proceed.
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4">
                  <div className="flex justify-end space-x-2">
                    <Button variant="outline" onClick={() => setIsAddingBroker(false)}>
                      Cancel
                    </Button>
                    <Button onClick={createBroker}>Add Broker</Button>
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
              <p className="text-2xl font-bold text-green-600">{brokers.filter(b => b.status === 'running').length}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Cluster Health</p>
              <Badge className={brokers.filter(b => b.status === 'running').length > 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}>
                {brokers.filter(b => b.status === 'running').length > 0 ? 'Healthy' : 'Down'}
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
              <div key={broker.broker_id} className="flex items-center justify-between p-4 border rounded-lg">
                <div className="flex items-center space-x-4">
                  <div className="flex items-center justify-center w-10 h-10 bg-muted rounded-lg">
                    <Server className="w-5 h-5" />
                  </div>
                  <div>
                    <h4 className="font-medium">Broker {broker.broker_id}</h4>
                    <p className="text-sm text-muted-foreground">
                      {broker.hostname}:{broker.port}
                    </p>
                  </div>
                </div>
                
                <div className="flex items-center space-x-4">
                  <Badge className={getStatusColor(broker.status)}>
                    {broker.status}
                  </Badge>
                  {/* <Badge variant="outline" className={getRoleColor(broker.role)}>
                    {broker.role}
                  </Badge> */}
                  
                  <div className="flex space-x-2">
                    {broker.status === 'running' ? (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => stopBroker(broker.broker_id)}
                      >
                        <Square className="w-4 h-4 mr-1" />
                        Stop
                      </Button>
                    ) : (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => startBroker(broker.broker_id)}
                      >
                        <Play className="w-4 h-4 mr-1" />
                        Start
                      </Button>
                      
                    )}
                    <Button variant="outline" size="sm" disabled={!brokers.find(b => b.broker_id === broker.broker_id && b.status === 'running')} // disable if value is empty
                            onClick={() => setViewingConfigBrokerId(broker.broker_id)} 
                            className={`p-1 rounded hover:bg-red-100 transition-colors ${
                              !brokers.find(b => b.broker_id === broker.broker_id && b.status === 'running')? 'opacity-50 cursor-not-allowed' : ''
                            }`}>
                      <Eye className="w-4 h-4 mr-1" />
                    </Button>
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
                            Are you sure you want to remove Broker {broker.broker_id}? This action cannot be undone.
                          </AlertDialogDescription>
                        </AlertDialogHeader>
                        <AlertDialogFooter>
                          <AlertDialogCancel>Cancel</AlertDialogCancel>
                          <AlertDialogAction onClick={() => removeBroker(broker.broker_id)}>
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

      {/* Broker Configuration Dialog */}
      <Dialog open={viewingConfigBrokerId !== null} onOpenChange={() => setViewingConfigBrokerId(null)}>
        <DialogContent className="!max-w-[90vw] sm:!max-w-[90vw] max-h-[85vh] flex flex-col" style={{
          maxWidth: '80%',
          maxHeight: 'calc(var(--vh, 1vh) * 100 - 4rem)',
          margin: '0 auto',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden'
        }}>
          <DialogHeader>
            <DialogTitle>Broker {viewingConfigBrokerId} Configuration</DialogTitle>
            <DialogDescription>
              View the configuration properties for this broker. Click values to copy to clipboard. Hover over the info icon for descriptions.
            </DialogDescription>
          </DialogHeader>
          <div className="overflow-y-auto flex-1 -mx-6 px-6">
            <TooltipProvider>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[40%]">Property</TableHead>
                    <TableHead className="w-[60%]">Value</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {viewingConfigBrokerId !== null && Object.entries(brokerConfig[viewingConfigBrokerId]).map(([configName,config]) => (
                    <TableRow key={configName}>
                      <TableCell className="font-mono text-sm align-top">
                        <div className="flex items-start gap-2">
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Info className="w-4 h-4 text-muted-foreground hover:text-foreground cursor-help flex-shrink-0 mt-0.5" />
                            </TooltipTrigger>
                            <TooltipContent side="left" className="max-w-sm">
                              <p>{CONFIG_DESCRIPTIONS[configName]}</p>
                            </TooltipContent>
                          </Tooltip>
                          <span className="break-all flex-1">{configName}</span>
                        </div>
                      </TableCell>
                      <TableCell className="font-mono text-sm align-top">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button
                              onClick={() => copyToClipboard(config.value, configName)}
                              className="text-left w-full break-all hover:bg-muted/50 rounded px-2 py-1 -mx-2 -my-1 transition-colors group flex items-start gap-2"
                            >
                              <span className="flex-1 whitespace-pre-wrap text-left">
                                {config.value?.split(',').map((part, i, arr) => (
                                  <React.Fragment key={i}>
                                    {part.trim()}
                                    {i < arr.length - 1 && <><br /></>}
                                  </React.Fragment>
                                ))}
                              </span>
                            </button>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="max-w-xs break-all">Click to copy: {configName}</p>
                          </TooltipContent>
                        </Tooltip>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TooltipProvider>
          </div>
          <div className="flex justify-end mt-4 pt-4 border-t">
            <Button variant="outline" onClick={() => setViewingConfigBrokerId(null)}>
              Close
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Cluster Configuration */}
      {/* <Card>
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
      </Card> */}
    </div>
  );
}