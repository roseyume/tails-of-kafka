import React, { useState } from 'react';
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
import { Settings, Plus, Trash2, Eye, FileCode, Download, Upload } from 'lucide-react';
import { toast } from 'sonner@2.0.3';

interface Schema {
  id: number;
  subject: string;
  version: number;
  schema: string;
  schemaType: 'AVRO' | 'JSON' | 'PROTOBUF';
  compatibility: 'BACKWARD' | 'FORWARD' | 'FULL' | 'NONE';
  createdAt: string;
  isDeleted: boolean;
}

interface CompatibilityConfig {
  subject: string;
  compatibility: 'BACKWARD' | 'FORWARD' | 'FULL' | 'NONE';
}

export function SchemaRegistry() {
  const apiURL = "https://8000-roseyume-tailsofkafka-md4yrcdut1c.ws-us121.gitpod.io";

  const [schemas, setSchemas] = useState<Schema[]>([
    {
      id: 1,
      subject: 'user-events-value',
      version: 1,
      schema: `{
  "type": "record",
  "name": "UserEvent",
  "fields": [
    {"name": "userId", "type": "string"},
    {"name": "action", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}`,
      schemaType: 'AVRO',
      compatibility: 'BACKWARD',
      createdAt: '2024-01-15T10:00:00Z',
      isDeleted: false,
    },
    {
      id: 2,
      subject: 'order-events-value',
      version: 1,
      schema: `{
  "type": "record",
  "name": "OrderEvent",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": "string"}
  ]
}`,
      schemaType: 'AVRO',
      compatibility: 'FULL',
      createdAt: '2024-01-15T10:15:00Z',
      isDeleted: false,
    },
    {
      id: 3,
      subject: 'user-events-value',
      version: 2,
      schema: `{
  "type": "record",
  "name": "UserEvent",
  "fields": [
    {"name": "userId", "type": "string"},
    {"name": "action", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "metadata", "type": ["null", "string"], "default": null}
  ]
}`,
      schemaType: 'AVRO',
      compatibility: 'BACKWARD',
      createdAt: '2024-01-15T11:00:00Z',
      isDeleted: false,
    },
  ]);

  const [compatibilityConfigs, setCompatibilityConfigs] = useState<CompatibilityConfig[]>([
    { subject: 'user-events-value', compatibility: 'BACKWARD' },
    { subject: 'order-events-value', compatibility: 'FULL' },
  ]);

  const [newSchema, setNewSchema] = useState({
    subject: '',
    schema: '',
    schemaType: 'AVRO' as const,
  });

  const [isCreatingSchema, setIsCreatingSchema] = useState(false);
  const [selectedSchema, setSelectedSchema] = useState<Schema | null>(null);

  const exampleSchemas = {
    AVRO: `{
  "type": "record",
  "name": "ExampleRecord",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}`,
    JSON: `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "timestamp": {"type": "number"}
  },
  "required": ["id", "name"]
}`,
    PROTOBUF: `syntax = "proto3";

message ExampleMessage {
  string id = 1;
  string name = 2;
  int64 timestamp = 3;
}`,
  };

  const registerSchema = () => {
    if (!newSchema.subject.trim() || !newSchema.schema.trim()) {
      toast.error('Subject and schema are required');
      return;
    }

    try {
      JSON.parse(newSchema.schema);
    } catch (error) {
      if (newSchema.schemaType === 'AVRO' || newSchema.schemaType === 'JSON') {
        toast.error('Invalid JSON schema format');
        return;
      }
    }

    const existingVersions = schemas.filter(s => s.subject === newSchema.subject && !s.isDeleted);
    const nextVersion = Math.max(...existingVersions.map(s => s.version), 0) + 1;

    const schema: Schema = {
      id: Math.max(...schemas.map(s => s.id)) + 1,
      subject: newSchema.subject,
      version: nextVersion,
      schema: newSchema.schema,
      schemaType: newSchema.schemaType,
      compatibility: 'BACKWARD',
      createdAt: new Date().toISOString(),
      isDeleted: false,
    };

    setSchemas(prev => [...prev, schema]);
    setNewSchema({ subject: '', schema: '', schemaType: 'AVRO' });
    setIsCreatingSchema(false);
    toast.success(`Schema registered for subject "${schema.subject}" version ${schema.version}`);
  };

  const deleteSchema = (schema: Schema) => {
    setSchemas(prev => prev.map(s => 
      s.id === schema.id ? { ...s, isDeleted: true } : s
    ));
    toast.success(`Schema deleted for subject "${schema.subject}" version ${schema.version}`);
  };

  const updateCompatibility = (subject: string, compatibility: any) => {
    setCompatibilityConfigs(prev => {
      const existing = prev.find(c => c.subject === subject);
      if (existing) {
        return prev.map(c => c.subject === subject ? { ...c, compatibility } : c);
      } else {
        return [...prev, { subject, compatibility }];
      }
    });
    toast.success(`Compatibility updated for subject "${subject}"`);
  };

  const getCompatibilityColor = (compatibility: string) => {
    switch (compatibility) {
      case 'FULL': return 'bg-green-100 text-green-800';
      case 'BACKWARD': return 'bg-blue-100 text-blue-800';
      case 'FORWARD': return 'bg-yellow-100 text-yellow-800';
      case 'NONE': return 'bg-red-100 text-red-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getSchemaTypeColor = (type: string) => {
    switch (type) {
      case 'AVRO': return 'bg-purple-100 text-purple-800';
      case 'JSON': return 'bg-green-100 text-green-800';
      case 'PROTOBUF': return 'bg-blue-100 text-blue-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const activeSchemas = schemas.filter(s => !s.isDeleted);
  const uniqueSubjects = [...new Set(activeSchemas.map(s => s.subject))];
  const totalVersions = activeSchemas.length;

  return (
    <div className="space-y-6">
      {/* Schema Registry Overview */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Schema Registry</CardTitle>
              <CardDescription>Manage schema evolution and compatibility</CardDescription>
            </div>
            <Dialog open={isCreatingSchema} onOpenChange={setIsCreatingSchema}>
              <DialogTrigger asChild>
                <Button>
                  <Plus className="w-4 h-4 mr-2" />
                  Register Schema
                </Button>
              </DialogTrigger>
              <DialogContent className="max-w-2xl">
                <DialogHeader>
                  <DialogTitle>Register New Schema</DialogTitle>
                  <DialogDescription>
                    Register a new schema version for a subject
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="schema-subject">Subject</Label>
                      <Input
                        id="schema-subject"
                        value={newSchema.subject}
                        onChange={(e) => setNewSchema(prev => ({ ...prev, subject: e.target.value }))}
                        placeholder="my-topic-value"
                      />
                    </div>
                    
                    <div className="space-y-2">
                      <Label htmlFor="schema-type">Schema Type</Label>
                      <Select
                        value={newSchema.schemaType}
                        onValueChange={(value: any) => {
                          setNewSchema(prev => ({ 
                            ...prev, 
                            schemaType: value,
                            schema: exampleSchemas[value as keyof typeof exampleSchemas]
                          }));
                        }}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="AVRO">AVRO</SelectItem>
                          <SelectItem value="JSON">JSON Schema</SelectItem>
                          <SelectItem value="PROTOBUF">Protocol Buffers</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  
                  <div className="space-y-2">
                    <Label htmlFor="schema-content">Schema Definition</Label>
                    <Textarea
                      id="schema-content"
                      value={newSchema.schema}
                      onChange={(e) => setNewSchema(prev => ({ ...prev, schema: e.target.value }))}
                      placeholder="Enter schema definition..."
                      rows={12}
                      className="font-mono text-sm"
                    />
                  </div>
                  
                  <div className="flex justify-end space-x-2">
                    <Button variant="outline" onClick={() => setIsCreatingSchema(false)}>
                      Cancel
                    </Button>
                    <Button onClick={registerSchema}>Register Schema</Button>
                  </div>
                </div>
              </DialogContent>
            </Dialog>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="space-y-2">
              <p className="text-sm font-medium">Total Subjects</p>
              <p className="text-2xl font-bold">{uniqueSubjects.length}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Schema Versions</p>
              <p className="text-2xl font-bold">{totalVersions}</p>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Registry Status</p>
              <Badge className="bg-green-100 text-green-800">Healthy</Badge>
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Global Compatibility</p>
              <Badge className="bg-blue-100 text-blue-800">BACKWARD</Badge>
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Schemas List */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Schema Versions</CardTitle>
            <CardDescription>All registered schema versions</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Subject</TableHead>
                  <TableHead>Version</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Compatibility</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {activeSchemas.map((schema) => (
                  <TableRow key={schema.id}>
                    <TableCell className="font-medium">{schema.subject}</TableCell>
                    <TableCell>{schema.version}</TableCell>
                    <TableCell>
                      <Badge className={getSchemaTypeColor(schema.schemaType)}>
                        {schema.schemaType}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge className={getCompatibilityColor(schema.compatibility)}>
                        {schema.compatibility}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex space-x-2">
                        <Dialog>
                          <DialogTrigger asChild>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setSelectedSchema(schema)}
                            >
                              <Eye className="w-4 h-4" />
                            </Button>
                          </DialogTrigger>
                          <DialogContent className="max-w-3xl">
                            <DialogHeader>
                              <DialogTitle>
                                Schema: {schema.subject} v{schema.version}
                              </DialogTitle>
                              <DialogDescription>
                                Schema definition and details
                              </DialogDescription>
                            </DialogHeader>
                            
                            <Tabs defaultValue="schema" className="w-full">
                              <TabsList>
                                <TabsTrigger value="schema">Schema</TabsTrigger>
                                <TabsTrigger value="metadata">Metadata</TabsTrigger>
                                <TabsTrigger value="compatibility">Compatibility</TabsTrigger>
                              </TabsList>
                              
                              <TabsContent value="schema" className="space-y-4">
                                <ScrollArea className="h-64 w-full">
                                  <pre className="text-sm bg-muted p-4 rounded-lg overflow-x-auto">
                                    {schema.schema}
                                  </pre>
                                </ScrollArea>
                              </TabsContent>
                              
                              <TabsContent value="metadata" className="space-y-4">
                                <div className="grid grid-cols-2 gap-4">
                                  <div>
                                    <Label>Schema ID</Label>
                                    <p className="text-sm text-muted-foreground">{schema.id}</p>
                                  </div>
                                  <div>
                                    <Label>Schema Type</Label>
                                    <p className="text-sm text-muted-foreground">{schema.schemaType}</p>
                                  </div>
                                  <div>
                                    <Label>Created At</Label>
                                    <p className="text-sm text-muted-foreground">
                                      {new Date(schema.createdAt).toLocaleString()}
                                    </p>
                                  </div>
                                  <div>
                                    <Label>Compatibility</Label>
                                    <p className="text-sm text-muted-foreground">{schema.compatibility}</p>
                                  </div>
                                </div>
                              </TabsContent>
                              
                              <TabsContent value="compatibility" className="space-y-4">
                                <div className="space-y-2">
                                  <Label>Update Compatibility Level</Label>
                                  <Select
                                    defaultValue={schema.compatibility}
                                    onValueChange={(value) => updateCompatibility(schema.subject, value)}
                                  >
                                    <SelectTrigger>
                                      <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                      <SelectItem value="BACKWARD">BACKWARD</SelectItem>
                                      <SelectItem value="FORWARD">FORWARD</SelectItem>
                                      <SelectItem value="FULL">FULL</SelectItem>
                                      <SelectItem value="NONE">NONE</SelectItem>
                                    </SelectContent>
                                  </Select>
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
                              <AlertDialogTitle>Delete Schema Version</AlertDialogTitle>
                              <AlertDialogDescription>
                                Are you sure you want to delete schema version {schema.version} 
                                for subject "{schema.subject}"? This action cannot be undone.
                              </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Cancel</AlertDialogCancel>
                              <AlertDialogAction onClick={() => deleteSchema(schema)}>
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

        {/* Subjects Overview */}
        <Card>
          <CardHeader>
            <CardTitle>Subjects</CardTitle>
            <CardDescription>Schema subjects and their latest versions</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {uniqueSubjects.map((subject) => {
                const subjectSchemas = activeSchemas.filter(s => s.subject === subject);
                const latestVersion = Math.max(...subjectSchemas.map(s => s.version));
                const latestSchema = subjectSchemas.find(s => s.version === latestVersion);
                
                return (
                  <div key={subject} className="p-3 border rounded-lg">
                    <div className="flex items-center justify-between mb-2">
                      <h4 className="font-medium">{subject}</h4>
                      <Badge className={getSchemaTypeColor(latestSchema?.schemaType || 'AVRO')}>
                        {latestSchema?.schemaType}
                      </Badge>
                    </div>
                    <div className="flex items-center justify-between text-sm text-muted-foreground">
                      <span>Latest: v{latestVersion}</span>
                      <span>{subjectSchemas.length} versions</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Schema Registry Configuration */}
      <Card>
        <CardHeader>
          <CardTitle>Registry Configuration</CardTitle>
          <CardDescription>Schema registry settings and compatibility rules</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <h4 className="font-medium mb-3">Compatibility Levels</h4>
                <div className="space-y-2 text-sm">
                  <div><strong>BACKWARD:</strong> New schema can read old data</div>
                  <div><strong>FORWARD:</strong> Old schema can read new data</div>
                  <div><strong>FULL:</strong> Both backward and forward compatible</div>
                  <div><strong>NONE:</strong> No compatibility checking</div>
                </div>
              </div>
            </div>
            
            <div className="space-y-4">
              <div>
                <h4 className="font-medium mb-3">Schema Types</h4>
                <div className="space-y-2 text-sm">
                  <div><strong>AVRO:</strong> Binary serialization with rich data structures</div>
                  <div><strong>JSON Schema:</strong> JSON-based schema validation</div>
                  <div><strong>Protocol Buffers:</strong> Language-neutral serialization</div>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}