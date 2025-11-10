import React, { useState, useEffect } from 'react';
import axios from "axios";
import { Sidebar, SidebarContent, SidebarGroup, SidebarGroupContent, SidebarGroupLabel, SidebarMenu, SidebarMenuItem, SidebarMenuButton, SidebarProvider, SidebarTrigger } from './components/ui/sidebar';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card';
import { Badge } from './components/ui/badge';
import { Server, Database, Send, MessageSquare, Settings, Activity } from 'lucide-react';
import { ClusterManagement } from './components/cluster-management';
import { TopicsManagement } from './components/topics-management';
import { ProducersManagement } from './components/producers-management';
import { ConsumersManagement } from './components/consumers-management';
import { SchemaRegistry } from './components/schema-registry';
import { Dashboard } from './components/dashboard';
import { Toaster } from './components/ui/sonner';
import { Producer, Consumer } from '@/types';

const navigation = [
  // { name: 'Dashboard', icon: Activity, id: 'dashboard' },
  { name: 'Cluster', icon: Server, id: 'cluster' },
  { name: 'Topics', icon: Database, id: 'topics' },
  { name: 'Producers', icon: Send, id: 'producers' },
  { name: 'Consumers', icon: MessageSquare, id: 'consumers' },
  // { name: 'Schema Registry', icon: Settings, id: 'schema' },
];

const codespaceName = import.meta.env.VITE_CODESPACE_NAME;
// When running in Codespaces, use a relative path so the Vite dev server can proxy requests
// to the backend (avoids GitHub tunnel auth redirects). Otherwise use configured API URL or localhost.
const apiURL = codespaceName ? '' : (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000');


export default function App() {
  const [activeSection, setActiveSection] = useState('cluster');
  const [topics, setTopics] = useState<string[]>([]);
  const [consumers, setConsumers] = useState<Consumer[]>([]);
  const [producers, setProducers] = useState<Producer[]>([]);
  

  const getTopics = async () => {
    try {
      const [topicResponse] = await Promise.all([
        axios.get(`${apiURL}/topics`)
      ]);
      setTopics(topicResponse.data)
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    const timerId = setTimeout(() => {
        getTopics();
      }, 1000); // 1-second delay before getting partition assignments
  }, []);

  const renderActiveSection = () => {
    switch (activeSection) {
      // case 'dashboard':
      //   return <Dashboard apiURL={apiURL}/>;
      case 'cluster':
        return <ClusterManagement apiURL={apiURL}/>;
      case 'topics':
        return <TopicsManagement topics={topics} setTopics={setTopics} consumers={consumers} producers={producers} apiURL={apiURL}/>;
      case 'producers':
        return <ProducersManagement topics={topics} apiURL={apiURL} producers={producers} setProducers={setProducers}/>;
      case 'consumers':
        return <ConsumersManagement topics={topics} apiURL={apiURL} consumers={consumers} setConsumers={setConsumers}/>;
      case 'schema':
        return <SchemaRegistry />;
      default:
        return <ClusterManagement apiURL={apiURL}/>;
        // return <Dashboard apiURL={apiURL}/>;
    }
  };

  return (
    <>
      <Toaster position="top-right" /> 
      <SidebarProvider>
        <div className="flex h-screen w-full">
          <Sidebar>
            <SidebarContent>
              <SidebarGroup>
                <SidebarGroupLabel>
                  <div className="flex items-center gap-2">
                    <div className="w-8 h-8 bg-primary rounded-lg flex items-center justify-center">
                      <Server className="w-4 h-4 text-primary-foreground" />
                    </div>
                    Kafka Workshop
                  </div>
                </SidebarGroupLabel>
                <SidebarGroupContent>
                  <SidebarMenu>
                    {navigation.map((item) => (
                      <SidebarMenuItem key={item.id}>
                        <SidebarMenuButton
                          onClick={() => setActiveSection(item.id)}
                          isActive={activeSection === item.id}
                        >
                          <item.icon className="w-4 h-4" />
                          <span>{item.name}</span>
                        </SidebarMenuButton>
                      </SidebarMenuItem>
                    ))}
                  </SidebarMenu>
                </SidebarGroupContent>
              </SidebarGroup>
            </SidebarContent>
          </Sidebar>
          
          <main className="flex-1 overflow-auto">
            <div className="container mx-auto p-6">
              <div className="flex items-center gap-4 mb-6">
                <SidebarTrigger />
                <div>
                  <h1 className="text-3xl font-bold">Kafka Learning Dashboard</h1>
                  <p className="text-muted-foreground">
                    Learn Apache Kafka through hands-on practice
                  </p>
                </div>
                <div className="ml-auto">
                  <Badge variant="secondary" className="bg-green-100 text-green-800">
                    Connected
                  </Badge>
                </div>
              </div>
              
              {renderActiveSection()}
            </div>
          </main>
        </div>
      </SidebarProvider>
    </>
  );
}