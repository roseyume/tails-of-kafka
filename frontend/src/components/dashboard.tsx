import React from 'react';
import axios from "axios";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card';
import { Badge } from './ui/badge';
import { Progress } from './ui/progress';
import { Server, Database, Send, MessageSquare, Activity, Users } from 'lucide-react';

export function Dashboard({apiURL}) {
  const stats = [
    {
      title: 'Cluster Status',
      value: 'Healthy',
      icon: Server,
      description: '3 brokers online',
      color: 'bg-green-100 text-green-800',
    },
    {
      title: 'Topics',
      value: '5',
      icon: Database,
      description: '2 with active producers',
      color: 'bg-blue-100 text-blue-800',
    },
    {
      title: 'Active Producers',
      value: '3',
      icon: Send,
      description: 'Sending messages',
      color: 'bg-purple-100 text-purple-800',
    },
    {
      title: 'Active Consumers',
      value: '4',
      icon: MessageSquare,
      description: '2 consumer groups',
      color: 'bg-orange-100 text-orange-800',
    },
  ];

  const recentActivity = [
    { time: '10:45 AM', action: 'Producer started', topic: 'user-events', type: 'producer' },
    { time: '10:42 AM', action: 'Topic created', topic: 'order-events', type: 'topic' },
    { time: '10:40 AM', action: 'Consumer group joined', topic: 'user-events', type: 'consumer' },
    { time: '10:38 AM', action: 'Message sent', topic: 'notifications', type: 'message' },
  ];

  const getActivityIcon = (type: string) => {
    switch (type) {
      case 'producer': return Send;
      case 'consumer': return MessageSquare;
      case 'topic': return Database;
      default: return Activity;
    }
  };

  const getActivityColor = (type: string) => {
    switch (type) {
      case 'producer': return 'text-purple-600';
      case 'consumer': return 'text-orange-600';
      case 'topic': return 'text-blue-600';
      default: return 'text-gray-600';
    }
  };

  return (
    <div className="space-y-6">
      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map((stat, index) => (
          <Card key={index}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">{stat.title}</CardTitle>
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
              <Badge variant="secondary" className={stat.color}>
                {stat.description}
              </Badge>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Cluster Health */}
        <Card>
          <CardHeader>
            <CardTitle>Cluster Health</CardTitle>
            <CardDescription>Real-time cluster metrics</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>CPU Usage</span>
                <span>45%</span>
              </div>
              <Progress value={45} className="h-2" />
            </div>
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>Memory Usage</span>
                <span>62%</span>
              </div>
              <Progress value={62} className="h-2" />
            </div>
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>Disk Usage</span>
                <span>38%</span>
              </div>
              <Progress value={38} className="h-2" />
            </div>
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>Network I/O</span>
                <span>28%</span>
              </div>
              <Progress value={28} className="h-2" />
            </div>
          </CardContent>
        </Card>

        {/* Recent Activity */}
        <Card>
          <CardHeader>
            <CardTitle>Recent Activity</CardTitle>
            <CardDescription>Latest events in your Kafka cluster</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {recentActivity.map((activity, index) => {
                const Icon = getActivityIcon(activity.type);
                return (
                  <div key={index} className="flex items-center space-x-3">
                    <div className={`rounded-full p-2 ${getActivityColor(activity.type)} bg-opacity-10`}>
                      <Icon className={`h-4 w-4 ${getActivityColor(activity.type)}`} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium">{activity.action}</p>
                      <p className="text-sm text-muted-foreground">
                        Topic: {activity.topic}
                      </p>
                    </div>
                    <div className="text-sm text-muted-foreground">
                      {activity.time}
                    </div>
                  </div>
                );
              })}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Learning Resources */}
      <Card>
        <CardHeader>
          <CardTitle>Learning Resources</CardTitle>
          <CardDescription>Quick access to Kafka concepts and documentation</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="space-y-2">
              <h4 className="font-medium">Key Concepts</h4>
              <ul className="text-sm text-muted-foreground space-y-1">
                <li>• Topics and Partitions</li>
                <li>• Producers and Consumers</li>
                <li>• Consumer Groups</li>
                <li>• Offsets and Replication</li>
              </ul>
            </div>
            <div className="space-y-2">
              <h4 className="font-medium">Best Practices</h4>
              <ul className="text-sm text-muted-foreground space-y-1">
                <li>• Partition Strategy</li>
                <li>• Serialization</li>
                <li>• Error Handling</li>
                <li>• Monitoring & Alerting</li>
              </ul>
            </div>
            <div className="space-y-2">
              <h4 className="font-medium">Advanced Topics</h4>
              <ul className="text-sm text-muted-foreground space-y-1">
                <li>• Schema Evolution</li>
                <li>• Exactly-Once Semantics</li>
                <li>• Kafka Streams</li>
                <li>• Connect Framework</li>
              </ul>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}