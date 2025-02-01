import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { ResponsiveContainer, LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';

export const TopicVisualization = ({ topics }) => {
  const topicData = Object.entries(topics).map(([topic, terms]) => ({
    topic,
    terms: terms.length,
    topTerms: terms.join(', ')
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle>Topic Distribution</CardTitle>
      </CardHeader>
      <CardContent className="h-96">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={topicData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="topic" />
            <YAxis />
            <Tooltip content={({ payload }) => {
              if (!payload?.length) return null;
              return (
                <div className="bg-white p-2 border rounded shadow">
                  <p className="font-bold">{payload[0].payload.topic}</p>
                  <p>Terms: {payload[0].payload.topTerms}</p>
                </div>
              );
            }} />
            <Bar dataKey="terms" fill="#8884d8" />
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};

export const SentimentVisualization = ({ sentiments }) => {
  const sentimentData = Object.entries(sentiments).map(([cluster, value]) => ({
    cluster,
    sentiment: value
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle>Cluster Sentiments</CardTitle>
      </CardHeader>
      <CardContent className="h-96">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={sentimentData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="cluster" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="sentiment" stroke="#82ca9d" />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};