import React from 'react';
import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { Alert, AlertDescription } from './ui/alert';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const TextAnalysisDashboard = () => {
  const [file, setFile] = useState(null);
  const [analysisResults, setAnalysisResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('http://127.0.0.1:800/api/analyze', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) throw new Error('Analysis failed');
      const results = await response.json();
      setAnalysisResults(results);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const renderTopicDistribution = () => {
    if (!analysisResults?.topics) return null;
    const topicData = Object.entries(analysisResults.topics).map(([topic, terms]) => ({
      topic,
      terms: terms.length,
      keywords: terms.join(', ')
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
                    <p>{payload[0].payload.topic}</p>
                    <p>Keywords: {payload[0].payload.keywords}</p>
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

  const renderSentimentAnalysis = () => {
    if (!analysisResults?.sentiments) return null;
    const sentimentData = analysisResults.sentiments.map((s, i) => ({
      index: i,
      polarity: s.polarity,
      subjectivity: s.subjectivity
    }));

    return (
      <Card>
        <CardHeader>
          <CardTitle>Sentiment Analysis</CardTitle>
        </CardHeader>
        <CardContent className="h-96">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={sentimentData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="index" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="polarity" stroke="#8884d8" name="Polarity" />
              <Line type="monotone" dataKey="subjectivity" stroke="#82ca9d" name="Subjectivity" />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    );
  };

  const renderEntityAnalysis = () => {
    if (!analysisResults?.entities) return null;
    const entityData = Object.entries(analysisResults.entities).map(([type, entities]) => ({
      type,
      count: Object.values(entities).reduce((sum, count) => sum + count, 0)
    }));

    return (
      <Card>
        <CardHeader>
          <CardTitle>Named Entities Distribution</CardTitle>
        </CardHeader>
        <CardContent className="h-96">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={entityData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="type" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" fill="#82ca9d" />
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    );
  };

  return (
    <div className="p-4 space-y-4">
      <div className="mb-6">
        <h1 className="text-2xl font-bold mb-4">Text Analysis Dashboard</h1>
        <input
          type="file"
          accept=".csv"
          onChange={handleFileUpload}
          className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
        />
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {loading && <div className="text-center">Analyzing data...</div>}

      {analysisResults && (
        <Tabs defaultValue="topics" className="space-y-4">
          <TabsList>
            <TabsTrigger value="topics">Topics</TabsTrigger>
            <TabsTrigger value="sentiment">Sentiment</TabsTrigger>
            <TabsTrigger value="entities">Entities</TabsTrigger>
          </TabsList>

          <TabsContent value="topics">
            {renderTopicDistribution()}
          </TabsContent>

          <TabsContent value="sentiment">
            {renderSentimentAnalysis()}
          </TabsContent>

          <TabsContent value="entities">
            {renderEntityAnalysis()}
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
};

export default TextAnalysisDashboard;