import spacy
import pandas as pd
import networkx as nx
from collections import defaultdict

class TextAnalyzer:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.entity_counts = defaultdict(int)
        self.relationship = defaultdict(list)
        
    def extract_entities(self, text):
        doc = self.nlp(text)
        entities = {ent.text: ent.label_ for ent in doc.ents}
        
        # Update counts
        for label in entities.values():
            self.entity_counts[label] += 1
            
        return entities
    
    def detect_relationships(self, entities, text):
        doc = self.nlp(text)
        
        # Simple relationship extraction based on dependency parsing
        for token in doc:
            if token.dep_ in ('nsubj', 'dobj'):
                subject = token.text
                verb = token.head.text
                for child in token.head.children:
                    if child.dep_ == 'dobj':
                        object = child.text
                        self.relationships[verb].append((subject, object))
                        
    def validate_data(self, df):
        quality_metrics = {
            'missing_values': df.isnull().sum().to_dict(),
            'unique_values': {col: df[col].nunique() for col in df.columns},
            'data_types': df.dtypes.to_dict()
        }
        return quality_metrics
    
    def create_network_graph(self):
        G = nx.Graph()
        
        # Add nodes and edges from relationships
        for verb, pairs in self.relationships.items():
            for subj, obj in pairs:
                G.add_edge(subj, obj, relationship=verb)
                
        return G

# Usage
def process_files(news_file, leaks_file):
    analyzer = TextAnalyzer()
    
    # Read files
    news_df = pd.read_excel(news_file)
    leaks_df = pd.read_excel(leaks_file)
    
    # Validate data
    news_quality = analyzer.validate_data(news_df)
    leaks_quality = analyzer.validate_data(leaks_df)
    
    # Process text columns
    for text in news_df['text'].fillna(''):  # Adjust column name as needed
        entities = analyzer.extract_entities(text)
        analyzer.detect_relationships(entities, text)
    
    # Create network graph
    graph = analyzer.create_network_graph()
    
    return {
        'entity_counts': dict(analyzer.entity_counts),
        'relationships': dict(analyzer.relationships),
        'data_quality': {
            'news': news_quality,
            'leaks': leaks_quality
        },
        'graph': graph
    }