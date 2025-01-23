import spacy
import pandas as pd
import networkx as nx
from datetime import datetime

class SecurityAnalyzer:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.threat_keywords = {
            'cyber': 'CYBER_THREAT',
            'hack': 'CYBER_THREAT',
            'extremist': 'EXTREMISM',
            'radical': 'EXTREMISM',
            'spy': 'ESPIONAGE',
            'surveillance': 'ESPIONAGE'
        }
        
    def identify_threats(self, text):
        doc = self.nlp(text.lower())
        threats = []
        
        for token in doc:
            if token.text in self.threat_keywords:
                threats.append(self.threat_keywords[token.text])
                
        return threats
    
    def extract_security_entities(self, text):
        doc = self.nlp(text)
        entities = {
            'PERSON': [],
            'ORG': [],
            'LOC': [],
            'DATE': []
        }
        
        for ent in doc.ents:
            if ent.label_ in entities:
                entities[ent.label_].append(ent.text)
                
        return entities
    
    def detect_suspicious_patterns(self, entities, text):
        patterns = []
        doc = self.nlp(text)
        
        # Check for communication patterns
        if any(word.text.lower() in ['contact', 'meet', 'communicate'] for word in doc):
            patterns.append('COMMUNICATION')
            
        # Check for financial patterns    
        if any(word.text.lower() in ['transfer', 'payment', 'fund'] for word in doc):
            patterns.append('FINANCIAL')
            
        return patterns
    
    def create_threat_network(self, texts):
        G = nx.Graph()
        
        for text in texts:
            entities = self.extract_security_entities(text)
            threats = self.identify_threats(text)
            
            # Add nodes for entities
            for entity_type, entity_list in entities.items():
                for entity in entity_list:
                    G.add_node(entity, type=entity_type)
                    
            # Add edges for threats
            for threat in threats:
                for person in entities['PERSON']:
                    G.add_edge(person, threat, type='ASSOCIATED')
                    
        return G

def process_security_data(news_file, leaks_file):
    analyzer = SecurityAnalyzer()
    
    # Read files
    news_df = pd.read_excel(news_file)
    leaks_df = pd.read_excel(leaks_file)
    
    results = {
        'threats': defaultdict(int),
        'entities': defaultdict(set),
        'patterns': defaultdict(int),
        'timeline': defaultdict(int)
    }
    
    # Process texts
    for df in [news_df, leaks_df]:
        for text in df['text'].fillna(''):
            threats = analyzer.identify_threats(text)
            entities = analyzer.extract_security_entities(text)
            patterns = analyzer.detect_suspicious_patterns(entities, text)
            
            # Update results
            for threat in threats:
                results['threats'][threat] += 1
            
            for entity_type, entity_list in entities.items():
                results['entities'][entity_type].update(entity_list)
                
            for pattern in patterns:
                results['patterns'][pattern] += 1
    
    return results