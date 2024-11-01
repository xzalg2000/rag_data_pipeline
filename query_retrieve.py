# query_retrieve.py
import psycopg2
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import logging
from api.config import db_config, model_name, index_file

class QueryRetriever:
    def __init__(self):
        """
        Initialize the query retriever.
        """
        self.db_config = db_config
        self.model_name = model_name
        self.index_file = index_file
        self.conn = None
        self.model = None
        self.index = None
        
        # Configure logging
        logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO,
                            format='%(asctime)s %(levelname)s %(message)s')
        
    def connect_db(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logging.info("Database connection established.")
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise e
    
    def close_db(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")
    
    def load_model(self):
        """Load the pre-trained language model."""
        try:
            self.model = SentenceTransformer(self.model_name)
            logging.info(f"Loaded model {self.model_name}.")
        except Exception as e:
            logging.error(f"Error loading model: {e}")
            raise e
    
    def load_faiss_index(self):
        """Load the Faiss index from disk."""
        try:
            self.index = faiss.read_index(self.index_file)
            logging.info(f"Loaded Faiss index from {self.index_file}.")
        except Exception as e:
            logging.error(f"Error loading Faiss index: {e}")
            raise e
    
    def query(self, query_text, top_k=5):
        """Query the vector store and retrieve similar records from the database."""
        # Generate embedding for the query text
        query_embedding = self.model.encode([query_text])
        query_embedding = np.array(query_embedding).astype('float32')
        
        # Search the Faiss index
        D, I = self.index.search(query_embedding, top_k)
        retrieved_ids = I[0]
        distances = D[0]
        
        # Map IDs back to original IDs
        retrieved_ids_str = []
        for id in retrieved_ids:
            if id >= 700000:  # Assuming IDs for customer_reviews start from 700000
                review_id = 'R' + str(id)
                retrieved_ids_str.append(review_id)
            else:
                patient_id = 'P' + str(id)
                retrieved_ids_str.append(patient_id)
        
        # Retrieve records from the database
        records = self.retrieve_records(retrieved_ids_str)
        
        # Combine distances and records
        results = []
        for idx, record in enumerate(records):
            results.append({
                'id': retrieved_ids_str[idx],
                'distance': float(distances[idx]),
                'table': record['table'],
                'data': record['data']
            })
        
        return results
    
    def retrieve_records(self, ids):
        """Retrieve records from the database based on the IDs."""
        try:
            cursor = self.conn.cursor()
            records = []
            for id in ids:
                if id.startswith('R'):
                    # Query customer_reviews
                    cursor.execute("""
                        SELECT * FROM customer_reviews
                        WHERE review_id = %s;
                    """, (id,))
                    row = cursor.fetchone()
                    if row:
                        records.append({
                            'table': 'customer_reviews',
                            'data': dict(zip([desc[0] for desc in cursor.description], row))
                        })
                elif id.startswith('P'):
                    # Query medical_records
                    cursor.execute("""
                        SELECT * FROM medical_records
                        WHERE patient_id = %s;
                    """, (id,))
                    row = cursor.fetchone()
                    if row:
                        records.append({
                            'table': 'medical_records',
                            'data': dict(zip([desc[0] for desc in cursor.description], row))
                        })
            cursor.close()
            return records
        except Exception as e:
            logging.error(f"Error retrieving records: {e}")
            raise e
    
    def run_query(self, query_text, top_k=5):
        """Run the full query process."""
        self.connect_db()
        self.load_model()
        self.load_faiss_index()
        try:
            results = self.query(query_text, top_k)
            return results
        finally:
            self.close_db()

if __name__ == '__main__':
    query_text = input("Enter your query: ")
    retriever = QueryRetriever()
    results = retriever.run_query(query_text)
    for result in results:
        print(f"ID: {result['id']}, Distance: {result['distance']}")
        print(f"Table: {result['table']}")
        print(f"Data: {result['data']}")
        print("-" * 50)
