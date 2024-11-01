# vectorization.py
import psycopg2
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
import logging

class VectorizationPipeline:
    def __init__(self, db_config, model_name='all-MiniLM-L6-v2', index_file='vector_store/faiss_index'):
        """
        Initialize the vectorization pipeline.
        
        Parameters:
            db_config (dict): A dictionary containing database configuration.
            model_name (str): The name of the pre-trained model to use.
            index_file (str): Path to save the Faiss index.
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

    def create_faiss_index(self, dimension):
        """Create a Faiss index."""
        # self.index = faiss.IndexFlatL2(dimension)

        base_index = faiss.IndexFlatL2(dimension)
        self.index = faiss.IndexIDMap(base_index)    

        logging.info("Created a new Faiss index.")

    def vectorize_reviews(self, batch_size=100):
        """Generate embeddings for customer reviews and add them to the Faiss index."""
        try:
            cursor = self.conn.cursor()
            offset = 0
            total_processed = 0

            # Get total number of records
            cursor.execute("SELECT COUNT(*) FROM customer_reviews WHERE review_text_cleaned IS NOT NULL;")
            total_records = cursor.fetchone()[0]
            logging.info(f"Total customer reviews to vectorize: {total_records}")

            while True:
                # Fetch a batch of records
                cursor.execute("""
                    SELECT review_id, review_text_cleaned FROM customer_reviews
                    WHERE review_text_cleaned IS NOT NULL
                    ORDER BY review_id
                    LIMIT %s OFFSET %s;
                """, (batch_size, offset))
                records = cursor.fetchall()
                if not records:
                    break

                ids = []
                texts = []
                for review_id, review_text in records:
                    ids.append(int(review_id[1:]))  # Convert review_id like 'R763609' to integer
                    texts.append(review_text)

                # Generate embeddings
                embeddings = self.model.encode(texts, batch_size=64, show_progress_bar=True)
                embeddings = np.array(embeddings).astype('float32')

                # Add embeddings and IDs to the index
                ids_array = np.array(ids)
                self.index.add_with_ids(embeddings, ids_array)

                total_processed += len(records)
                logging.info(f"Vectorized {total_processed}/{total_records} customer reviews.")
                offset += batch_size

            cursor.close()
        except Exception as e:
            logging.error(f"Error vectorizing customer reviews: {e}")
            raise e

    def vectorize_medical_records(self, batch_size=100):
        """Generate embeddings for medical records and add them to the Faiss index."""
        try:
            cursor = self.conn.cursor()
            offset = 0
            total_processed = 0

            # Get total number of records
            cursor.execute("SELECT COUNT(*) FROM medical_records WHERE symptoms_cleaned IS NOT NULL OR doctor_notes_cleaned IS NOT NULL;")
            total_records = cursor.fetchone()[0]
            logging.info(f"Total medical records to vectorize: {total_records}")

            while True:
                # Fetch a batch of records
                cursor.execute("""
                    SELECT patient_id, symptoms_cleaned, doctor_notes_cleaned FROM medical_records
                    WHERE symptoms_cleaned IS NOT NULL OR doctor_notes_cleaned IS NOT NULL
                    ORDER BY patient_id
                    LIMIT %s OFFSET %s;
                """, (batch_size, offset))
                records = cursor.fetchall()
                if not records:
                    break

                ids = []
                texts = []
                for patient_id, symptoms_text, doctor_notes_text in records:
                    ids.append(int(patient_id[1:]))  # Convert patient_id like 'P751456' to integer
                    combined_text = ' '.join(filter(None, [symptoms_text, doctor_notes_text]))
                    texts.append(combined_text.strip())

                # Generate embeddings
                embeddings = self.model.encode(texts, batch_size=64, show_progress_bar=True)
                embeddings = np.array(embeddings).astype('float32')

                # Add embeddings and IDs to the index
                ids_array = np.array(ids)
                self.index.add_with_ids(embeddings, ids_array)

                total_processed += len(records)
                logging.info(f"Vectorized {total_processed}/{total_records} medical records.")
                offset += batch_size

            cursor.close()
        except Exception as e:
            logging.error(f"Error vectorizing medical records: {e}")
            raise e

    def save_faiss_index(self):
        """Save the Faiss index to disk."""
        try:
            faiss.write_index(self.index, self.index_file)
            logging.info(f"Faiss index saved to {self.index_file}.")
        except Exception as e:
            logging.error(f"Error saving Faiss index: {e}")
            raise e

    def run(self):
        """Run the vectorization pipeline."""
        self.connect_db()
        self.load_model()
        # Assuming embeddings are 384-dimensional for 'all-MiniLM-L6-v2' 
        self.create_faiss_index(dimension=384)
        self.vectorize_reviews()
        self.vectorize_medical_records()
        self.save_faiss_index()
        self.close_db()

if __name__ == '__main__':
    # Database configuration
    db_config = {
        'dbname': 'my_database',    
        'user': 'my_user',      
        'password': 'my_password',
        'host': 'localhost',
        'port': 5432
    }

    pipeline = VectorizationPipeline(db_config)
    pipeline.run()
