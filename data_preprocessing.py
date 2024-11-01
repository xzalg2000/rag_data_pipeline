# data_preprocessing.py
import psycopg2
import re
import logging

class DataPreprocessingPipeline:
    def __init__(self, db_config):
        """
        Initialize the data preprocessing pipeline.
        
        Parameters:
            db_config (dict): A dictionary containing database configuration.
        """
        self.db_config = db_config
        self.conn = None

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

    def preprocess_text(self, text):
        """Clean and preprocess the input text."""
        if text:
            # Convert to lowercase
            text = text.lower()
            # Remove special characters and numbers
            text = re.sub(r'[^a-z\s]', '', text)
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        else:
            return ''

    def preprocess_reviews(self, batch_size=100):
        """Fetch, preprocess, and update customer reviews in batches."""
        try:
            cursor = self.conn.cursor()
            offset = 0
            total_processed = 0

            # Get total number of records
            cursor.execute("SELECT COUNT(*) FROM customer_reviews;")
            total_records = cursor.fetchone()[0]
            logging.info(f"Total customer reviews to process: {total_records}")

            while True:
                # Fetch a batch of records
                cursor.execute("""
                    SELECT review_id, review_text FROM customer_reviews
                    ORDER BY review_id
                    LIMIT %s OFFSET %s;
                """, (batch_size, offset))
                records = cursor.fetchall()
                # print(f"**records **{records!=None}")
                if not records:
                    break

                for review_id, review_text in records:
                    cleaned_text = self.preprocess_text(review_text)
                    # print(f"--{review_id}: {type(review_id)} , {cleaned_text}: {type(cleaned_text)}")
                    # Update the record with cleaned text
                    cursor.execute("""
                        UPDATE customer_reviews
                        SET review_text_cleaned = %s
                        WHERE review_id = %s;
                    """, (cleaned_text, review_id))

                self.conn.commit()
                total_processed += len(records)
                logging.info(f"Processed {total_processed}/{total_records} customer reviews.")
                offset += batch_size

            cursor.close()
        except Exception as e:
            logging.error(f"Error preprocessing customer reviews: {e}")
            self.conn.rollback()
            raise e

    def preprocess_medical_records(self, batch_size=100):
        """Fetch, preprocess, and update medical records in batches."""
        try:
            cursor = self.conn.cursor()
            offset = 0
            total_processed = 0

            # Get total number of records
            cursor.execute("SELECT COUNT(*) FROM medical_records;")
            total_records = cursor.fetchone()[0]
            logging.info(f"Total medical records to process: {total_records}")

            while True:
                # Fetch a batch of records
                cursor.execute("""
                    SELECT patient_id, symptoms, doctor_notes FROM medical_records
                    ORDER BY patient_id
                    LIMIT %s OFFSET %s;
                """, (batch_size, offset))
                records = cursor.fetchall()
                if not records:
                    break

                for patient_id, symptoms, doctor_notes in records:
                    cleaned_symptoms = self.preprocess_text(symptoms)
                    cleaned_doctor_notes = self.preprocess_text(doctor_notes)
                    # Update the record with cleaned text
                    cursor.execute("""
                        UPDATE medical_records
                        SET symptoms_cleaned = %s,
                            doctor_notes_cleaned = %s
                        WHERE patient_id = %s;
                    """, (cleaned_symptoms, cleaned_doctor_notes, patient_id))

                self.conn.commit()
                total_processed += len(records)
                logging.info(f"Processed {total_processed}/{total_records} medical records.")
                offset += batch_size

            cursor.close()
        except Exception as e:
            logging.error(f"Error preprocessing medical records: {e}")
            self.conn.rollback()
            raise e

    def run(self):
        """Run the preprocessing pipeline."""
        self.connect_db()
        self.preprocess_reviews()
        self.preprocess_medical_records()
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

    pipeline = DataPreprocessingPipeline(db_config)
    pipeline.run()
