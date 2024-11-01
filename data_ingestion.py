# data_ingestion.py
import psycopg2
import json
import csv
import os
import logging

class DataIngestionPipeline:
    def __init__(self, db_config):
        """
        Initialize the data ingestion pipeline.
        
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
    
    def create_tables(self):
        """Create the necessary tables in the database."""
        try:
            cursor = self.conn.cursor()
            # Create customer_reviews table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_reviews (
                    review_id VARCHAR(20) PRIMARY KEY,
                    timestamp TIMESTAMP,
                    rating REAL,
                    verified_purchase BOOLEAN,
                    product_id VARCHAR(20),
                    category VARCHAR(100),
                    price REAL,
                    brand VARCHAR(100),
                    review_text TEXT,
                    age_group VARCHAR(10),
                    location VARCHAR(100),
                    purchase_history_count INTEGER,
                    review_text_cleaned TEXT,
                    normalized_rating REAL
                );
            """)
            # Create medical_records table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS medical_records (
                    patient_id VARCHAR(20) PRIMARY KEY,
                    age INTEGER,
                    gender VARCHAR(10),
                    blood_type VARCHAR(5),
                    visit_date TIMESTAMP,
                    vital_signs TEXT,
                    symptoms TEXT,
                    doctor_notes TEXT,
                    doctor_notes_cleaned TEXT,
                    symptoms_cleaned TEXT
                );
            """)
            self.conn.commit()
            cursor.close()
            logging.info("Tables created successfully.")
        except Exception as e:
            logging.error(f"Error creating tables: {e}")
            self.conn.rollback()
            raise e
    
    def ingest_customer_reviews(self, file_path):
        """Ingest customer reviews data from JSON file."""
        try:
            cursor = self.conn.cursor()
            with open(file_path, 'r') as f:
                data = json.load(f)
            # Prepare data
            records = []
            for record in data:
                review_id = record.get('review_id')
                timestamp = record.get('timestamp')
                rating = record.get('rating')
                verified_purchase = record.get('verified_purchase')
                product_id = record.get('product_id')
                structured_metadata = record.get('structured_metadata', {})
                category = structured_metadata.get('category')
                price = structured_metadata.get('price')
                brand = structured_metadata.get('brand')
                review_text = record.get('review_text')
                user_profile = record.get('user_profile', {})
                age_group = user_profile.get('age_group')
                location = user_profile.get('location')
                purchase_history_count = user_profile.get('purchase_history_count')
                
                records.append((
                    review_id,
                    timestamp,
                    rating,
                    verified_purchase,
                    product_id,
                    category,
                    price,
                    brand,
                    review_text,
                    age_group,
                    location,
                    purchase_history_count
                ))
            # Insert data
            insert_query = """
                INSERT INTO customer_reviews (
                    review_id, timestamp, rating, verified_purchase, product_id,
                    category, price, brand, review_text, age_group, location, purchase_history_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (review_id) DO NOTHING;
            """
            cursor.executemany(insert_query, records)
            self.conn.commit()
            cursor.close()
            logging.info(f"Ingested customer reviews from {file_path}.")
        except Exception as e:
            logging.error(f"Error ingesting customer reviews: {e}")
            self.conn.rollback()
            raise e
    
    def ingest_medical_records(self, file_path):
        """Ingest medical records data from CSV file."""
        try:
            cursor = self.conn.cursor()
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                records = []
                for row in reader:
                    patient_id = row.get('patient_id')
                    age = int(row.get('age')) if row.get('age') else None
                    gender = row.get('gender')
                    blood_type = row.get('blood_type')
                    visit_date = row.get('visit_date')
                    vital_signs = row.get('vital_signs')
                    symptoms = row.get('symptoms')
                    doctor_notes = row.get('doctor_notes')
                    
                    records.append((
                        patient_id,
                        age,
                        gender,
                        blood_type,
                        visit_date,
                        vital_signs,
                        symptoms,
                        doctor_notes
                    ))
            # Insert data
            insert_query = """
                INSERT INTO medical_records (
                    patient_id, age, gender, blood_type, visit_date,
                    vital_signs, symptoms, doctor_notes
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (patient_id) DO NOTHING;
            """
            cursor.executemany(insert_query, records)
            self.conn.commit()
            cursor.close()
            logging.info(f"Ingested medical records from {file_path}.")
        except Exception as e:
            logging.error(f"Error ingesting medical records: {e}")
            self.conn.rollback()
            raise e
    
    def ingest_data(self, data_dir):
        """Ingest data from the specified directory."""
        self.connect_db()
        try:
            # # Ingest customer reviews
            customer_reviews_file = os.path.join(data_dir, 'customer_reviews.json')
            self.ingest_customer_reviews(customer_reviews_file)
            # Ingest medical records
            medical_records_file = os.path.join(data_dir, 'medical_records.csv')
            self.ingest_medical_records(medical_records_file)
        finally:
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
    
    pipeline = DataIngestionPipeline(db_config)
    # Create tables
    pipeline.connect_db()
    pipeline.create_tables()
    pipeline.close_db()
    # Ingest data
    pipeline.ingest_data('data/') 
