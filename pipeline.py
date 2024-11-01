# pipeline.py
import logging
from data_ingestion import DataIngestionPipeline
from data_preprocessing import DataPreprocessingPipeline
from vectorization import VectorizationPipeline
from api.config import db_config, model_name, index_file

def main():
    # Configure logging
    logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    
    logging.info("Starting the data pipeline.")

    # Step 1: Data Ingestion
    try:
        logging.info("Starting data ingestion.")
        ingestion_pipeline = DataIngestionPipeline(db_config)
        # Create tables and ingest data
        ingestion_pipeline.connect_db()
        ingestion_pipeline.create_tables()
        ingestion_pipeline.close_db()
        ingestion_pipeline.ingest_data('data/') 
        logging.info("Data ingestion completed successfully.")
    except Exception as e:
        logging.error(f"Data ingestion failed: {e}")
        return

    # Step 2: Data Preprocessing
    try:
        logging.info("Starting data preprocessing.")
        preprocessing_pipeline = DataPreprocessingPipeline(db_config)
        preprocessing_pipeline.run()
        logging.info("Data preprocessing completed successfully.")
    except Exception as e:
        logging.error(f"Data preprocessing failed: {e}")
        return

    # Step 3: Vectorization
    try:
        logging.info("Starting vectorization.")
        vectorization_pipeline = VectorizationPipeline(db_config, model_name, index_file)
        vectorization_pipeline.run()
        logging.info("Vectorization completed successfully.")
    except Exception as e:
        logging.error(f"Vectorization failed: {e}")
        return

    logging.info("Data pipeline completed successfully.")

if __name__ == '__main__':
    main()
