db_config = {
    'dbname': 'my_database',      # Replace with your database name
    'user': 'my_user',        # Replace with your database user
    'password': 'my_password',# Replace with your database password
    'host': 'localhost',
    'port': 5432
}

model_name = 'all-MiniLM-L6-v2'   # Pre-trained model name
index_file = 'vector_store/faiss_index'  # Path to save the Faiss index