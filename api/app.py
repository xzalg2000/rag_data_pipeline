# api/app.py
import os 
import sys 
from pathlib import Path
# Add parent directory to path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root))

from flask import Flask, request, jsonify
from query_retrieve import QueryRetriever
from transformers import pipeline
import logging
from openai import OpenAI

app = Flask(__name__)

# Initialize QueryRetriever
retriever = QueryRetriever()

# Load model and index once at startup
retriever.load_model()
retriever.load_faiss_index()

# Configure OpenAI
client = OpenAI(
    api_key='api_key')

def generate_customer_review_response(query_text, retrieved_texts):
    """Generate response for customer reviews using OpenAI."""
    system_prompt = """You are a customer service analyst. Analyze the provided customer reviews and answer questions about them.
    Focus on identifying patterns in customer sentiment, product issues, and service quality.
    Provide specific examples from the reviews when relevant."""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Based on these customer reviews:\n{retrieved_texts}\n\nQuestion: {query_text}"}
    ]
    
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=messages,
            temperature=0.7,
            max_tokens=300
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Error generating customer review response: {e}")
        raise

def generate_medical_record_response(query_text, retrieved_texts):
    """Generate response for medical records using OpenAI."""
    system_prompt = """You are a medical data analyst. Analyze the provided medical records and answer questions about them.
    Focus on identifying patterns in symptoms, treatments, and patient conditions.
    Maintain medical terminology and professional tone. Ensure patient privacy by not revealing specific identifiers."""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Based on these medical records:\n{retrieved_texts}\n\nQuestion: {query_text}"}
    ]
    
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=messages,
            temperature=0.7,
            max_tokens=300
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Error generating medical record response: {e}")
        raise

@app.route('/query', methods=['POST'])
def query():
    data = request.get_json()
    query_text = data.get('query')
    table_type = data.get('table_type')  # 'customer_reviews' or 'medical_records'
    top_k = data.get('top_k', 5)
    
    if not query_text:
        return jsonify({'error': 'No query text provided.'}), 400
        
    if table_type and table_type not in ['customer_reviews', 'medical_records']:
        return jsonify({'error': 'Invalid table_type. Must be either "customer_reviews" or "medical_records"'}), 400
    
    try:
        # Retrieve similar documents
        retriever.connect_db()
        results = retriever.query(query_text, top_k)
        retriever.close_db()
        
        for i, res in enumerate(results):
            print(res)
        
        
        # Combine retrieved texts based on table type
        retrieved_texts = ""
        if table_type == 'customer_reviews':
            retrieved_texts = '\n'.join([
                f"Review {i+1}:\nRating: {res['data'].get('rating', 'N/A')}\n"
                f"Text: {res['data'].get('review_text', '')}"
                for i, res in enumerate(results)
            ])
            generated_answer = generate_customer_review_response(query_text, retrieved_texts)
            
        elif table_type == 'medical_records':
            retrieved_texts = '\n'.join([
                f"Record {i+1}:\nSymptoms: {res['data'].get('symptoms', 'N/A')}\n"
                f"Doctor Notes: {res['data'].get('doctor_notes', '')}"
                for i, res in enumerate(results)
            ])
            generated_answer = generate_medical_record_response(query_text, retrieved_texts)
            
        
        return jsonify({
            'results': results,
            'generated_answer': generated_answer
        })
        
    except Exception as e:
        logging.error(f"Error during query: {e}")
        return jsonify({'error': 'An error occurred during the query.'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)



