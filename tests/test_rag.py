# test_api.py
import requests
import json
from pprint import pprint

def test_query(query_text, table_type=None, top_k=3):
    """
    Test the query API endpoint
    
    Parameters:
        query_text (str): The text to search for
        table_type (str): Either 'customer_reviews' or 'medical_records' or None for both
        top_k (int): Number of results to return
    """
    url = "http://localhost:5001/query"
    
    payload = {
        "query": query_text,
        "top_k": top_k
    }
    
    if table_type:
        payload["table_type"] = table_type
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        print(f"\nSending query: {query_text}")
        print(f"Table Type: {table_type if table_type else 'Mixed'}")
        print("=" * 50)
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        # Print the generated answer
        if 'generated_answer' in data:
            print("\nGenerated Answer:")
            print("-" * 40)
            print(data['generated_answer'])
            print("-" * 40)
        
        # Print retrieved results
        print("\nRetrieved Results:")
        for i, result in enumerate(data['results'], 1):
            print(f"\nResult {i}:")
            print(f"ID: {result['id']}")
            print(f"Distance: {result['distance']:.4f}")
            print(f"Table: {result['table']}")
            pprint(result['data'])
            print("-" * 40)
            
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing response: {e}")
    except KeyError as e:
        print(f"Unexpected response format: {e}")

if __name__ == "__main__":
    # Test 1: Basic query with customer reviews
    test_query(
        query_text="What are common complaints about shipping?",
        table_type="customer_reviews",
        top_k=3
    )
    
    # Test 2: Basic query with medical records
    test_query(
        query_text="Find patients with fever symptoms",
        table_type="medical_records",
        top_k=4
    )