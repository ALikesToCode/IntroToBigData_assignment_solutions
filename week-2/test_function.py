"""
Test script for the GCS line counting Cloud Function.
This script can be used to test the function locally or make HTTP requests to the deployed function.
"""

import requests
import json
import argparse
import sys

def test_http_function(function_url: str, bucket_name: str, blob_name: str):
    """
    Test the HTTP Cloud Function with the given parameters.
    
    Args:
        function_url: The URL of the deployed Cloud Function
        bucket_name: The name of the GCS bucket
        blob_name: The name/path of the file in the bucket
    """
    payload = {
        "bucket_name": bucket_name,
        "blob_name": blob_name
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        print(f"Testing Cloud Function at: {function_url}")
        print(f"Request payload: {json.dumps(payload, indent=2)}")
        print("-" * 50)
        
        response = requests.post(function_url, json=payload, headers=headers)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Body:")
        
        try:
            response_json = response.json()
            print(json.dumps(response_json, indent=2))
            
            if response_json.get("success"):
                print(f"\n✅ SUCCESS: Found {response_json['line_count']} lines in the file!")
            else:
                print(f"\n❌ ERROR: {response_json.get('error', 'Unknown error')}")
                
        except json.JSONDecodeError:
            print(response.text)
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False
        
    return response.status_code == 200

def test_local_function(bucket_name: str, blob_name: str):
    """
    Test the function logic locally by importing and calling it directly.
    
    Args:
        bucket_name: The name of the GCS bucket
        blob_name: The name/path of the file in the bucket
    """
    try:
        # Import the function from main.py
        from main import count_lines_in_gcs_file
        
        print(f"Testing function locally...")
        print(f"Bucket: {bucket_name}")
        print(f"File: {blob_name}")
        print("-" * 50)
        
        result = count_lines_in_gcs_file(bucket_name, blob_name)
        
        print("Result:")
        print(json.dumps(result, indent=2))
        
        if result.get("success"):
            print(f"\n✅ SUCCESS: Found {result['line_count']} lines in the file!")
            return True
        else:
            print(f"\n❌ ERROR: {result.get('error', 'Unknown error')}")
            return False
            
    except ImportError as e:
        print(f"❌ Failed to import function: {e}")
        print("Make sure main.py is in the same directory and all dependencies are installed.")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test the GCS line counting Cloud Function"
    )
    
    parser.add_argument(
        "bucket_name",
        help="The name of the GCS bucket"
    )
    
    parser.add_argument(
        "blob_name", 
        help="The name/path of the file in the bucket"
    )
    
    parser.add_argument(
        "--url",
        help="URL of the deployed Cloud Function (for HTTP testing)"
    )
    
    parser.add_argument(
        "--local",
        action="store_true",
        help="Test the function locally instead of making HTTP request"
    )
    
    args = parser.parse_args()
    
    if args.local:
        success = test_local_function(args.bucket_name, args.blob_name)
    elif args.url:
        success = test_http_function(args.url, args.bucket_name, args.blob_name)
    else:
        print("❌ Error: Either specify --local for local testing or --url for HTTP testing")
        sys.exit(1)
    
    sys.exit(0 if success else 1) 