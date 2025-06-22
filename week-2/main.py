import functions_framework
from google.cloud import storage
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def count_lines_in_gcs_file(bucket_name: str, blob_name: str) -> dict:
    """
    Counts the number of lines in a file stored in GCS.
    
    Args:
        bucket_name: The name of the GCS bucket.
        blob_name: The name of the blob (file) in the GCS bucket.
        
    Returns:
        Dictionary containing the result or error information.
    """
    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        logger.info(f"Attempting to download file gs://{bucket_name}/{blob_name}...")
        
        # Check if the blob exists
        if not blob.exists():
            error_msg = f"File not found: gs://{bucket_name}/{blob_name}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "line_count": None
            }
        
        # For large files, we could use streaming: blob.open('r')
        # For this example, we'll use download_as_text() for simplicity
        content = blob.download_as_text()
        
        # Count lines by splitting on newline characters
        lines = content.splitlines()
        line_count = len(lines)
        
        logger.info(f"Successfully counted {line_count} lines in gs://{bucket_name}/{blob_name}")
        
        return {
            "success": True,
            "bucket_name": bucket_name,
            "blob_name": blob_name,
            "line_count": line_count,
            "file_size_bytes": blob.size
        }
        
    except storage.exceptions.NotFound:
        error_msg = f"File not found (NotFound exception): gs://{bucket_name}/{blob_name}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "line_count": None
        }
    except storage.exceptions.Forbidden as e:
        error_msg = f"Permission denied for gs://{bucket_name}/{blob_name}. Details: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "line_count": None
        }
    except Exception as e:
        error_msg = f"Unexpected error processing gs://{bucket_name}/{blob_name}: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "line_count": None
        }

@functions_framework.http
def count_gcs_lines_http(request):
    """
    HTTP Cloud Function to count lines in a GCS file.
    
    Request should contain JSON with:
    {
        "bucket_name": "your-bucket-name",
        "blob_name": "path/to/your/file.txt"
    }
    
    Returns JSON response with line count or error information.
    """
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        # Parse the request data
        if request.content_type == 'application/json':
            request_json = request.get_json(silent=True)
        else:
            return json.dumps({
                "success": False,
                "error": "Content-Type must be application/json"
            }), 400, headers
            
        if not request_json:
            return json.dumps({
                "success": False,
                "error": "Invalid JSON in request body"
            }), 400, headers
        
        # Extract required parameters
        bucket_name = request_json.get('bucket_name')
        blob_name = request_json.get('blob_name')
        
        if not bucket_name or not blob_name:
            return json.dumps({
                "success": False,
                "error": "Both 'bucket_name' and 'blob_name' are required"
            }), 400, headers
        
        # Count lines in the GCS file
        result = count_lines_in_gcs_file(bucket_name, blob_name)
        
        # Return appropriate status code based on success
        status_code = 200 if result["success"] else 404 if "not found" in result.get("error", "").lower() else 500
        
        return json.dumps(result, indent=2), status_code, headers
        
    except Exception as e:
        logger.error(f"Error in HTTP function: {str(e)}")
        return json.dumps({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500, headers

@functions_framework.cloud_event
def count_gcs_lines_event(cloud_event):
    """
    Cloud Event function triggered by GCS object creation/finalization.
    Automatically counts lines when a file is uploaded to a specific bucket.
    
    This function is triggered by GCS events and will automatically process
    any text file uploaded to the configured bucket.
    """
    try:
        # Extract data from the cloud event
        data = cloud_event.data
        bucket_name = data["bucket"]
        blob_name = data["name"]
        
        logger.info(f"Processing GCS event for gs://{bucket_name}/{blob_name}")
        
        # Only process text files (optional filter)
        text_extensions = ['.txt', '.csv', '.log', '.md', '.py', '.js', '.html', '.xml', '.json']
        if not any(blob_name.lower().endswith(ext) for ext in text_extensions):
            logger.info(f"Skipping non-text file: {blob_name}")
            return
        
        # Count lines in the uploaded file
        result = count_lines_in_gcs_file(bucket_name, blob_name)
        
        if result["success"]:
            logger.info(f"File processed successfully: {result}")
            # Here you could store the result in a database, send to another service, etc.
        else:
            logger.error(f"Failed to process file: {result}")
            
    except Exception as e:
        logger.error(f"Error in cloud event function: {str(e)}")
        raise 