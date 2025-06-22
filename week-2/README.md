# Assignment: Line Count of a GCS File via Google Cloud Functions

## 1. Explaining the Problem Statement

The primary objective of this assignment is to count the number of lines in a text file that is stored in Google Cloud Storage (GCS) using Google Cloud Functions. Unlike the traditional approach of using a Virtual Machine (VM), this solution leverages serverless computing to provide a more scalable, cost-effective, and managed approach.

This involves:
- Creating a Cloud Function that can access and process files in GCS
- Implementing HTTP and event-driven triggers for the function
- Reading file content from GCS and counting lines
- Providing a robust, serverless solution that scales automatically
- Handling errors gracefully and providing meaningful responses
- Demonstrating both on-demand (HTTP) and automated (event-driven) processing

Key differences from the VM approach:
- **Serverless**: No infrastructure management required
- **Event-driven**: Can automatically process files as they're uploaded
- **Scalable**: Automatically scales based on demand
- **Cost-effective**: Pay only for execution time
- **Managed**: Google handles all infrastructure concerns

## 2. Explaining your approach to reach the objective

The approach involves creating two types of Cloud Functions to demonstrate different interaction patterns:

### 2.1 HTTP Cloud Function (`count_gcs_lines_http`)
- **Purpose**: On-demand line counting via HTTP requests
- **Trigger**: HTTP POST requests with JSON payload
- **Use case**: Manual or programmatic requests to count lines in specific files
- **Input**: JSON with `bucket_name` and `blob_name`
- **Output**: JSON response with line count and metadata

### 2.2 Cloud Event Function (`count_gcs_lines_event`)
- **Purpose**: Automatic line counting when files are uploaded to GCS
- **Trigger**: GCS object finalization events
- **Use case**: Real-time processing of uploaded files
- **Input**: Cloud event data from GCS
- **Output**: Logging and potential downstream processing

### 2.3 Core Implementation Strategy
1. **Develop Cloud Functions:**
   - Use the `functions-framework` for local development and testing
   - Implement robust error handling for GCS operations
   - Use the `google-cloud-storage` library for GCS interaction
   - Provide comprehensive logging for debugging and monitoring

2. **GCP Environment Setup:**
   - Enable required APIs (Cloud Functions, Cloud Storage, Cloud Build)
   - Create GCS bucket and upload test files
   - Configure proper IAM permissions for function execution
   - Deploy functions using `gcloud functions deploy`

3. **Testing and Validation:**
   - Local testing using the Functions Framework
   - HTTP testing using curl or Python requests
   - Event testing by uploading files to GCS
   - Comprehensive error scenario testing## 3. Explaining and demonstrating the configuration of cloud compute setup

### A. Prerequisites and Initial Setup

1. **Enable Required APIs:**
   ```bash
   gcloud services enable cloudfunctions.googleapis.com
   gcloud services enable cloudbuild.googleapis.com
   gcloud services enable storage.googleapis.com
   ```

2. **Create a GCS Bucket:**
   ```bash
   gsutil mb gs://your-function-test-bucket-unique-name
   ```

3. **Set up Local Development Environment:**
   ```bash
   pip install functions-framework
   pip install google-cloud-storage
   ```

### B. Cloud Function Deployment

**Deploy HTTP Function:**
```bash
gcloud functions deploy count-gcs-lines-http \
    --runtime python311 \
    --trigger-http \
    --entry-point count_gcs_lines_http \
    --source . \
    --allow-unauthenticated \
    --memory 256MB \
    --timeout 60s
```

**Deploy Event-Driven Function:**
```bash
gcloud functions deploy count-gcs-lines-event \
    --runtime python311 \
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
    --trigger-event-filters="bucket=your-function-test-bucket-unique-name" \
    --entry-point count_gcs_lines_event \
    --source . \
    --memory 256MB \
    --timeout 60s
```

### C. IAM Configuration

The Cloud Functions automatically receive a service account with the following default permissions:
- **Cloud Functions Invoker** (for HTTP functions)
- **Storage Object Viewer** (to read from GCS buckets in the same project)

For cross-project access or enhanced security, you can create custom service accounts:
```bash
# Create a custom service account
gcloud iam service-accounts create gcs-function-sa \
    --display-name="GCS Function Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:gcs-function-sa@PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"
```### D. Monitoring and Logging

Cloud Functions automatically integrate with Google Cloud's operations suite:
- **Logs**: Available in Cloud Logging
- **Metrics**: Function invocations, duration, errors
- **Alerting**: Can be configured for error rates or latency

## 4. Explaining Input files/data

### 4.1 Function Code Files
- **`main.py`**: Contains the main Cloud Function code with both HTTP and event handlers
- **`requirements.txt`**: Specifies Python dependencies (`functions-framework`, `google-cloud-storage`)
- **`test_function.py`**: Testing script for local and remote function testing

### 4.2 Test Data Files
- **`Week-2-test-file.txt`**: Sample text file for testing the line counting functionality
  - Contains 24 lines including empty lines
  - Mixed content to test various scenarios
  - Used for validation of function accuracy

### 4.3 Input Formats

**HTTP Function Input (JSON):**
```json
{
    "bucket_name": "your-bucket-name",
    "blob_name": "path/to/your/file.txt"
}
```

**Event Function Input (Automatic):**
- Triggered automatically when files are uploaded to the configured GCS bucket
- Event data includes bucket name, object name, and metadata
- Filters can be applied to process only specific file types

## 5. Explaining and demonstrating sequence of actions performed

### 5.1 Development Phase
1. **Local Development:**
   ```bash
   # Install dependencies
   pip install -r requirements.txt
   
   # Test function locally
   functions-framework --target count_gcs_lines_http --debug
   ```

2. **Local Testing:**
   ```bash
   # Test with local import
   python test_function.py your-bucket test-file.txt --local
   
   # Test HTTP endpoint locally
   curl -X POST http://localhost:8080 \
     -H "Content-Type: application/json" \
     -d '{"bucket_name": "your-bucket", "blob_name": "test-file.txt"}'
   ```### 5.2 Deployment Phase
1. **Prepare GCS Environment:**
   ```bash
   # Create bucket
   gsutil mb gs://my-function-test-bucket-2023
   
   # Upload test file
   gsutil cp Week-2-test-file.txt gs://my-function-test-bucket-2023/
   ```

2. **Deploy Functions:**
   ```bash
   # Deploy HTTP function
   gcloud functions deploy count-gcs-lines-http \
     --runtime python311 \
     --trigger-http \
     --entry-point count_gcs_lines_http \
     --source . \
     --allow-unauthenticated
   
   # Deploy event function
   gcloud functions deploy count-gcs-lines-event \
     --runtime python311 \
     --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
     --trigger-event-filters="bucket=my-function-test-bucket-2023" \
     --entry-point count_gcs_lines_event \
     --source .
   ```

### 5.3 Testing Phase
1. **HTTP Function Testing:**
   ```bash
   # Get function URL
   FUNCTION_URL=$(gcloud functions describe count-gcs-lines-http --format="value(httpsTrigger.url)")
   
   # Test with curl
   curl -X POST $FUNCTION_URL \
     -H "Content-Type: application/json" \
     -d '{"bucket_name": "my-function-test-bucket-2023", "blob_name": "Week-2-test-file.txt"}'
   
   # Test with Python script
   python test_function.py my-function-test-bucket-2023 Week-2-test-file.txt --url $FUNCTION_URL
   ```

2. **Event Function Testing:**
   ```bash
   # Upload a new file to trigger the event function
   echo "Test line 1\nTest line 2\nTest line 3" > new-test.txt
   gsutil cp new-test.txt gs://my-function-test-bucket-2023/
   
   # Check logs to see the function execution
   gcloud functions logs read count-gcs-lines-event --limit 10
   ```

### 5.4 Monitoring Phase
1. **Check Function Logs:**
   ```bash
   # View HTTP function logs
   gcloud functions logs read count-gcs-lines-http --limit 20
   
   # View event function logs
   gcloud functions logs read count-gcs-lines-event --limit 20
   ```

2. **Monitor Function Metrics:**
   - Access Cloud Console → Cloud Functions
   - View invocation counts, execution times, and error rates
   - Set up alerting for error thresholds## 6. Exhaustive explanation of scripts/code and its objective

### 6.1 Main Function Code (`main.py`)

#### Core Function: `count_lines_in_gcs_file()`
```python
def count_lines_in_gcs_file(bucket_name: str, blob_name: str) -> dict:
```
**Objective:** Encapsulates the core logic for counting lines in a GCS file.

**Key Features:**
- **GCS Client Initialization:** Uses `storage.Client()` which automatically handles authentication
- **File Existence Check:** Validates the file exists before attempting to download
- **Content Processing:** Downloads file content and splits by newlines using `splitlines()`
- **Error Handling:** Comprehensive exception handling for common GCS errors
- **Structured Response:** Returns a dictionary with success status, line count, and metadata

#### HTTP Function: `count_gcs_lines_http()`
```python
@functions_framework.http
def count_gcs_lines_http(request):
```
**Objective:** Provides an HTTP endpoint for on-demand line counting.

**Key Features:**
- **CORS Support:** Handles preflight requests and sets appropriate headers
- **Request Validation:** Validates JSON payload and required parameters
- **Content-Type Checking:** Ensures proper JSON content type
- **Error Response Formatting:** Returns appropriate HTTP status codes
- **Structured JSON Response:** Consistent response format for success and error cases

#### Event Function: `count_gcs_lines_event()`
```python
@functions_framework.cloud_event
def count_gcs_lines_event(cloud_event):
```
**Objective:** Automatically processes files when uploaded to GCS.

**Key Features:**
- **Event Data Extraction:** Parses cloud event data to get bucket and file information
- **File Type Filtering:** Optional filtering to process only text files
- **Automatic Processing:** Triggered by GCS object finalization events
- **Logging Integration:** Comprehensive logging for monitoring and debugging

### 6.2 Testing Script (`test_function.py`)

**Objective:** Provides comprehensive testing capabilities for both local and deployed functions.

**Key Features:**
- **Local Testing:** Direct function import and execution
- **HTTP Testing:** Makes requests to deployed functions
- **Flexible Configuration:** Command-line arguments for different testing scenarios
- **Response Validation:** Checks both success and error responses
- **User-Friendly Output:** Clear formatting and status indicators### 6.3 Dependencies (`requirements.txt`)

```
functions-framework==3.*
google-cloud-storage
```

**functions-framework:** 
- Provides local development server
- Handles HTTP request/response formatting
- Supports both HTTP and event-driven functions

**google-cloud-storage:**
- Official Google Cloud Storage client library
- Handles authentication and API interactions
- Provides high-level abstractions for GCS operations

## 7. Errors that you have encountered in the process and how did you overcome them (if any)

### 7.1 Authentication and Permission Errors

**Error:** `google.auth.exceptions.DefaultCredentialsError`
- **Cause:** Missing or incorrect authentication credentials
- **Solution:** 
  ```bash
  # For local development
  gcloud auth application-default login
  
  # For deployed functions, ensure proper IAM roles
  gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:PROJECT_ID@appspot.gserviceaccount.com" \
    --role="roles/storage.objectViewer"
  ```

**Error:** `403 Forbidden: Caller does not have storage.objects.get access`
- **Cause:** Insufficient permissions to read GCS objects
- **Solution:** Grant appropriate IAM roles to the function's service account

### 7.2 Function Deployment Errors

**Error:** `Build failed: requirements.txt not found`
- **Cause:** Missing or incorrectly named requirements file
- **Solution:** Ensure `requirements.txt` is in the same directory as `main.py`

**Error:** `Function entry point not found`
- **Cause:** Mismatch between deployed entry point and function name
- **Solution:** Verify the `--entry-point` parameter matches the function name in code

### 7.3 HTTP Function Errors

**Error:** `CORS policy: No 'Access-Control-Allow-Origin' header`
- **Cause:** Missing CORS headers for browser-based requests
- **Solution:** Add proper CORS headers in the function response:
  ```python
  headers = {'Access-Control-Allow-Origin': '*'}
  return response, status_code, headers
  ```

**Error:** `400 Bad Request: Invalid JSON`
- **Cause:** Malformed JSON in request body
- **Solution:** Add JSON validation and provide clear error messages### 7.4 Event Function Errors

**Error:** Event function not triggering on file upload
- **Cause:** Incorrect event filters or bucket configuration
- **Solution:** Verify event filters match the bucket name and event type:
  ```bash
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized"
  --trigger-event-filters="bucket=your-bucket-name"
  ```

### 7.5 Memory and Timeout Issues

**Error:** `Function execution took 60001 ms, finished with status: 'timeout'`
- **Cause:** Processing large files exceeds default timeout
- **Solution:** Increase timeout and memory allocation:
  ```bash
  gcloud functions deploy function-name \
    --timeout 300s \
    --memory 1024MB
  ```

**Error:** `MemoryError` for very large files
- **Cause:** Loading entire file content into memory
- **Solution:** Implement streaming for large files:
  ```python
  line_count = 0
  with blob.open("r") as f:
      for _ in f:
          line_count += 1
  ```

## 8. Working demonstration of your pipeline in GCP Environment

### Scenario Setup
- **GCS Bucket:** `my-function-demo-bucket-2023`
- **Test File:** `Week-2-test-file.txt` (24 lines)
- **HTTP Function:** `count-gcs-lines-http`
- **Event Function:** `count-gcs-lines-event`

### Step-by-Step Demonstration

#### Step 1: Environment Preparation
```bash
# Create bucket
gsutil mb gs://my-function-demo-bucket-2023

# Upload test file
gsutil cp Week-2-test-file.txt gs://my-function-demo-bucket-2023/
```

#### Step 2: Deploy Functions
```bash
# Deploy HTTP function
gcloud functions deploy count-gcs-lines-http \
  --runtime python311 \
  --trigger-http \
  --entry-point count_gcs_lines_http \
  --source . \
  --allow-unauthenticated

# Expected Output:
# Deploying function... Done.
# availableMemoryMb: 256
# httpsTrigger:
#   url: https://us-central1-PROJECT_ID.cloudfunctions.net/count-gcs-lines-http
```#### Step 3: Test HTTP Function
```bash
# Get function URL
FUNCTION_URL=$(gcloud functions describe count-gcs-lines-http --format="value(httpsTrigger.url)")

# Test with curl
curl -X POST $FUNCTION_URL \
  -H "Content-Type: application/json" \
  -d '{"bucket_name": "my-function-demo-bucket-2023", "blob_name": "Week-2-test-file.txt"}'
```

**Expected Response:**
```json
{
  "success": true,
  "bucket_name": "my-function-demo-bucket-2023",
  "blob_name": "Week-2-test-file.txt",
  "line_count": 24,
  "file_size_bytes": 1234
}
```

#### Step 4: Deploy and Test Event Function
```bash
# Deploy event function
gcloud functions deploy count-gcs-lines-event \
  --runtime python311 \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=my-function-demo-bucket-2023" \
  --entry-point count_gcs_lines_event \
  --source .

# Upload a new file to trigger the function
echo -e "Line 1\nLine 2\nLine 3" > trigger-test.txt
gsutil cp trigger-test.txt gs://my-function-demo-bucket-2023/

# Check function logs
gcloud functions logs read count-gcs-lines-event --limit 5
```

**Expected Log Output:**
```
INFO: Processing GCS event for gs://my-function-demo-bucket-2023/trigger-test.txt
INFO: Attempting to download file gs://my-function-demo-bucket-2023/trigger-test.txt...
INFO: Successfully counted 3 lines in gs://my-function-demo-bucket-2023/trigger-test.txt
INFO: File processed successfully: {'success': True, 'bucket_name': 'my-function-demo-bucket-2023', 'blob_name': 'trigger-test.txt', 'line_count': 3, 'file_size_bytes': 21}
```

#### Step 5: Python Testing Script
```bash
# Test locally
python test_function.py my-function-demo-bucket-2023 Week-2-test-file.txt --local

# Test deployed function
python test_function.py my-function-demo-bucket-2023 Week-2-test-file.txt --url $FUNCTION_URL
```

**Expected Output:**
```
Testing Cloud Function at: https://us-central1-PROJECT_ID.cloudfunctions.net/count-gcs-lines-http
Request payload: {
  "bucket_name": "my-function-demo-bucket-2023",
  "blob_name": "Week-2-test-file.txt"
}
--------------------------------------------------
Status Code: 200
Response Body:
{
  "success": true,
  "bucket_name": "my-function-demo-bucket-2023",
  "blob_name": "Week-2-test-file.txt",
  "line_count": 24,
  "file_size_bytes": 1234
}

✅ SUCCESS: Found 24 lines in the file!
```## 9. Explaining output files/data

### 9.1 HTTP Function Response Format

**Success Response:**
```json
{
  "success": true,
  "bucket_name": "my-bucket",
  "blob_name": "my-file.txt",
  "line_count": 42,
  "file_size_bytes": 1024
}
```

**Error Response:**
```json
{
  "success": false,
  "error": "File not found: gs://my-bucket/nonexistent-file.txt",
  "line_count": null
}
```

### 9.2 Function Logs

**HTTP Function Logs:**
- Request details (bucket, file path)
- Processing status and timing
- Success/error information
- Response payload

**Event Function Logs:**
- Triggered event details
- File processing results
- Error conditions and handling

### 9.3 Cloud Monitoring Metrics

**Available Metrics:**
- **Invocations:** Number of function calls
- **Duration:** Execution time per invocation
- **Memory Usage:** Peak memory consumption
- **Error Rate:** Percentage of failed executions

### 9.4 Testing Script Output

The `test_function.py` script provides detailed output including:
- Request/response details for HTTP testing
- Function execution results for local testing
- Success/error indicators with clear formatting
- Debugging information for troubleshooting## 10. Learnings from this Assignment

### 10.1 Serverless Architecture Benefits
- **No Infrastructure Management:** Complete abstraction from server maintenance
- **Automatic Scaling:** Functions scale from 0 to thousands of instances automatically
- **Cost Efficiency:** Pay only for actual execution time, not idle resources
- **Event-Driven Processing:** Natural integration with GCP services for reactive architectures

### 10.2 Google Cloud Functions Fundamentals
- **Runtime Support:** Understanding of supported Python versions and limitations
- **Trigger Types:** HTTP triggers for synchronous operations, event triggers for asynchronous processing
- **Execution Environment:** Stateless execution model and cold start considerations
- **Local Development:** Using Functions Framework for local testing and development

### 10.3 Cloud Storage Integration
- **Authentication:** Automatic service account authentication in Cloud Functions
- **Client Libraries:** Efficient use of `google-cloud-storage` for file operations
- **Event Integration:** GCS event triggers for real-time file processing
- **Permission Models:** IAM roles and scopes for secure resource access

### 10.4 Error Handling and Monitoring
- **Comprehensive Exception Handling:** Specific handling for different GCS error types
- **Structured Logging:** Using Cloud Logging for debugging and monitoring
- **HTTP Response Codes:** Proper use of status codes for different error conditions
- **Function Observability:** Leveraging Cloud Monitoring for function performance insights

### 10.5 Development Best Practices
- **Separation of Concerns:** Core logic separated from trigger-specific handling
- **Testing Strategies:** Local testing, HTTP testing, and event simulation
- **Code Organization:** Clear function structure and documentation
- **Dependency Management:** Efficient use of requirements.txt for function dependencies

### 10.6 Deployment and Operations
- **CI/CD Integration:** Understanding of gcloud deployment workflows
- **Version Management:** Function versioning and rollback capabilities
- **Configuration Management:** Environment variables and runtime configuration
- **Security Considerations:** IAM roles, service accounts, and least privilege principles

### 10.7 Performance Considerations
- **Memory Allocation:** Right-sizing memory for function requirements
- **Timeout Configuration:** Balancing timeout limits with processing needs
- **Cold Start Optimization:** Minimizing cold start impact through efficient code structure
- **Concurrency Limits:** Understanding function concurrency and scaling behavior

### 10.8 Comparison with Traditional VM Approach
- **Deployment Speed:** Functions deploy in seconds vs. minutes for VMs
- **Resource Utilization:** More efficient resource usage with automatic scaling
- **Maintenance Overhead:** Significantly reduced operational complexity
- **Integration Capabilities:** Native integration with other GCP services
- **Cost Model:** More predictable and usage-based pricing structure