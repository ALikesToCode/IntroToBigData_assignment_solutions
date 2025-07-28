"""
Google Cloud Function for Real-time File Processing
Course: Introduction to Big Data - Week 6 Assignment

This Cloud Function gets triggered when a file is uploaded to a GCS bucket.
It publishes the file information to a Pub/Sub topic for further processing.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import functions_framework
from google.cloud import pubsub_v1
import json
import logging
import os
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get environment variables
PROJECT_ID = os.environ.get('GCP_PROJECT', 'steady-triumph-447006-f8')
TOPIC_NAME = os.environ.get('PUBSUB_TOPIC', 'file-processing-topic')

logger.info(f"Function initialized - Project: {PROJECT_ID}, Topic: {TOPIC_NAME}")

@functions_framework.cloud_event
def process_file_upload(cloud_event):
    """
    Cloud Function triggered by Cloud Storage events.
    
    Args:
        cloud_event: CloudEvent object containing event data
    """
    logger.info("=== CLOUD FUNCTION TRIGGERED ===")
    
    try:
        # Log the entire event for debugging
        logger.info(f"Event type: {cloud_event.get('type', 'unknown')}")
        logger.info(f"Event data: {cloud_event.data}")
        
        # Extract event data
        event_data = cloud_event.data
        
        # Get file information from the event
        bucket_name = event_data.get('bucket', '')
        file_name = event_data.get('name', '')
        event_type = event_data.get('eventType', cloud_event.get('type', ''))
        time_created = event_data.get('timeCreated', '')
        
        logger.info(f"Processing file: gs://{bucket_name}/{file_name}")
        logger.info(f"Event type: {event_type}")
        
        # Only process file creation/upload events
        if 'finalize' not in event_type.lower() and 'create' not in event_type.lower():
            logger.info(f"Ignoring event type: {event_type}")
            return {"status": "ignored", "reason": f"Event type {event_type} not processed"}
        
        # Skip processing for directories or system files
        if file_name.endswith('/') or file_name.startswith('.'):
            logger.info(f"Skipping directory or system file: {file_name}")
            return {"status": "skipped", "reason": "Directory or system file"}
        
        # Initialize Pub/Sub publisher
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
        
        logger.info(f"Publishing to topic: {topic_path}")
        
        # COUNT LINES IN THE FILE
        from google.cloud import storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        try:
            # Download and count lines
            content = blob.download_as_text()
            lines = content.splitlines()
            line_count = len(lines)
            file_size = blob.size
            
            logger.info(f"📊 ACTUAL LINE COUNT: {line_count} lines")
            logger.info(f"📏 FILE SIZE: {file_size} bytes")
            
        except Exception as count_error:
            logger.error(f"Failed to count lines: {count_error}")
            line_count = -1
            file_size = -1

        # Create message payload with ACTUAL LINE COUNT
        message_data = {
            'bucket_name': bucket_name,
            'file_name': file_name,
            'event_type': event_type,
            'time_created': time_created,
            'file_path': f"gs://{bucket_name}/{file_name}",
            'processing_request': 'line_count',
            'function_triggered_at': time_created,
            'LINE_COUNT': line_count,
            'FILE_SIZE': file_size,
            'PROCESSING_COMPLETE': True
        }
        
        # Convert message to JSON bytes
        message_json = json.dumps(message_data)
        message_bytes = message_json.encode('utf-8')
        
        logger.info(f"Message to publish: {message_json}")
        
        # Publish message to Pub/Sub topic
        future = publisher.publish(topic_path, message_bytes)
        message_id = future.result()
        
        logger.info(f"✅ Message published successfully with ID: {message_id}")
        
        result = {
            'status': 'success',
            'message_id': message_id,
            'file_processed': f"gs://{bucket_name}/{file_name}",
            'topic': topic_path
        }
        
        logger.info(f"Function completed successfully: {result}")
        return result
        
    except Exception as e:
        error_msg = f"Error processing file upload: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error(f"Event data: {cloud_event.data if hasattr(cloud_event, 'data') else 'No data'}")
        
        # Return error but don't re-raise to avoid infinite retries
        return {
            'status': 'error',
            'error': error_msg,
            'event_data': str(cloud_event.data) if hasattr(cloud_event, 'data') else 'No data'
        }

# Test function to verify environment
def test_environment():
    """Test function to verify environment variables."""
    logger.info("=== TESTING ENVIRONMENT ===")
    logger.info(f"PROJECT_ID: {PROJECT_ID}")
    logger.info(f"TOPIC_NAME: {TOPIC_NAME}")
    
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
        logger.info(f"Topic path: {topic_path}")
        logger.info("✅ Environment test successful")
    except Exception as e:
        logger.error(f"❌ Environment test failed: {e}")

# Run environment test on import
test_environment()