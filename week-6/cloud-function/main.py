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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Get project ID and topic name from environment variables
PROJECT_ID = os.environ.get('GCP_PROJECT', 'your-project-id')
TOPIC_NAME = os.environ.get('PUBSUB_TOPIC', 'file-processing-topic')
TOPIC_PATH = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

@functions_framework.cloud_event
def process_file_upload(cloud_event):
    """
    Cloud Function triggered by Cloud Storage events.
    
    Args:
        cloud_event: CloudEvent object containing event data
    """
    try:
        # Extract event data
        event_data = cloud_event.data
        
        # Get file information from the event
        bucket_name = event_data.get('bucket', '')
        file_name = event_data.get('name', '')
        event_type = event_data.get('eventType', '')
        time_created = event_data.get('timeCreated', '')
        
        logger.info(f"Processing event: {event_type}")
        logger.info(f"File: gs://{bucket_name}/{file_name}")
        
        # Only process file creation/upload events
        if 'finalize' not in event_type.lower():
            logger.info(f"Ignoring event type: {event_type}")
            return
        
        # Skip processing for directories or system files
        if file_name.endswith('/') or file_name.startswith('.'):
            logger.info(f"Skipping directory or system file: {file_name}")
            return
        
        # Create message payload
        message_data = {
            'bucket_name': bucket_name,
            'file_name': file_name,
            'event_type': event_type,
            'time_created': time_created,
            'file_path': f"gs://{bucket_name}/{file_name}",
            'processing_request': 'line_count'
        }
        
        # Convert message to JSON bytes
        message_json = json.dumps(message_data)
        message_bytes = message_json.encode('utf-8')
        
        # Publish message to Pub/Sub topic
        future = publisher.publish(TOPIC_PATH, message_bytes)
        message_id = future.result()
        
        logger.info(f"Message published successfully with ID: {message_id}")
        logger.info(f"Message content: {message_json}")
        
        return {
            'status': 'success',
            'message_id': message_id,
            'file_processed': f"gs://{bucket_name}/{file_name}"
        }
        
    except Exception as e:
        logger.error(f"Error processing file upload: {str(e)}")
        logger.error(f"Event data: {cloud_event.data}")
        
        # Re-raise the exception to trigger retry if needed
        raise e

def validate_environment():
    """
    Validate that required environment variables are set.
    """
    required_vars = ['GCP_PROJECT', 'PUBSUB_TOPIC']
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

# Validate environment on module load
try:
    validate_environment()
    logger.info(f"Cloud Function initialized for project: {PROJECT_ID}")
    logger.info(f"Publishing to topic: {TOPIC_PATH}")
except ValueError as e:
    logger.warning(f"Environment validation warning: {e}")
    logger.warning("Using default values - ensure proper configuration for deployment")