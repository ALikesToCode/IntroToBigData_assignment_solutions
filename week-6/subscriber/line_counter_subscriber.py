#!/usr/bin/env python3
"""
Pub/Sub Subscriber for Real-time Line Counting
Course: Introduction to Big Data - Week 6 Assignment

This program subscribes to a Pub/Sub topic, receives file notifications,
downloads files from GCS, counts lines, and prints the results.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import json
import logging
import os
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from google.cloud import pubsub_v1
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('line_counter_subscriber.log')
    ]
)
logger = logging.getLogger(__name__)

class LineCounterSubscriber:
    """
    Pub/Sub subscriber that processes file upload notifications and counts lines.
    """
    
    def __init__(self, project_id, subscription_name, max_workers=5):
        """
        Initialize the subscriber.
        
        Args:
            project_id: Google Cloud project ID
            subscription_name: Pub/Sub subscription name
            max_workers: Maximum number of concurrent message processors
        """
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.max_workers = max_workers
        
        # Initialize clients
        try:
            self.subscriber_client = pubsub_v1.SubscriberClient()
            self.storage_client = storage.Client()
            self.subscription_path = self.subscriber_client.subscription_path(
                project_id, subscription_name
            )
            
            logger.info(f"Initialized subscriber for project: {project_id}")
            logger.info(f"Subscription path: {self.subscription_path}")
            
        except DefaultCredentialsError as e:
            logger.error(f"Authentication error: {e}")
            logger.error("Please run 'gcloud auth application-default login'")
            raise
        except Exception as e:
            logger.error(f"Error initializing subscriber: {e}")
            raise
    
    def count_lines_in_file(self, bucket_name, file_name):
        """
        Download file from GCS and count lines.
        
        Args:
            bucket_name: GCS bucket name
            file_name: File name in the bucket
            
        Returns:
            int: Number of lines in the file, or None if error
        """
        try:
            logger.info(f"Processing file: gs://{bucket_name}/{file_name}")
            
            # Get bucket and blob references
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            
            # Check if file exists
            if not blob.exists():
                logger.error(f"File not found: gs://{bucket_name}/{file_name}")
                return None
            
            # Download file content as text
            logger.info(f"Downloading file: {file_name}")
            start_time = time.time()
            
            try:
                content = blob.download_as_text(encoding='utf-8')
            except UnicodeDecodeError:
                # Try with different encoding if UTF-8 fails
                logger.warning(f"UTF-8 decode failed, trying latin-1 encoding")
                content = blob.download_as_text(encoding='latin-1')
            
            download_time = time.time() - start_time
            
            # Count lines
            lines = content.splitlines()
            line_count = len(lines)
            
            # Get file size for additional metrics
            file_size = blob.size
            
            logger.info(f"✅ LINE COUNT RESULT:")
            logger.info(f"   File: gs://{bucket_name}/{file_name}")
            logger.info(f"   Lines: {line_count:,}")
            logger.info(f"   Size: {file_size:,} bytes")
            logger.info(f"   Download time: {download_time:.2f} seconds")
            
            # Print result to stdout for real-time visibility
            print(f"\n{'='*60}")
            print(f"📄 FILE PROCESSED: {file_name}")
            print(f"📊 LINE COUNT: {line_count:,}")
            print(f"📏 FILE SIZE: {file_size:,} bytes")
            print(f"⏱️  PROCESSING TIME: {download_time:.2f} seconds")
            print(f"{'='*60}\n")
            
            return line_count
            
        except Exception as e:
            logger.error(f"Error counting lines in file gs://{bucket_name}/{file_name}: {e}")
            return None
    
    def process_message(self, message):
        """
        Process a single Pub/Sub message.
        
        Args:
            message: Pub/Sub message object
        """
        try:
            # Parse message data
            message_data = json.loads(message.data.decode('utf-8'))
            
            logger.info(f"Received message: {message.message_id}")
            logger.debug(f"Message data: {message_data}")
            
            # Extract file information
            bucket_name = message_data.get('bucket_name')
            file_name = message_data.get('file_name')
            event_type = message_data.get('event_type', 'unknown')
            processing_request = message_data.get('processing_request', 'unknown')
            
            if not bucket_name or not file_name:
                logger.error(f"Invalid message data - missing bucket or file name: {message_data}")
                message.ack()
                return
            
            # Verify this is a line counting request
            if processing_request != 'line_count':
                logger.info(f"Skipping message - not a line count request: {processing_request}")
                message.ack()
                return
            
            logger.info(f"Processing line count request for: gs://{bucket_name}/{file_name}")
            
            # Count lines in the file
            line_count = self.count_lines_in_file(bucket_name, file_name)
            
            if line_count is not None:
                logger.info(f"Successfully processed file with {line_count} lines")
            else:
                logger.error(f"Failed to process file: gs://{bucket_name}/{file_name}")
            
            # Acknowledge the message
            message.ack()
            logger.info(f"Message {message.message_id} acknowledged")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing message JSON: {e}")
            logger.error(f"Raw message data: {message.data}")
            message.ack()  # Acknowledge to prevent reprocessing
            
        except Exception as e:
            logger.error(f"Error processing message {message.message_id}: {e}")
            logger.error(f"Message will be retried")
            # Don't acknowledge - let message be retried
    
    def start_listening(self, timeout=None):
        """
        Start listening for messages.
        
        Args:
            timeout: Timeout in seconds, None for indefinite listening
        """
        logger.info(f"Starting to listen for messages...")
        logger.info(f"Subscription: {self.subscription_path}")
        logger.info(f"Max workers: {self.max_workers}")
        
        if timeout:
            logger.info(f"Timeout: {timeout} seconds")
        else:
            logger.info("Listening indefinitely (Ctrl+C to stop)")
        
        # Configure flow control settings
        flow_control = pubsub_v1.types.FlowControl(max_messages=100)
        
        # Start pulling messages
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                streaming_pull_future = self.subscriber_client.subscribe(
                    self.subscription_path,
                    callback=self.process_message,
                    flow_control=flow_control
                )
                
                logger.info("🚀 Subscriber started - waiting for messages...")
                
                try:
                    # Block and listen for messages
                    streaming_pull_future.result(timeout=timeout)
                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt - shutting down...")
                    streaming_pull_future.cancel()
                    streaming_pull_future.result()  # Block until shutdown is complete
                except Exception as e:
                    logger.error(f"Error during message processing: {e}")
                    streaming_pull_future.cancel()
                    raise
                    
        except Exception as e:
            logger.error(f"Error starting subscriber: {e}")
            raise
        
        logger.info("Subscriber stopped")

def main():
    """
    Main function to run the subscriber.
    """
    parser = argparse.ArgumentParser(description='Pub/Sub Line Counter Subscriber')
    parser.add_argument('--project-id', required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--subscription', required=True,
                       help='Pub/Sub subscription name')
    parser.add_argument('--timeout', type=int,
                       help='Timeout in seconds (default: run indefinitely)')
    parser.add_argument('--max-workers', type=int, default=5,
                       help='Maximum number of concurrent message processors')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Create and start subscriber
        subscriber = LineCounterSubscriber(
            project_id=args.project_id,
            subscription_name=args.subscription,
            max_workers=args.max_workers
        )
        
        subscriber.start_listening(timeout=args.timeout)
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()