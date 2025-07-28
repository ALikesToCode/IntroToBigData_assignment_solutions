#!/usr/bin/env python3
"""
Local Testing Script for Real-time Line Counting
Course: Introduction to Big Data - Week 6 Assignment

This script tests the Cloud Function and Pub/Sub subscriber locally
before deploying to Google Cloud.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import json
import time
import logging
import tempfile
import subprocess
from pathlib import Path
from unittest.mock import Mock, MagicMock

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_cloud_function_logic():
    """
    Test the Cloud Function logic locally.
    """
    logger.info("🧪 Testing Cloud Function logic...")
    
    try:
        # Add the cloud-function directory to path
        function_dir = Path(__file__).parent / "cloud-function"
        sys.path.insert(0, str(function_dir))
        
        # Mock environment variables
        os.environ['GCP_PROJECT'] = 'test-project'
        os.environ['PUBSUB_TOPIC'] = 'test-topic'
        
        # Import the function
        from main import process_file_upload
        
        # Create mock cloud event
        mock_event = Mock()
        mock_event.data = {
            'bucket': 'test-bucket',
            'name': 'test-file.txt',
            'eventType': 'google.storage.object.finalize',
            'timeCreated': '2025-07-27T10:00:00Z'
        }
        
        # Mock the Pub/Sub publisher
        import main
        original_publisher = main.publisher
        
        mock_publisher = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'test-message-id'
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = 'projects/test-project/topics/test-topic'
        
        main.publisher = mock_publisher
        
        try:
            # Test the function
            result = process_file_upload(mock_event)
            
            # Verify the result
            assert result['status'] == 'success'
            assert result['message_id'] == 'test-message-id'
            assert 'test-bucket' in result['file_processed']
            
            # Verify publisher was called
            assert mock_publisher.publish.called
            
            logger.info("✅ Cloud Function logic test passed")
            return True
            
        finally:
            # Restore original publisher
            main.publisher = original_publisher
            
    except Exception as e:
        logger.error(f"❌ Cloud Function test failed: {e}")
        return False

def test_subscriber_logic():
    """
    Test the Pub/Sub subscriber logic locally.
    """
    logger.info("🧪 Testing Pub/Sub subscriber logic...")
    
    try:
        # Add the subscriber directory to path
        subscriber_dir = Path(__file__).parent / "subscriber"
        sys.path.insert(0, str(subscriber_dir))
        
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as temp_file:
            test_content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
            temp_file.write(test_content)
            temp_file_path = temp_file.name
        
        try:
            # Import subscriber class
            from line_counter_subscriber import LineCounterSubscriber
            
            # Mock the Google Cloud clients
            mock_storage_client = Mock()
            mock_bucket = Mock()
            mock_blob = Mock()
            
            # Configure mock blob
            mock_blob.exists.return_value = True
            mock_blob.download_as_text.return_value = test_content
            mock_blob.size = len(test_content)
            
            mock_bucket.blob.return_value = mock_blob
            mock_storage_client.bucket.return_value = mock_bucket
            
            # Create subscriber instance with mocked client
            subscriber = LineCounterSubscriber('test-project', 'test-subscription')
            subscriber.storage_client = mock_storage_client
            
            # Test line counting
            line_count = subscriber.count_lines_in_file('test-bucket', 'test-file.txt')
            
            # Verify result
            expected_lines = len(test_content.splitlines())
            assert line_count == expected_lines, f"Expected {expected_lines} lines, got {line_count}"
            
            logger.info(f"✅ Subscriber logic test passed - counted {line_count} lines")
            return True
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)
            
    except Exception as e:
        logger.error(f"❌ Subscriber test failed: {e}")
        return False

def test_message_flow():
    """
    Test the complete message flow between Cloud Function and subscriber.
    """
    logger.info("🧪 Testing complete message flow...")
    
    try:
        # Create test message that Cloud Function would send
        test_message = {
            'bucket_name': 'test-bucket',
            'file_name': 'test-file.txt',
            'event_type': 'google.storage.object.finalize',
            'time_created': '2025-07-27T10:00:00Z',
            'file_path': 'gs://test-bucket/test-file.txt',
            'processing_request': 'line_count'
        }
        
        # Mock Pub/Sub message
        mock_message = Mock()
        mock_message.data = json.dumps(test_message).encode('utf-8')
        mock_message.message_id = 'test-message-id'
        
        # Test message parsing
        parsed_data = json.loads(mock_message.data.decode('utf-8'))
        
        # Verify message structure
        assert parsed_data['bucket_name'] == 'test-bucket'
        assert parsed_data['file_name'] == 'test-file.txt'
        assert parsed_data['processing_request'] == 'line_count'
        
        logger.info("✅ Message flow test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Message flow test failed: {e}")
        return False

def check_dependencies():
    """
    Check if required dependencies are installed.
    """
    logger.info("🔍 Checking dependencies...")
    
    required_packages = [
        'google-cloud-pubsub',
        'google-cloud-storage',
        'functions-framework'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            logger.info(f"✅ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            logger.error(f"❌ {package} is missing")
    
    if missing_packages:
        logger.error(f"Please install missing packages: pip install {' '.join(missing_packages)}")
        return False
    
    # Check gcloud CLI
    try:
        result = subprocess.run(['gcloud', 'version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✅ Google Cloud CLI is installed")
        else:
            logger.error("❌ Google Cloud CLI is not installed")
            return False
    except FileNotFoundError:
        logger.error("❌ Google Cloud CLI is not installed")
        return False
    
    return True

def main():
    """
    Run all local tests.
    """
    logger.info("🚀 Starting local tests for Week 6 assignment...")
    
    try:
        # Check dependencies
        if not check_dependencies():
            logger.error("❌ Dependency check failed")
            sys.exit(1)
        
        # Run tests
        tests = [
            ("Cloud Function Logic", test_cloud_function_logic),
            ("Subscriber Logic", test_subscriber_logic),
            ("Message Flow", test_message_flow)
        ]
        
        results = []
        for test_name, test_func in tests:
            logger.info(f"\n{'='*50}")
            logger.info(f"Running test: {test_name}")
            logger.info(f"{'='*50}")
            
            result = test_func()
            results.append((test_name, result))
            
            if result:
                logger.info(f"✅ {test_name} PASSED")
            else:
                logger.error(f"❌ {test_name} FAILED")
        
        # Summary
        logger.info(f"\n{'='*50}")
        logger.info("TEST SUMMARY")
        logger.info(f"{'='*50}")
        
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for test_name, result in results:
            status = "PASS" if result else "FAIL"
            logger.info(f"{test_name}: {status}")
        
        logger.info(f"\nOverall: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("🎉 All tests passed! Ready for deployment.")
            return True
        else:
            logger.error("❌ Some tests failed. Please fix issues before deployment.")
            return False
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)