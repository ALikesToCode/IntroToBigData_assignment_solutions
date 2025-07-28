#!/usr/bin/env python3
"""
Infrastructure Deployment Script for Real-time Line Counting
Course: Introduction to Big Data - Week 6 Assignment

This script automates the deployment of:
1. GCS bucket for file uploads
2. Pub/Sub topic and subscription
3. Cloud Function triggered by bucket events
4. IAM permissions setup

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import json
import time
import argparse
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InfrastructureDeployer:
    """
    Handles deployment of GCP infrastructure for real-time file processing.
    """
    
    def __init__(self, project_id, region='us-central1'):
        """
        Initialize deployer with project settings.
        
        Args:
            project_id: Google Cloud project ID
            region: GCP region for resources
        """
        self.project_id = project_id
        self.region = region
        self.bucket_name = f"{project_id}-realtime-linecount-bucket"
        self.topic_name = "file-processing-topic"
        self.subscription_name = "file-processing-subscription"
        self.function_name = "file-upload-processor"
        
        # Paths
        self.script_dir = Path(__file__).parent
        self.function_dir = self.script_dir / "cloud-function"
        
        logger.info(f"Initialized deployer for project: {project_id}")
        logger.info(f"Region: {region}")
        logger.info(f"Bucket: {self.bucket_name}")
    
    def run_command(self, command, description, check_result=True):
        """
        Execute a shell command with logging.
        
        Args:
            command: Command to execute
            description: Description for logging
            check_result: Whether to check command result
            
        Returns:
            subprocess.CompletedProcess: Command result
        """
        logger.info(f"🔧 {description}")
        logger.debug(f"Command: {command}")
        
        try:
            result = subprocess.run(
                command, 
                shell=True, 
                capture_output=True, 
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if check_result and result.returncode != 0:
                logger.error(f"Command failed: {command}")
                logger.error(f"Error: {result.stderr}")
                raise subprocess.CalledProcessError(result.returncode, command)
            
            if result.stdout.strip():
                logger.debug(f"Output: {result.stdout.strip()}")
            
            return result
            
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out: {command}")
            raise
        except Exception as e:
            logger.error(f"Error executing command: {e}")
            raise
    
    def check_authentication(self):
        """
        Verify Google Cloud authentication and project access.
        """
        logger.info("🔐 Checking Google Cloud authentication...")
        
        # Check if gcloud is installed
        result = self.run_command("gcloud version", "Checking gcloud installation")
        
        # Check authentication
        result = self.run_command(
            "gcloud auth list --filter=status:ACTIVE --format='value(account)'",
            "Checking authentication"
        )
        
        if not result.stdout.strip():
            logger.error("No active authentication found")
            logger.error("Please run: gcloud auth login")
            raise RuntimeError("Authentication required")
        
        logger.info(f"Authenticated as: {result.stdout.strip()}")
        
        # Set project
        self.run_command(f"gcloud config set project {self.project_id}", "Setting project")
        
        # Verify project access
        self.run_command(f"gcloud projects describe {self.project_id}", "Verifying project access")
        
        logger.info("✅ Authentication verified")
    
    def enable_apis(self):
        """
        Enable required Google Cloud APIs.
        """
        apis = [
            'cloudfunctions.googleapis.com',
            'pubsub.googleapis.com',
            'storage.googleapis.com',
            'cloudbuild.googleapis.com',
            'logging.googleapis.com'
        ]
        
        logger.info("🔌 Enabling required APIs...")
        
        for api in apis:
            self.run_command(
                f"gcloud services enable {api}",
                f"Enabling {api}"
            )
        
        logger.info("✅ APIs enabled")
        time.sleep(10)  # Wait for API propagation
    
    def create_bucket(self):
        """
        Create GCS bucket for file uploads.
        """
        logger.info(f"🪣 Creating GCS bucket: {self.bucket_name}")
        
        # Check if bucket already exists
        result = self.run_command(
            f"gsutil ls gs://{self.bucket_name}",
            "Checking if bucket exists",
            check_result=False
        )
        
        if result.returncode == 0:
            logger.info(f"Bucket {self.bucket_name} already exists")
            return
        
        # Create bucket
        self.run_command(
            f"gsutil mb -p {self.project_id} -l {self.region} gs://{self.bucket_name}",
            f"Creating bucket gs://{self.bucket_name}"
        )
        
        # Set uniform bucket-level access
        self.run_command(
            f"gsutil uniformbucketlevelaccess set on gs://{self.bucket_name}",
            "Setting uniform bucket-level access"
        )
        
        logger.info("✅ GCS bucket created")
    
    def create_pubsub_resources(self):
        """
        Create Pub/Sub topic and subscription.
        """
        logger.info("📡 Creating Pub/Sub resources...")
        
        # Create topic
        result = self.run_command(
            f"gcloud pubsub topics describe {self.topic_name}",
            "Checking if topic exists",
            check_result=False
        )
        
        if result.returncode != 0:
            self.run_command(
                f"gcloud pubsub topics create {self.topic_name}",
                f"Creating topic: {self.topic_name}"
            )
        else:
            logger.info(f"Topic {self.topic_name} already exists")
        
        # Create subscription
        result = self.run_command(
            f"gcloud pubsub subscriptions describe {self.subscription_name}",
            "Checking if subscription exists",
            check_result=False
        )
        
        if result.returncode != 0:
            self.run_command(
                f"gcloud pubsub subscriptions create {self.subscription_name} --topic={self.topic_name}",
                f"Creating subscription: {self.subscription_name}"
            )
        else:
            logger.info(f"Subscription {self.subscription_name} already exists")
        
        logger.info("✅ Pub/Sub resources created")
    
    def deploy_cloud_function(self):
        """
        Deploy Cloud Function for file processing.
        """
        logger.info(f"☁️ Deploying Cloud Function: {self.function_name}")
        
        if not self.function_dir.exists():
            raise FileNotFoundError(f"Cloud Function directory not found: {self.function_dir}")
        
        # Change to function directory
        original_dir = os.getcwd()
        os.chdir(self.function_dir)
        
        try:
            # Deploy function
            command = f"""
            gcloud functions deploy {self.function_name} \
                --runtime python39 \
                --trigger-event google.storage.object.finalize \
                --trigger-resource {self.bucket_name} \
                --entry-point process_file_upload \
                --region {self.region} \
                --set-env-vars GCP_PROJECT={self.project_id},PUBSUB_TOPIC={self.topic_name} \
                --timeout 540 \
                --memory 256MB \
                --max-instances 10
            """
            
            self.run_command(command, "Deploying Cloud Function")
            
        finally:
            os.chdir(original_dir)
        
        logger.info("✅ Cloud Function deployed")
    
    def setup_iam_permissions(self):
        """
        Set up required IAM permissions.
        """
        logger.info("🔑 Setting up IAM permissions...")
        
        # Get the Cloud Function service account
        result = self.run_command(
            f"gcloud functions describe {self.function_name} --region={self.region} --format='value(serviceAccountEmail)'",
            "Getting Cloud Function service account"
        )
        
        service_account = result.stdout.strip()
        if not service_account:
            logger.warning("Could not determine Cloud Function service account")
            return
        
        logger.info(f"Cloud Function service account: {service_account}")
        
        # Grant Pub/Sub publisher role
        self.run_command(
            f"gcloud projects add-iam-policy-binding {self.project_id} "
            f"--member='serviceAccount:{service_account}' "
            f"--role='roles/pubsub.publisher'",
            "Granting Pub/Sub publisher role"
        )
        
        # Grant Storage object viewer role
        self.run_command(
            f"gcloud projects add-iam-policy-binding {self.project_id} "
            f"--member='serviceAccount:{service_account}' "
            f"--role='roles/storage.objectViewer'",
            "Granting Storage object viewer role"
        )
        
        logger.info("✅ IAM permissions configured")
    
    def test_deployment(self):
        """
        Test the deployment by uploading a sample file.
        """
        logger.info("🧪 Testing deployment...")
        
        # Create a test file
        test_file = self.script_dir / "test_file.txt"
        test_content = "Line 1\nLine 2\nLine 3\nHello World\nTest Line"
        
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        logger.info(f"Created test file with {len(test_content.splitlines())} lines")
        
        # Upload test file to bucket
        self.run_command(
            f"gsutil cp {test_file} gs://{self.bucket_name}/",
            "Uploading test file"
        )
        
        # Clean up local test file
        test_file.unlink()
        
        logger.info("✅ Test file uploaded - check Cloud Function logs and Pub/Sub messages")
        logger.info(f"View logs: gcloud functions logs read {self.function_name} --region={self.region}")
    
    def deploy_all(self):
        """
        Deploy complete infrastructure.
        """
        logger.info("🚀 Starting complete infrastructure deployment...")
        
        try:
            self.check_authentication()
            self.enable_apis()
            self.create_bucket()
            self.create_pubsub_resources()
            self.deploy_cloud_function()
            self.setup_iam_permissions()
            self.test_deployment()
            
            logger.info("🎉 Deployment completed successfully!")
            self.print_usage_instructions()
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            raise
    
    def print_usage_instructions(self):
        """
        Print usage instructions after successful deployment.
        """
        print(f"\n{'='*60}")
        print("🎉 DEPLOYMENT SUCCESSFUL!")
        print(f"{'='*60}")
        print(f"Project ID: {self.project_id}")
        print(f"Region: {self.region}")
        print(f"Bucket: gs://{self.bucket_name}")
        print(f"Topic: {self.topic_name}")
        print(f"Subscription: {self.subscription_name}")
        print(f"Function: {self.function_name}")
        print(f"\n📋 NEXT STEPS:")
        print(f"1. Start the subscriber:")
        print(f"   python subscriber/line_counter_subscriber.py \\")
        print(f"     --project-id {self.project_id} \\")
        print(f"     --subscription {self.subscription_name}")
        print(f"\n2. Upload files to trigger processing:")
        print(f"   gsutil cp your_file.txt gs://{self.bucket_name}/")
        print(f"\n3. Monitor Cloud Function logs:")
        print(f"   gcloud functions logs read {self.function_name} --region={self.region}")
        print(f"\n4. View Pub/Sub messages:")
        print(f"   gcloud pubsub subscriptions pull {self.subscription_name} --auto-ack")
        print(f"{'='*60}\n")

def main():
    """
    Main function for deployment script.
    """
    parser = argparse.ArgumentParser(description='Deploy real-time line counting infrastructure')
    parser.add_argument('--project-id', required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--region', default='us-central1',
                       help='GCP region (default: us-central1)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        deployer = InfrastructureDeployer(args.project_id, args.region)
        deployer.deploy_all()
        
    except KeyboardInterrupt:
        logger.info("Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()