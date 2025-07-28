#!/usr/bin/env python3
"""
Google Cloud Deployment Script for Week 6 Real-time Line Counting
Course: Introduction to Big Data - Week 6

This script deploys the complete real-time file processing pipeline to Google Cloud
using Cloud Functions, Pub/Sub, and Cloud Storage.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import json
import time
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Week6CloudDeployment:
    """
    Manages deployment of Week 6 real-time file processing pipeline to Google Cloud.
    """
    
    def __init__(self, project_id, region='us-central1'):
        """
        Initialize cloud deployment manager.
        
        Args:
            project_id: Google Cloud project ID
            region: GCP region for resources
        """
        self.project_id = project_id
        self.region = region
        self.timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        
        # Resource names
        self.bucket_name = f"{project_id}-week6-linecount-bucket"
        self.function_name = "file-upload-processor"
        self.topic_name = "file-processing-topic"
        self.subscription_name = "file-processing-subscription"
        
        # Paths
        self.script_dir = Path(__file__).parent
        self.project_dir = self.script_dir.parent
        self.function_dir = self.project_dir / "cloud-function"
        
        logger.info(f"🚀 Week 6 Cloud Deployment initialized")
        logger.info(f"   Project: {self.project_id}")
        logger.info(f"   Region: {self.region}")
        logger.info(f"   Bucket: {self.bucket_name}")
        logger.info(f"   Function: {self.function_name}")
    
    def run_command(self, command, description, check=True):
        """
        Execute shell command with logging.
        
        Args:
            command: Command to execute
            description: Description for logging
            check: Whether to check return code
            
        Returns:
            subprocess.CompletedProcess result
        """
        logger.info(f"🔧 {description}")
        logger.debug(f"   Command: {command}")
        
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if check and result.returncode != 0:
                logger.error(f"❌ Command failed: {command}")
                logger.error(f"   Error: {result.stderr}")
                raise subprocess.CalledProcessError(result.returncode, command)
            
            if result.stdout.strip():
                logger.debug(f"   Output: {result.stdout.strip()}")
            
            return result
            
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Command timed out: {command}")
            raise
        except Exception as e:
            logger.error(f"❌ Command execution error: {e}")
            raise
    
    def check_authentication(self):
        """
        Verify Google Cloud authentication and project access.
        """
        logger.info("🔐 Checking Google Cloud authentication...")
        
        # Check authentication
        result = self.run_command(
            "gcloud auth list --filter=status:ACTIVE --format='value(account)'",
            "Checking authentication"
        )
        
        if not result.stdout.strip():
            logger.error("❌ No active authentication found")
            logger.error("Please run: gcloud auth login")
            logger.error("And: gcloud auth application-default login")
            raise RuntimeError("Authentication required")
        
        logger.info(f"✅ Authenticated as: {result.stdout.strip()}")
        
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
            'logging.googleapis.com',
            'eventarc.googleapis.com',
            'iam.googleapis.com'
        ]
        
        logger.info("🔌 Enabling required APIs...")
        
        for api in apis:
            self.run_command(f"gcloud services enable {api}", f"Enabling {api}")
            time.sleep(2)  # Rate limiting
        
        logger.info("✅ APIs enabled")
        time.sleep(10)  # Wait for API propagation
    
    def create_gcs_bucket(self):
        """
        Create GCS bucket for file uploads.
        """
        logger.info(f"🪣 Creating GCS bucket: {self.bucket_name}")
        
        # Check if bucket exists
        result = self.run_command(
            f"gsutil ls gs://{self.bucket_name}",
            "Checking if bucket exists",
            check=False
        )
        
        if result.returncode == 0:
            logger.info(f"ℹ️ Bucket {self.bucket_name} already exists")
        else:
            # Create bucket
            self.run_command(
                f"gsutil mb -p {self.project_id} -l {self.region} gs://{self.bucket_name}",
                f"Creating bucket {self.bucket_name}"
            )
            
            # Set uniform bucket-level access
            self.run_command(
                f"gsutil uniformbucketlevelaccess set on gs://{self.bucket_name}",
                "Setting uniform bucket-level access"
            )
        
        logger.info("✅ GCS bucket ready")
    
    def configure_gcs_iam_permissions(self):
        """
        Configure IAM permissions for GCS service account to publish to Pub/Sub.
        
        This is required for Cloud Functions 2nd gen GCS triggers to work with Eventarc.
        """
        logger.info("🔐 Configuring IAM permissions for GCS service account...")
        
        try:
            # Get the GCS service account for the project
            result = self.run_command(
                f"gsutil kms serviceaccount -p {self.project_id}",
                "Getting GCS service account"
            )
            
            gcs_service_account = result.stdout.strip()
            
            if not gcs_service_account:
                raise RuntimeError("Unable to retrieve GCS service account")
            
            logger.info(f"📧 GCS Service Account: {gcs_service_account}")
            
            # Check if pubsub.publisher role is already granted
            check_result = self.run_command(
                f"gcloud projects get-iam-policy {self.project_id} "
                f"--format='value(bindings[].members)' "
                f"--filter='bindings.role:roles/pubsub.publisher'",
                "Checking existing Pub/Sub Publisher permissions",
                check=False
            )
            
            if f"serviceAccount:{gcs_service_account}" not in check_result.stdout:
                # Grant pubsub.publisher role to GCS service account
                self.run_command(
                    f"gcloud projects add-iam-policy-binding {self.project_id} "
                    f"--member='serviceAccount:{gcs_service_account}' "
                    f"--role='roles/pubsub.publisher'",
                    "Granting Pub/Sub Publisher role to GCS service account"
                )
                logger.info("✅ Granted Pub/Sub Publisher role")
            else:
                logger.info("ℹ️ Pub/Sub Publisher role already granted")
            
            # Check if eventarc.eventReceiver role is already granted
            check_result = self.run_command(
                f"gcloud projects get-iam-policy {self.project_id} "
                f"--format='value(bindings[].members)' "
                f"--filter='bindings.role:roles/eventarc.eventReceiver'",
                "Checking existing Eventarc Event Receiver permissions",
                check=False
            )
            
            if f"serviceAccount:{gcs_service_account}" not in check_result.stdout:
                # Grant eventarc.eventReceiver role for completeness
                self.run_command(
                    f"gcloud projects add-iam-policy-binding {self.project_id} "
                    f"--member='serviceAccount:{gcs_service_account}' "
                    f"--role='roles/eventarc.eventReceiver'",
                    "Granting Eventarc Event Receiver role to GCS service account"
                )
                logger.info("✅ Granted Eventarc Event Receiver role")
            else:
                logger.info("ℹ️ Eventarc Event Receiver role already granted")
            
            # Wait for IAM changes to propagate
            logger.info("⏳ Waiting for IAM changes to propagate...")
            time.sleep(30)
            
            logger.info("✅ IAM permissions configured for GCS service account")
            
        except Exception as e:
            logger.error(f"❌ Failed to configure IAM permissions: {e}")
            logger.error("   This may cause Cloud Functions GCS triggers to fail")
            logger.error("   Manual fix: Run the following commands:")
            logger.error(f"   gsutil kms serviceaccount -p {self.project_id}")
            logger.error("   gcloud projects add-iam-policy-binding PROJECT_ID \\")
            logger.error("     --member='serviceAccount:GCS_SERVICE_ACCOUNT' \\")
            logger.error("     --role='roles/pubsub.publisher'")
            raise
    
    def create_pubsub_resources(self):
        """
        Create Pub/Sub topic and subscription.
        """
        logger.info("📡 Creating Pub/Sub resources...")
        
        # Create topic
        result = self.run_command(
            f"gcloud pubsub topics describe {self.topic_name}",
            "Checking if topic exists",
            check=False
        )
        
        if result.returncode != 0:
            self.run_command(
                f"gcloud pubsub topics create {self.topic_name}",
                f"Creating topic: {self.topic_name}"
            )
        else:
            logger.info(f"ℹ️ Topic {self.topic_name} already exists")
        
        # Create subscription
        result = self.run_command(
            f"gcloud pubsub subscriptions describe {self.subscription_name}",
            "Checking if subscription exists",
            check=False
        )
        
        if result.returncode != 0:
            self.run_command(
                f"gcloud pubsub subscriptions create {self.subscription_name} --topic={self.topic_name}",
                f"Creating subscription: {self.subscription_name}"
            )
        else:
            logger.info(f"ℹ️ Subscription {self.subscription_name} already exists")
        
        logger.info("✅ Pub/Sub resources ready")
    
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
            # Deploy function with 2nd gen architecture
            command = f"""
            gcloud functions deploy {self.function_name} \\
                --gen2 \\
                --runtime python311 \\
                --trigger-bucket {self.bucket_name} \\
                --entry-point process_file_upload \\
                --region {self.region} \\
                --set-env-vars GCP_PROJECT={self.project_id},PUBSUB_TOPIC={self.topic_name} \\
                --timeout 540 \\
                --memory 512Mi \\
                --max-instances 10 \\
                --source .
            """
            
            self.run_command(command, "Deploying Cloud Function")
            
        finally:
            os.chdir(original_dir)
        
        logger.info("✅ Cloud Function deployed")
    
    def upload_sample_files(self):
        """
        Upload sample files to test the pipeline.
        """
        logger.info("📄 Uploading sample files for testing...")
        
        # Upload existing sample files
        data_dir = self.project_dir / "data"
        if data_dir.exists():
            for file_path in data_dir.glob("*.txt"):
                gcs_path = f"gs://{self.bucket_name}/test-files/{file_path.name}"
                self.run_command(f"gsutil cp {file_path} {gcs_path}", f"Uploading {file_path.name}")
        
        # Create additional test files
        test_files = [
            ("test_small.txt", "Line 1\nLine 2\nLine 3"),
            ("test_medium.txt", "\n".join([f"Line {i}" for i in range(1, 51)])),  # 50 lines
            ("test_large.txt", "\n".join([f"Line {i}" for i in range(1, 1001)]))  # 1000 lines
        ]
        
        for filename, content in test_files:
            # Create temporary file
            temp_file = Path(f"/tmp/{filename}")
            with open(temp_file, 'w') as f:
                f.write(content)
            
            # Upload to GCS
            gcs_path = f"gs://{self.bucket_name}/test-files/{filename}"
            self.run_command(f"gsutil cp {temp_file} {gcs_path}", f"Uploading {filename}")
            
            # Clean up
            temp_file.unlink()
        
        logger.info("✅ Sample files uploaded")
    
    def test_pipeline(self):
        """
        Test the deployed pipeline by uploading a file and checking results.
        """
        logger.info("🧪 Testing the deployed pipeline...")
        
        # Create a test file
        test_content = "Test line 1\nTest line 2\nTest line 3\nTest line 4\nTest line 5"
        test_file = Path(f"/tmp/pipeline_test_{self.timestamp}.txt")
        
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        logger.info(f"📝 Created test file with {len(test_content.splitlines())} lines")
        
        # Upload test file to trigger function
        gcs_path = f"gs://{self.bucket_name}/pipeline_test_{self.timestamp}.txt"
        self.run_command(f"gsutil cp {test_file} {gcs_path}", "Uploading test file to trigger pipeline")
        
        # Wait for processing
        logger.info("⏳ Waiting for pipeline processing...")
        time.sleep(10)
        
        # Check function logs
        self.run_command(
            f"gcloud functions logs read {self.function_name} --region={self.region} --limit=10",
            "Checking Cloud Function logs"
        )
        
        # Check Pub/Sub messages
        logger.info("📡 Checking Pub/Sub messages...")
        result = self.run_command(
            f"gcloud pubsub subscriptions pull {self.subscription_name} --limit=5 --auto-ack",
            "Pulling Pub/Sub messages",
            check=False
        )
        
        if result.returncode == 0 and result.stdout.strip():
            logger.info("✅ Pipeline test successful - messages found in Pub/Sub")
        else:
            logger.warning("⚠️ No messages found - pipeline may need more time")
        
        # Clean up test file
        test_file.unlink()
        
        logger.info("✅ Pipeline testing completed")
    
    def create_subscriber_vm(self):
        """
        Create a Compute Engine VM to run the subscriber.
        """
        logger.info("🖥️ Creating Compute Engine VM for subscriber...")
        
        vm_name = f"week6-subscriber-vm-{self.timestamp}"
        
        # Create startup script file to avoid gcloud metadata parsing issues
        startup_script_content = f'''#!/bin/bash
apt-get update
apt-get install -y python3 python3-pip git
pip3 install google-cloud-pubsub google-cloud-storage

# Create subscriber script
cat > /home/subscriber.py << 'EOF'
#!/usr/bin/env python3
import time
import json
import logging
from google.cloud import pubsub_v1
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def count_lines_in_file(bucket_name, file_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        if not blob.exists():
            logger.error(f"File not found: gs://{{bucket_name}}/{{file_name}}")
            return None
        
        content = blob.download_as_text()
        line_count = len(content.splitlines())
        
        print(f"\\n{{'='*60}}")
        print(f"📄 FILE PROCESSED: {{file_name}}")
        print(f"📊 LINE COUNT: {{line_count:,}}")
        print(f"📏 FILE SIZE: {{blob.size:,}} bytes")
        print(f"{{'='*60}}\\n")
        
        return line_count
    except Exception as e:
        logger.error(f"Error processing file: {{e}}")
        return None

def process_message(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        bucket_name = data.get("bucket_name")
        file_name = data.get("file_name")
        
        if bucket_name and file_name:
            count_lines_in_file(bucket_name, file_name)
        
        message.ack()
    except Exception as e:
        logger.error(f"Error processing message: {{e}}")

def main():
    project_id = "{self.project_id}"
    subscription_name = "{self.subscription_name}"
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    
    logger.info(f"Listening for messages on {{subscription_path}}")
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_message)
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

if __name__ == "__main__": 
    main()
EOF

chmod +x /home/subscriber.py

# Create systemd service
cat > /etc/systemd/system/subscriber.service << 'EOF'
[Unit]
Description=Week 6 Pub/Sub Subscriber
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/home
ExecStart=/usr/bin/python3 /home/subscriber.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable subscriber
systemctl start subscriber
'''
        
        # Write startup script to temporary file
        startup_script_path = f"/tmp/startup-script-{self.timestamp}.sh"
        with open(startup_script_path, 'w') as f:
            f.write(startup_script_content)
        
        try:
            # Create VM with startup script file
            create_vm_cmd = f"""
            gcloud compute instances create {vm_name} \\
                --zone={self.region}-b \\
                --machine-type=e2-medium \\
                --boot-disk-size=20GB \\
                --boot-disk-type=pd-standard \\
                --image-family=debian-11 \\
                --image-project=debian-cloud \\
                --scopes=https://www.googleapis.com/auth/cloud-platform \\
                --metadata-from-file=startup-script={startup_script_path}
            """
            
            self.run_command(create_vm_cmd, f"Creating VM: {vm_name}")
            
            logger.info(f"✅ VM created: {vm_name}")
            logger.info(f"   The subscriber service will start automatically")
            
        finally:
            # Clean up temporary startup script file
            import os
            if os.path.exists(startup_script_path):
                os.remove(startup_script_path)
                logger.debug(f"Cleaned up temporary startup script: {startup_script_path}")
        
        return vm_name
    
    def deploy_complete_pipeline(self):
        """
        Deploy the complete Week 6 pipeline to Google Cloud.
        """
        logger.info("🚀 Starting complete Week 6 pipeline deployment...")
        
        try:
            # Check prerequisites
            self.check_authentication()
            
            # Enable APIs
            self.enable_apis()
            
            # Create GCS bucket
            self.create_gcs_bucket()
            
            # Configure IAM permissions for GCS service account
            self.configure_gcs_iam_permissions()
            
            # Create Pub/Sub resources
            self.create_pubsub_resources()
            
            # Deploy Cloud Function
            self.deploy_cloud_function()
            
            # Upload sample files
            self.upload_sample_files()
            
            # Create subscriber VM
            vm_name = self.create_subscriber_vm()
            
            # Test pipeline
            self.test_pipeline()
            
            logger.info("🎉 Week 6 pipeline deployment completed successfully!")
            
            # Print usage instructions
            self.print_usage_instructions(vm_name)
            
        except Exception as e:
            logger.error(f"❌ Pipeline deployment failed: {e}")
            raise
    
    def print_usage_instructions(self, vm_name):
        """
        Print usage instructions for the deployed pipeline.
        """
        print(f"\n{'='*80}")
        print("🎉 WEEK 6 REAL-TIME LINE COUNTING PIPELINE DEPLOYED!")
        print(f"{'='*80}")
        print(f"Project: {self.project_id}")
        print(f"Region: {self.region}")
        print(f"Bucket: gs://{self.bucket_name}")
        print(f"Function: {self.function_name}")
        print(f"Subscriber VM: {vm_name}")
        print(f"\n📋 USAGE INSTRUCTIONS:")
        print(f"1. Upload files to trigger processing:")
        print(f"   gsutil cp your_file.txt gs://{self.bucket_name}/")
        print(f"\n2. Monitor Cloud Function logs:")
        print(f"   gcloud functions logs read {self.function_name} --region={self.region}")
        print(f"\n3. Monitor subscriber VM:")
        print(f"   gcloud compute ssh {vm_name} --zone={self.region}-b")
        print(f"   sudo journalctl -u subscriber -f")
        print(f"\n4. Check Pub/Sub messages:")
        print(f"   gcloud pubsub subscriptions pull {self.subscription_name} --auto-ack")
        print(f"\n5. View uploaded test files:")
        print(f"   gsutil ls gs://{self.bucket_name}/test-files/")
        print(f"\n🧪 TEST THE PIPELINE:")
        print(f"   echo 'Line 1\\nLine 2\\nLine 3' > test.txt")
        print(f"   gsutil cp test.txt gs://{self.bucket_name}/")
        print(f"   # Watch the subscriber VM logs for real-time line counting!")
        print(f"\n🧹 CLEANUP WHEN DONE:")
        print(f"   gcloud compute instances delete {vm_name} --zone={self.region}-b")
        print(f"   gcloud functions delete {self.function_name} --region={self.region}")
        print(f"   gsutil rm -r gs://{self.bucket_name}")
        print(f"{'='*80}\n")

def main():
    """
    Main function for Week 6 cloud deployment.
    """
    parser = argparse.ArgumentParser(description='Deploy Week 6 Real-time Line Counting to Google Cloud')
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
        # Create deployment manager
        deployment = Week6CloudDeployment(
            project_id=args.project_id,
            region=args.region
        )
        
        # Deploy complete pipeline
        deployment.deploy_complete_pipeline()
        
    except KeyboardInterrupt:
        logger.info("👋 Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Deployment failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()