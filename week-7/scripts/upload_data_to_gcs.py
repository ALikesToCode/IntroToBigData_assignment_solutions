#!/usr/bin/env python3
"""
Upload Generated Data to Google Cloud Storage
Course: Introduction to Big Data - Week 7

This script uploads the generated customer transaction data to GCS bucket
for cloud-based processing with Dataproc and Pub/Sub.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import logging
import argparse
import subprocess
from pathlib import Path
from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GCSDataUploader:
    """
    Handles uploading data files to Google Cloud Storage for cloud processing.
    """
    
    def __init__(self, project_id, bucket_name=None):
        """
        Initialize GCS uploader.
        
        Args:
            project_id: Google Cloud project ID
            bucket_name: GCS bucket name (default: {project_id}-week7-streaming-data)
        """
        self.project_id = project_id
        self.bucket_name = bucket_name or f"{project_id}-week7-streaming-data"
        
        try:
            self.client = storage.Client(project=project_id)
            logger.info(f"✅ GCS client initialized for project: {project_id}")
            logger.info(f"   Target bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize GCS client: {e}")
            raise
    
    def create_bucket_if_not_exists(self, location='us-central1'):
        """
        Create GCS bucket if it doesn't exist.
        
        Args:
            location: GCS bucket location
        """
        try:
            # Check if bucket exists
            try:
                bucket = self.client.get_bucket(self.bucket_name)
                logger.info(f"✅ Bucket {self.bucket_name} already exists")
                return bucket
            except Exception:
                logger.info(f"📦 Creating bucket: {self.bucket_name}")
                
                bucket = self.client.bucket(self.bucket_name)
                bucket = self.client.create_bucket(bucket, location=location)
                
                logger.info(f"✅ Bucket created: {self.bucket_name}")
                logger.info(f"   Location: {location}")
                return bucket
                
        except Exception as e:
            logger.error(f"❌ Failed to create bucket: {e}")
            raise
    
    def upload_file(self, local_file_path, gcs_file_path=None):
        """
        Upload local file to GCS bucket.
        
        Args:
            local_file_path: Path to local file
            gcs_file_path: Path in GCS bucket (default: same as local filename)
            
        Returns:
            GCS blob object
        """
        try:
            local_path = Path(local_file_path)
            if not local_path.exists():
                raise FileNotFoundError(f"Local file not found: {local_file_path}")
            
            # Default GCS path
            if gcs_file_path is None:
                gcs_file_path = f"data/{local_path.name}"
            
            logger.info(f"📤 Uploading file to GCS...")
            logger.info(f"   Local: {local_file_path}")
            logger.info(f"   GCS: gs://{self.bucket_name}/{gcs_file_path}")
            
            # Create bucket if needed
            bucket = self.create_bucket_if_not_exists()
            
            # Upload file
            blob = bucket.blob(gcs_file_path)
            
            # Get file size for progress
            file_size = local_path.stat().st_size
            logger.info(f"   File size: {file_size:,} bytes")
            
            blob.upload_from_filename(str(local_path))
            
            logger.info(f"✅ Upload completed successfully")
            logger.info(f"   GCS URL: gs://{self.bucket_name}/{gcs_file_path}")
            
            return blob
            
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            raise
    
    def upload_directory(self, local_dir_path, gcs_prefix=""):
        """
        Upload entire directory to GCS.
        
        Args:
            local_dir_path: Path to local directory
            gcs_prefix: Prefix for GCS paths
            
        Returns:
            List of uploaded blob objects
        """
        try:
            local_dir = Path(local_dir_path)
            if not local_dir.exists() or not local_dir.is_dir():
                raise FileNotFoundError(f"Local directory not found: {local_dir_path}")
            
            logger.info(f"📁 Uploading directory to GCS...")
            logger.info(f"   Local: {local_dir_path}")
            logger.info(f"   GCS prefix: {gcs_prefix}")
            
            uploaded_blobs = []
            
            # Upload all files in directory
            for file_path in local_dir.rglob('*'):
                if file_path.is_file():
                    # Calculate relative path
                    relative_path = file_path.relative_to(local_dir)
                    gcs_path = f"{gcs_prefix}/{relative_path}".strip('/')
                    
                    blob = self.upload_file(file_path, gcs_path)
                    uploaded_blobs.append(blob)
            
            logger.info(f"✅ Directory upload completed")
            logger.info(f"   Files uploaded: {len(uploaded_blobs)}")
            
            return uploaded_blobs
            
        except Exception as e:
            logger.error(f"❌ Directory upload failed: {e}")
            raise
    
    def list_bucket_contents(self):
        """
        List contents of the GCS bucket.
        """
        try:
            logger.info(f"📋 Listing bucket contents: {self.bucket_name}")
            
            bucket = self.client.get_bucket(self.bucket_name)
            blobs = list(bucket.list_blobs())
            
            if not blobs:
                logger.info("   Bucket is empty")
                return []
            
            logger.info(f"   Found {len(blobs)} objects:")
            for blob in blobs:
                logger.info(f"     gs://{self.bucket_name}/{blob.name} ({blob.size:,} bytes)")
            
            return blobs
            
        except Exception as e:
            logger.error(f"❌ Failed to list bucket contents: {e}")
            return []
    
    def generate_and_upload_data(self):
        """
        Generate customer transaction data and upload to GCS.
        """
        try:
            logger.info("🚀 Generating and uploading customer transaction data...")
            
            # Import and run data generation
            script_dir = Path(__file__).parent
            data_dir = script_dir.parent / "data"
            
            # Ensure data directory exists
            data_dir.mkdir(exist_ok=True)
            
            # Generate data using the existing script
            logger.info("📝 Generating customer transaction data...")
            
            sys.path.insert(0, str(script_dir))
            from generate_data import generate_customer_data, save_to_csv
            
            # Generate 1200 records
            data = generate_customer_data(num_records=1200)
            
            # Save to local file
            local_file = data_dir / "customer_transactions_1200.csv"
            save_to_csv(data, local_file)
            
            # Upload to GCS
            blob = self.upload_file(local_file, "input-data/customer_transactions_1200.csv")
            
            logger.info(f"✅ Data generation and upload completed")
            logger.info(f"   Local file: {local_file}")
            logger.info(f"   GCS location: gs://{self.bucket_name}/input-data/customer_transactions_1200.csv")
            
            return blob
            
        except Exception as e:
            logger.error(f"❌ Data generation and upload failed: {e}")
            raise

def main():
    """
    Main function to upload data to GCS.
    """
    parser = argparse.ArgumentParser(description='Upload Week 7 data to Google Cloud Storage')
    parser.add_argument('--project-id', required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--bucket-name',
                       help='GCS bucket name (default: {project-id}-week7-streaming-data)')
    parser.add_argument('--location', default='us-central1',
                       help='GCS bucket location (default: us-central1)')
    parser.add_argument('--generate-data', action='store_true',
                       help='Generate customer transaction data before upload')
    parser.add_argument('--upload-file',
                       help='Specific file to upload')
    parser.add_argument('--upload-directory',
                       help='Directory to upload recursively')
    parser.add_argument('--list-bucket', action='store_true',
                       help='List bucket contents after upload')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Authenticate with gcloud
        logger.info("🔐 Checking Google Cloud authentication...")
        result = subprocess.run(
            ['gcloud', 'auth', 'list', '--filter=status:ACTIVE', '--format=value(account)'],
            capture_output=True, text=True
        )
        
        if not result.stdout.strip():
            logger.error("❌ No active Google Cloud authentication found")
            logger.error("Please run: gcloud auth login")
            logger.error("And: gcloud auth application-default login")
            sys.exit(1)
        
        logger.info(f"✅ Authenticated as: {result.stdout.strip()}")
        
        # Set project
        subprocess.run(['gcloud', 'config', 'set', 'project', args.project_id], check=True)
        
        # Create uploader
        uploader = GCSDataUploader(
            project_id=args.project_id,
            bucket_name=args.bucket_name
        )
        
        # Generate and upload data
        if args.generate_data:
            uploader.generate_and_upload_data()
        
        # Upload specific file
        if args.upload_file:
            uploader.upload_file(args.upload_file)
        
        # Upload directory
        if args.upload_directory:
            uploader.upload_directory(args.upload_directory)
        
        # List bucket contents
        if args.list_bucket:
            uploader.list_bucket_contents()
        
        # If no specific action, generate and upload data by default
        if not any([args.generate_data, args.upload_file, args.upload_directory, args.list_bucket]):
            uploader.generate_and_upload_data()
        
        logger.info("🎉 GCS upload completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("👋 Upload interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Upload failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()