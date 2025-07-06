#!/usr/bin/env python3
"""
Deploy SCD Type II implementation to Google Cloud Dataproc
"""

import os
import sys
import subprocess
import logging
import time
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataprocDeployer:
    def __init__(self):
        self.project_id = None
        self.region = "us-central1"
        self.zone = "us-central1-b"
        self.cluster_name = "scd-type2-cluster"
        self.bucket_name = None
        self.timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
    def run_command(self, command, check=True, capture_output=True):
        """Run shell command and return result"""
        try:
            logger.info(f"Running: {command}")
            result = subprocess.run(
                command, 
                shell=True, 
                check=check,
                capture_output=capture_output,
                text=True
            )
            if capture_output:
                return result.stdout.strip()
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {command}")
            logger.error(f"Error: {e}")
            if capture_output and e.stdout:
                logger.error(f"Stdout: {e.stdout}")
            if capture_output and e.stderr:
                logger.error(f"Stderr: {e.stderr}")
            raise

    def check_gcloud_auth(self):
        """Check if gcloud is authenticated"""
        try:
            result = self.run_command("gcloud auth list --filter=status:ACTIVE --format='value(account)'")
            if result:
                logger.info("Google Cloud authentication verified")
                return True
            else:
                logger.error("No active Google Cloud authentication found")
                return False
        except Exception as e:
            logger.error(f"Error checking authentication: {e}")
            return False

    def get_project_id(self):
        """Get current project ID"""
        try:
            self.project_id = self.run_command("gcloud config get-value project")
            if not self.project_id:
                raise Exception("No project ID found")
            logger.info(f"Using Google Cloud project: {self.project_id}")
            self.bucket_name = f"{self.project_id}-scd-type2-bucket"
            return True
        except Exception as e:
            logger.error(f"Error getting project ID: {e}")
            return False

    def enable_apis(self):
        """Enable required Google Cloud APIs"""
        apis = [
            "dataproc.googleapis.com",
            "compute.googleapis.com",
            "storage-component.googleapis.com"
        ]
        
        for api in apis:
            try:
                logger.info(f"Enabling API: {api}")
                self.run_command(f"gcloud services enable {api}")
                logger.info(f"API {api} enabled successfully")
            except Exception as e:
                logger.error(f"Error enabling API {api}: {e}")
                return False
        return True

    def create_bucket(self):
        """Create GCS bucket for storing data and scripts"""
        try:
            # Check if bucket exists
            try:
                self.run_command(f"gcloud storage buckets describe gs://{self.bucket_name}")
                logger.info(f"Bucket gs://{self.bucket_name} already exists")
                return True
            except:
                pass
            
            # Create bucket
            logger.info(f"Creating GCS bucket: gs://{self.bucket_name}")
            self.run_command(f"gcloud storage buckets create gs://{self.bucket_name} --location={self.region}")
            logger.info(f"Bucket gs://{self.bucket_name} created successfully")
            return True
        except Exception as e:
            logger.error(f"Error with GCS bucket: {e}")
            return False

    def upload_files(self):
        """Upload data files and scripts to GCS"""
        try:
            # Upload data files
            logger.info("Uploading data files to GCS")
            self.run_command(f"gcloud storage cp data/customer_existing.csv gs://{self.bucket_name}/data/")
            self.run_command(f"gcloud storage cp data/customer_new.csv gs://{self.bucket_name}/data/")
            
            # Upload main script
            logger.info("Uploading PySpark script to GCS")
            self.run_command(f"gcloud storage cp scd_type2_implementation.py gs://{self.bucket_name}/scripts/")
            
            logger.info("Files uploaded successfully")
            return True
        except Exception as e:
            logger.error(f"Error uploading files: {e}")
            return False

    def create_cluster(self):
        """Create Dataproc cluster"""
        try:
            # Check if cluster exists
            try:
                result = self.run_command(
                    f"gcloud dataproc clusters describe {self.cluster_name} --region={self.region}"
                )
                logger.info(f"Cluster {self.cluster_name} already exists")
                return True
            except:
                pass
            
            # Create cluster with minimal resources to fit quota
            logger.info(f"Creating Dataproc cluster: {self.cluster_name}")
            create_cmd = f"""
            gcloud dataproc clusters create {self.cluster_name} \
                --region={self.region} \
                --zone={self.zone} \
                --master-machine-type=e2-standard-2 \
                --worker-machine-type=e2-standard-2 \
                --num-workers=2 \
                --master-boot-disk-size=50GB \
                --worker-boot-disk-size=50GB \
                --image-version=2.0-debian10 \
                --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
                --metadata=PIP_PACKAGES=pandas \
                --max-idle=10m
            """
            
            self.run_command(create_cmd.replace('\n', ' ').replace('\\', ''))
            logger.info(f"Cluster {self.cluster_name} created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating cluster: {e}")
            return False

    def submit_job(self):
        """Submit PySpark job to Dataproc cluster"""
        try:
            job_id = f"scd-type2-job-{self.timestamp}"
            logger.info(f"Submitting PySpark job: {job_id}")
            
            submit_cmd = f"""
            gcloud dataproc jobs submit pyspark \
                gs://{self.bucket_name}/scripts/scd_type2_implementation.py \
                --cluster={self.cluster_name} \
                --region={self.region} \
                --py-files=gs://{self.bucket_name}/scripts/scd_type2_implementation.py \
                --properties=spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true \
                -- \
                --existing_data_path=gs://{self.bucket_name}/data/customer_existing.csv \
                --new_data_path=gs://{self.bucket_name}/data/customer_new.csv \
                --output_path=gs://{self.bucket_name}/output/customer_dimension_{self.timestamp}
            """
            
            result = self.run_command(submit_cmd.replace('\n', ' ').replace('\\', ''), capture_output=False)
            logger.info(f"Job submitted successfully")
            return job_id
        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            return None

    def monitor_job(self, job_id):
        """Monitor job status"""
        try:
            logger.info(f"Monitoring job: {job_id}")
            
            while True:
                status = self.run_command(
                    f"gcloud dataproc jobs describe {job_id} --region={self.region} --format='value(status.state)'"
                )
                
                logger.info(f"Job status: {status}")
                
                if status in ['DONE', 'ERROR', 'CANCELLED']:
                    break
                
                time.sleep(30)
            
            if status == 'DONE':
                logger.info("Job completed successfully!")
                return True
            else:
                logger.error(f"Job failed with status: {status}")
                return False
                
        except Exception as e:
            logger.error(f"Error monitoring job: {e}")
            return False

    def get_job_output(self, job_id):
        """Get job output and logs"""
        try:
            logger.info("Retrieving job output...")
            
            # Get job logs
            logs = self.run_command(
                f"gcloud dataproc jobs describe {job_id} --region={self.region} --format='value(driverOutputResourceUri)'"
            )
            
            if logs:
                logger.info(f"Job logs available at: {logs}")
            
            # List output files
            logger.info("Listing output files...")
            output_files = self.run_command(
                f"gcloud storage ls gs://{self.bucket_name}/output/"
            )
            
            logger.info("Output files:")
            print(output_files)
            
            return True
        except Exception as e:
            logger.error(f"Error retrieving job output: {e}")
            return False

    def cleanup(self):
        """Clean up resources"""
        try:
            logger.info("Cleaning up resources...")
            
            # Delete cluster
            logger.info(f"Deleting cluster: {self.cluster_name}")
            self.run_command(
                f"gcloud dataproc clusters delete {self.cluster_name} --region={self.region} --quiet"
            )
            
            logger.info("Cleanup completed")
            return True
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False

    def deploy(self):
        """Main deployment function"""
        try:
            logger.info("Starting SCD Type II Dataproc deployment")
            
            # Check prerequisites
            if not self.check_gcloud_auth():
                return False
            
            if not self.get_project_id():
                return False
            
            if not self.enable_apis():
                return False
            
            if not self.create_bucket():
                return False
            
            if not self.upload_files():
                return False
            
            if not self.create_cluster():
                return False
            
            # Submit and monitor job
            job_id = self.submit_job()
            if not job_id:
                return False
            
            if not self.monitor_job(job_id):
                return False
            
            if not self.get_job_output(job_id):
                return False
            
            logger.info("Deployment completed successfully!")
            
            # Ask about cleanup
            cleanup_choice = input("\nDo you want to clean up resources (delete cluster)? (y/n): ")
            if cleanup_choice.lower() == 'y':
                self.cleanup()
            else:
                logger.info(f"Cluster {self.cluster_name} left running. Don't forget to delete it later!")
            
            return True
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            return False

def main():
    """Main function"""
    deployer = DataprocDeployer()
    
    if deployer.deploy():
        logger.info("✅ SCD Type II deployment successful!")
        print("\n" + "="*50)
        print("DEPLOYMENT SUMMARY")
        print("="*50)
        print(f"Project ID: {deployer.project_id}")
        print(f"Region: {deployer.region}")
        print(f"Cluster: {deployer.cluster_name}")
        print(f"Bucket: gs://{deployer.bucket_name}")
        print(f"Output: gs://{deployer.bucket_name}/output/")
        print("="*50)
        sys.exit(0)
    else:
        logger.error("❌ SCD Type II deployment failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 