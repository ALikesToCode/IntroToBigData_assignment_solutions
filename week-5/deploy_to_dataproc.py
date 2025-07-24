#!/usr/bin/env python3
"""
Deploy SparkSQL SCD Type II implementation to Google Cloud Dataproc
"""

import os
import sys
import subprocess
import logging
import time
import re
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
        self.cluster_name = "spark-click-analysis-cluster"
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
            self.bucket_name = f"{self.project_id}-sparksql-scd-bucket"
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
            
            # Upload main SparkSQL script
            logger.info("Uploading SparkSQL script to GCS")
            self.run_command(f"gcloud storage cp sparksql_scd_type2_implementation.py gs://{self.bucket_name}/scripts/")
            
            logger.info("Files uploaded successfully")
            return True
        except Exception as e:
            logger.error(f"Error uploading files: {e}")
            return False

    def verify_existing_cluster(self):
        """Verify existing Dataproc cluster is available"""
        try:
            logger.info(f"Verifying existing cluster: {self.cluster_name}")
            
            # Check if cluster exists and get its status
            try:
                status = self.run_command(
                    f"gcloud dataproc clusters describe {self.cluster_name} --region={self.region} --format='value(status.state)'"
                )
                
                if status == "RUNNING":
                    logger.info(f"✅ Existing cluster {self.cluster_name} is running and ready")
                    return True
                elif status in ["CREATING", "STARTING"]:
                    logger.info(f"Cluster {self.cluster_name} is {status.lower()}, waiting for it to be ready...")
                    return self.wait_for_cluster_ready()
                else:
                    logger.warning(f"Cluster {self.cluster_name} is in state: {status}")
                    logger.info("Attempting to use cluster anyway...")
                    return True
                    
            except subprocess.CalledProcessError as e:
                if "NOT_FOUND" in str(e) or "does not exist" in str(e):
                    logger.error(f"❌ Cluster {self.cluster_name} not found!")
                    logger.error("Please make sure the cluster exists and is in the correct region.")
                    return False
                else:
                    logger.error(f"Error checking cluster status: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error verifying cluster: {e}")
            return False
            
    def wait_for_cluster_ready(self, max_wait_minutes=10):
        """Wait for cluster to be in RUNNING state"""
        max_attempts = max_wait_minutes * 2  # Check every 30 seconds
        
        for attempt in range(max_attempts):
            try:
                status = self.run_command(
                    f"gcloud dataproc clusters describe {self.cluster_name} --region={self.region} --format='value(status.state)'"
                )
                
                if status == "RUNNING":
                    logger.info("✅ Cluster is now ready")
                    return True
                else:
                    logger.info(f"Cluster state: {status} - waiting...")
                    time.sleep(30)
                    
            except Exception as e:
                logger.warning(f"Error checking cluster state: {e}")
                time.sleep(30)
        
        logger.warning(f"Cluster did not become ready within {max_wait_minutes} minutes, proceeding anyway...")
        return True  # Proceed even if we can't confirm it's ready

    def submit_job(self):
        """Submit SparkSQL PySpark job to Dataproc cluster"""
        try:
            logger.info(f"Submitting SparkSQL PySpark job with timestamp: {self.timestamp}")
            
            submit_cmd = f"""
            gcloud dataproc jobs submit pyspark \
                gs://{self.bucket_name}/scripts/sparksql_scd_type2_implementation.py \
                --cluster={self.cluster_name} \
                --region={self.region} \
                --py-files=gs://{self.bucket_name}/scripts/sparksql_scd_type2_implementation.py \
                --properties=spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true \
                -- \
                --existing_data_path=gs://{self.bucket_name}/data/customer_existing.csv \
                --new_data_path=gs://{self.bucket_name}/data/customer_new.csv \
                --output_path=gs://{self.bucket_name}/output/customer_dimension_{self.timestamp}
            """
            
            # Capture the output to extract the real job ID
            result = self.run_command(submit_cmd.replace('\n', ' ').replace('\\', ''), capture_output=True)
            
            # Debug: Show partial output for troubleshooting
            logger.debug(f"Job submit output (first 500 chars): {result[:500]}")
            
            # Extract job ID from YAML output format "jobId: abc123..."
            job_id_match = re.search(r'jobId:\s*([a-f0-9]+)', result)
            if job_id_match:
                actual_job_id = job_id_match.group(1)
                logger.info(f"Job submitted successfully with ID: {actual_job_id}")
                self.actual_job_id = actual_job_id  # Store for later use
                return actual_job_id
            else:
                # Try alternative pattern in case format changes
                job_id_match = re.search(r'Job \[([a-f0-9]+)\] submitted', result)
                if job_id_match:
                    actual_job_id = job_id_match.group(1)
                    logger.info(f"Job submitted successfully with ID: {actual_job_id}")
                    self.actual_job_id = actual_job_id
                    return actual_job_id
                else:
                    # Final fallback - extract any 32-character hex string that looks like a job ID
                    job_id_match = re.search(r'([a-f0-9]{32})', result)
                    if job_id_match:
                        actual_job_id = job_id_match.group(1)
                        logger.info(f"Job submitted successfully with ID (fallback): {actual_job_id}")
                        self.actual_job_id = actual_job_id
                        return actual_job_id
                    else:
                        logger.error(f"Could not extract job ID from output: {result}")
                        logger.info("Attempting to find recent job as fallback...")
                        
                        # Try to find recently submitted job for this cluster
                        try:
                            recent_jobs = self.run_command(
                                f"gcloud dataproc jobs list --region={self.region} --cluster={self.cluster_name} --limit=1 --format='value(reference.jobId)'"
                            )
                            if recent_jobs.strip():
                                fallback_job_id = recent_jobs.strip()
                                logger.info(f"Found recent job ID as fallback: {fallback_job_id}")
                                self.actual_job_id = fallback_job_id
                                return fallback_job_id
                        except Exception as e:
                            logger.warning(f"Could not find fallback job: {e}")
                        
                        return None
                
        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            return None

    def monitor_job(self, job_id):
        """Monitor job status"""
        try:
            logger.info(f"Monitoring job: {job_id}")
            
            # Check initial status to see if job completed quickly
            try:
                status = self.run_command(
                    f"gcloud dataproc jobs describe {job_id} --region={self.region} --format='value(status.state)'"
                )
                logger.info(f"Initial job status: {status}")
                
                if status == 'DONE':
                    logger.info("Job completed successfully (completed quickly)!")
                    return True
                elif status in ['ERROR', 'CANCELLED']:
                    logger.error(f"Job failed with status: {status}")
                    return False
                    
            except Exception as e:
                logger.warning(f"Could not get initial job status: {e}")
                # Continue with monitoring loop
            
            # Monitor job progress
            while True:
                try:
                    status = self.run_command(
                        f"gcloud dataproc jobs describe {job_id} --region={self.region} --format='value(status.state)'"
                    )
                    
                    logger.info(f"Job status: {status}")
                    
                    if status in ['DONE', 'ERROR', 'CANCELLED']:
                        break
                    
                    time.sleep(30)
                    
                except Exception as e:
                    logger.warning(f"Error checking job status: {e}")
                    time.sleep(30)
                    continue
            
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
            logger.info(f"Retrieving job output for job ID: {job_id}")
            
            # Get job details first
            try:
                job_status = self.run_command(
                    f"gcloud dataproc jobs describe {job_id} --region={self.region} --format='value(status.state)'"
                )
                logger.info(f"Final job status: {job_status}")
            except Exception as e:
                logger.warning(f"Could not get job status: {e}")
            
            # Get job logs
            try:
                logs = self.run_command(
                    f"gcloud dataproc jobs describe {job_id} --region={self.region} --format='value(driverOutputResourceUri)'"
                )
                if logs:
                    logger.info(f"Job logs available at: {logs}")
            except Exception as e:
                logger.warning(f"Could not get job logs: {e}")
            
            # List output files
            logger.info("Listing output files...")
            try:
                output_files = self.run_command(
                    f"gcloud storage ls gs://{self.bucket_name}/output/"
                )
                logger.info("Output files:")
                print(output_files)
                
                # Try to get specific output for this run
                specific_output = f"gs://{self.bucket_name}/output/customer_dimension_{self.timestamp}/"
                try:
                    specific_files = self.run_command(f"gcloud storage ls {specific_output}")
                    logger.info(f"Files for this run ({self.timestamp}):")
                    print(specific_files)
                except:
                    logger.info(f"No specific output found at {specific_output}")
                    
            except Exception as e:
                logger.warning(f"Could not list output files: {e}")
            
            return True
        except Exception as e:
            logger.error(f"Error retrieving job output: {e}")
            return False

    def download_and_verify_results(self):
        """Download and verify a sample of the results"""
        try:
            logger.info("Downloading and verifying results...")
            
            # Try to download the first part file from the output
            output_base = f"gs://{self.bucket_name}/output/customer_dimension_{self.timestamp}/"
            local_file = f"sparksql_results_{self.timestamp}.csv"
            
            try:
                # List files in the output directory
                files = self.run_command(f"gcloud storage ls {output_base}")
                
                # Find the first CSV part file
                for line in files.split('\n'):
                    if line.strip().endswith('.csv') and 'part-' in line:
                        part_file = line.strip()
                        logger.info(f"Downloading result file: {part_file}")
                        
                        # Download the file
                        self.run_command(f"gcloud storage cp {part_file} {local_file}")
                        
                        # Show first few lines
                        logger.info("Sample of results:")
                        result = self.run_command(f"head -10 {local_file}")
                        print(result)
                        
                        # Count rows
                        row_count = self.run_command(f"wc -l {local_file}")
                        logger.info(f"Result file contains: {row_count}")
                        
                        logger.info(f"Results downloaded to: {local_file}")
                        return True
                        
                logger.warning("No CSV part files found in output")
                return False
                        
            except Exception as e:
                logger.warning(f"Could not download results: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error downloading and verifying results: {e}")
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
            logger.info("Starting SparkSQL SCD Type II Dataproc deployment")
            
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
            
            if not self.verify_existing_cluster():
                return False
            
            # Submit and monitor job
            job_id = self.submit_job()
            if not job_id:
                return False
            
            # Store job_id for final summary
            self.actual_job_id = job_id
            
            if not self.monitor_job(job_id):
                return False
            
            if not self.get_job_output(job_id):
                return False
            
            # Download and verify results
            if not self.download_and_verify_results():
                logger.warning("Could not download results, but job completed successfully")
            
            logger.info("Deployment completed successfully!")
            logger.info(f"Used existing cluster: {self.cluster_name} (left running)")
            
            return True
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            return False

def main():
    """Main function"""
    deployer = DataprocDeployer()
    
    if deployer.deploy():
        logger.info("✅ SparkSQL SCD Type II deployment successful!")
        print("\n" + "="*50)
        print("SPARKSQL SCD TYPE II DEPLOYMENT SUMMARY")
        print("="*50)
        print(f"Project ID: {deployer.project_id}")
        print(f"Region: {deployer.region}")
        print(f"Cluster: {deployer.cluster_name}")
        print(f"Job ID: {getattr(deployer, 'actual_job_id', 'N/A')}")
        print(f"Timestamp: {deployer.timestamp}")
        print(f"Bucket: gs://{deployer.bucket_name}")
        print(f"Output: gs://{deployer.bucket_name}/output/customer_dimension_{deployer.timestamp}/")
        print("="*50)
        sys.exit(0)
    else:
        logger.error("❌ SparkSQL SCD Type II deployment failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
