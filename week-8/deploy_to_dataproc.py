#!/usr/bin/env python3
"""
Deploy Decision Tree CrossValidator to Google Cloud Dataproc
Course: Introduction to Big Data - Week 8
Author: Abhyudaya B Tharakan 22f3001492
Date: August 2025
"""

import os
import sys
import time
import subprocess
import json
from pathlib import Path

# Configuration
PROJECT_ID = "spheric-gecko-467914-f8"
REGION = "us-central1"
CLUSTER_NAME = "week8-ml-cluster"
BUCKET_NAME = f"{PROJECT_ID}-week8-ml"
ZONE = "us-central1-b"

def run_command(cmd, description=""):
    """Execute a shell command."""
    print(f"\n{'='*60}")
    if description:
        print(f"📋 {description}")
    print(f"🔧 Running: {cmd}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error: {result.stderr}")
        return False
    else:
        if result.stdout:
            print(result.stdout)
        print(f"✅ Success")
        return True

def create_bucket():
    """Create GCS bucket for storing scripts and data."""
    print("\n🪣 Creating GCS bucket...")
    
    # Check if bucket exists
    check_cmd = f"gcloud storage ls gs://{BUCKET_NAME} 2>/dev/null"
    result = subprocess.run(check_cmd, shell=True, capture_output=True)
    
    if result.returncode == 0:
        print(f"✅ Bucket gs://{BUCKET_NAME} already exists")
    else:
        # Create bucket
        create_cmd = f"gcloud storage buckets create gs://{BUCKET_NAME} --location={REGION}"
        if not run_command(create_cmd, "Creating GCS bucket"):
            print("⚠️ Failed to create bucket, but continuing...")

def upload_files():
    """Upload scripts and data to GCS."""
    print("\n📤 Uploading files to GCS...")
    
    # Upload main script
    src_file = "src/decision_trees_crossvalidator.py"
    if os.path.exists(src_file):
        upload_cmd = f"gcloud storage cp {src_file} gs://{BUCKET_NAME}/scripts/"
        run_command(upload_cmd, f"Uploading {src_file}")
    
    # Upload any data files if they exist
    data_dir = Path("data")
    if data_dir.exists():
        for data_file in data_dir.glob("*"):
            upload_cmd = f"gcloud storage cp {data_file} gs://{BUCKET_NAME}/data/"
            run_command(upload_cmd, f"Uploading {data_file.name}")
    
    return True

def create_cluster():
    """Create Dataproc cluster for running Spark jobs."""
    print("\n🔧 Creating Dataproc cluster...")
    
    # Check if cluster exists
    check_cmd = f"gcloud dataproc clusters describe {CLUSTER_NAME} --region={REGION} --format=json 2>/dev/null"
    result = subprocess.run(check_cmd, shell=True, capture_output=True)
    
    if result.returncode == 0:
        print(f"✅ Cluster {CLUSTER_NAME} already exists")
        return True
    
    # Create cluster with appropriate configuration for ML workloads
    create_cmd = f"""
    gcloud dataproc clusters create {CLUSTER_NAME} \
        --region={REGION} \
        --zone={ZONE} \
        --master-machine-type=n1-standard-2 \
        --master-boot-disk-size=50GB \
        --num-workers=2 \
        --worker-machine-type=n1-standard-2 \
        --worker-boot-disk-size=50GB \
        --image-version=2.1-debian11 \
        --properties="spark:spark.executor.memory=2g,spark:spark.driver.memory=2g" \
        --initialization-actions="" \
        --max-idle=30m \
        --max-age=3h
    """
    
    return run_command(create_cmd, "Creating Dataproc cluster")

def submit_job():
    """Submit PySpark job to Dataproc."""
    print("\n🚀 Submitting PySpark job to Dataproc...")
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = f"gs://{BUCKET_NAME}/output/results_{timestamp}"
    
    # Submit job with parameters
    submit_cmd = f"""
    gcloud dataproc jobs submit pyspark \
        gs://{BUCKET_NAME}/scripts/decision_trees_crossvalidator.py \
        --cluster={CLUSTER_NAME} \
        --region={REGION} \
        --properties="spark.executor.memory=2g,spark.driver.memory=2g" \
        -- \
        --cv-folds 3 \
        --output-report {output_path}/analysis_report.txt
    """
    
    if run_command(submit_cmd, "Submitting ML job to Dataproc"):
        print(f"\n📊 Job completed! Results saved to: {output_path}")
        
        # Download results
        download_cmd = f"gcloud storage cp {output_path}/analysis_report.txt ./cloud_results_{timestamp}.txt"
        if run_command(download_cmd, "Downloading results"):
            print(f"✅ Results downloaded to: cloud_results_{timestamp}.txt")
            
            # Display results
            with open(f"cloud_results_{timestamp}.txt", "r") as f:
                print("\n" + "="*60)
                print("📈 ANALYSIS RESULTS:")
                print("="*60)
                print(f.read())
    
    return True

def cleanup_cluster():
    """Delete Dataproc cluster to avoid charges."""
    print("\n🧹 Cleaning up resources...")
    
    delete_cmd = f"gcloud dataproc clusters delete {CLUSTER_NAME} --region={REGION} --quiet"
    return run_command(delete_cmd, "Deleting Dataproc cluster")

def main():
    """Main deployment workflow."""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║      Week 8: Decision Tree CrossValidator Deployment        ║
    ║                    Google Cloud Dataproc                    ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    print(f"📍 Project ID: {PROJECT_ID}")
    print(f"📍 Region: {REGION}")
    print(f"📍 Cluster: {CLUSTER_NAME}")
    print(f"📍 Bucket: gs://{BUCKET_NAME}")
    
    try:
        # Step 1: Create bucket
        create_bucket()
        
        # Step 2: Upload files
        if not upload_files():
            print("❌ Failed to upload files")
            return 1
        
        # Step 3: Create cluster
        if not create_cluster():
            print("❌ Failed to create cluster")
            return 1
        
        # Wait for cluster to be ready
        print("\n⏳ Waiting for cluster to be ready...")
        time.sleep(10)
        
        # Step 4: Submit job
        if not submit_job():
            print("❌ Job submission failed")
            return 1
        
        print("\n" + "="*60)
        print("🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        # Step 5: Cleanup (optional)
        response = input("\n🗑️ Delete cluster to save costs? (y/n): ")
        if response.lower() == 'y':
            cleanup_cluster()
        else:
            print(f"⚠️ Remember to delete cluster manually: {CLUSTER_NAME}")
            print(f"   Command: gcloud dataproc clusters delete {CLUSTER_NAME} --region={REGION}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Deployment interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Deployment failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())