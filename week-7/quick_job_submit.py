#!/usr/bin/env python3
"""
Quick Job Submission for Week 7 Pipeline
Submit producer and consumer jobs to existing cluster
"""

import subprocess
import time
import sys

def run_command(cmd, description):
    print(f"\n🔧 {description}")
    print(f"Command: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, text=True, timeout=300)
        if result.returncode == 0:
            print(f"✅ Success")
            return True
        else:
            print(f"❌ Failed with exit code {result.returncode}")
            return False
    except Exception as e:
        print(f"💥 Error: {e}")
        return False

def main():
    # Use your current project from the debug output
    project_id = "steady-triumph-447006-f8"
    cluster_name = "week7-streaming-cluster"
    region = "us-central1"
    bucket_name = f"{project_id}-week7-streaming-data"
    
    print("🚀 QUICK JOB SUBMISSION FOR WEEK 7 PIPELINE")
    print("=" * 60)
    print(f"Project: {project_id}")
    print(f"Cluster: {cluster_name}")
    print(f"Bucket: {bucket_name}")
    
    # First, let's try to create the bucket and upload required files
    print(f"\n📦 Setting up bucket and data...")
    
    # Create bucket if it doesn't exist
    run_command(
        f"gcloud storage buckets create gs://{bucket_name} --location=us-central1 --project={project_id} || echo 'Bucket may already exist'",
        "Create GCS bucket"
    )
    
    # Try to find and upload data file
    data_files = [
        "data/customer_transactions_1200.csv",
        "../week-6/data/customer_transactions_1200.csv",
        "customer_transactions_1200.csv"
    ]
    
    data_uploaded = False
    for data_file in data_files:
        if run_command(f"ls {data_file} && gcloud storage cp {data_file} gs://{bucket_name}/input-data/", f"Upload {data_file}"):
            data_uploaded = True
            break
    
    if not data_uploaded:
        print("❌ Could not find customer_transactions_1200.csv data file")
        print("Please ensure the data file exists and run upload script first")
        return
    
    # Upload application files
    app_files = [
        ("producer/kafka_producer.py", "apps/kafka_producer.py"),
        ("consumer/spark_streaming_consumer.py", "apps/spark_streaming_consumer.py"),
        ("requirements.txt", "apps/requirements.txt")
    ]
    
    for local_file, gcs_path in app_files:
        run_command(
            f"gcloud storage cp {local_file} gs://{bucket_name}/{gcs_path}",
            f"Upload {local_file}"
        )
    
    # Submit consumer job FIRST (so it's ready to receive data)
    print(f"\n📊 Submitting consumer job...")
    consumer_cmd = f"""
    gcloud dataproc jobs submit pyspark \\
        gs://{bucket_name}/apps/spark_streaming_consumer.py \\
        --cluster={cluster_name} \\
        --region={region} \\
        --properties="spark.executor.memory=1g,spark.driver.memory=512m,spark.executor.instances=2,spark.executor.cores=1,spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true" \\
        -- \\
        --kafka-servers=localhost:9092 \\
        --topic=customer-transactions \\
        --window-duration=10 \\
        --slide-duration=5
    """
    
    run_command(consumer_cmd, "Submit Spark Streaming consumer")
    
    # Wait for consumer to start
    print(f"\n⏰ Waiting 30 seconds for consumer to initialize...")
    time.sleep(30)
    
    # Submit producer job
    print(f"\n📤 Submitting producer job...")
    producer_cmd = f"""
    gcloud dataproc jobs submit pyspark \\
        gs://{bucket_name}/apps/kafka_producer.py \\
        --cluster={cluster_name} \\
        --region={region} \\
        --properties="spark.executor.memory=1g,spark.driver.memory=512m,spark.executor.instances=2,spark.executor.cores=1" \\
        -- \\
        --data-file=gs://{bucket_name}/input-data/customer_transactions_1200.csv \\
        --topic=customer-transactions \\
        --batch-size=10 \\
        --sleep-seconds=10 \\
        --max-records=100
    """
    
    run_command(producer_cmd, "Submit Kafka producer")
    
    print(f"\n✅ Jobs submitted! Monitor with:")
    print(f"gcloud dataproc jobs list --cluster={cluster_name} --region={region}")
    print(f"\nView job logs with:")
    print(f"gcloud dataproc jobs describe <JOB_ID> --region={region}")

if __name__ == "__main__":
    main()