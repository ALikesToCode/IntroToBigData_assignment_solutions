#!/usr/bin/env python3
"""
GCP Deployment Script for PyTorch Streaming Image Classifier
Direct conversion of Lecture 10 batch notebook to streaming on GCP
Course: Introduction to Big Data - Week 9
Author: Abhyudaya B Tharakan 22f3001492

This script deploys the PyTorch-based streaming classifier on Google Cloud Platform
maintaining the same model and approach as the original notebook.
"""

import os
import sys
import json
import subprocess
import argparse
from pathlib import Path
from typing import Dict, Any


# GCP Configuration
GCP_CONFIG = {
    "project_id": "steady-triumph-447006-f8",
    "region": "asia-south1",
    "cluster_name": "week9-pytorch-streaming",
    "bucket_name": "week9-pytorch-streaming",
    "dataset_bucket": "flower-photos-dataset",
    "pubsub_topic": "flower-images-topic",
    "pubsub_subscription": "flower-images-sub",
    "bigquery_dataset": "pytorch_streaming",
    "bigquery_table": "flower_predictions"
}


class PyTorchStreamingDeployer:
    """Deploy PyTorch streaming image classifier on GCP."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.project_id = config["project_id"]
        self.region = config["region"]
        
        print("=" * 80)
        print("PyTorch Streaming Image Classifier - GCP Deployment")
        print("Direct Conversion of Lecture 10 Notebook to Streaming")
        print("=" * 80)
        print(f"Project: {self.project_id}")
        print(f"Region: {self.region}")
        print(f"Cluster: {config['cluster_name']}")
    
    def run_command(self, cmd: str, description: str = "") -> bool:
        """Execute shell command with logging."""
        if description:
            print(f"\n📋 {description}")
        print(f"🔧 Executing: {cmd[:100]}...")  # Show first 100 chars
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error: {result.stderr}")
            return False
        else:
            if result.stdout.strip():
                print(result.stdout[:500])  # Show first 500 chars of output
            print("✅ Success")
            return True
    
    def setup_gcp_apis(self) -> bool:
        """Enable required GCP APIs."""
        print("\n🔧 Enabling GCP APIs...")
        
        apis = [
            "compute.googleapis.com",
            "dataproc.googleapis.com",
            "storage.googleapis.com",
            "pubsub.googleapis.com",
            "bigquery.googleapis.com"
        ]
        
        for api in apis:
            cmd = f"gcloud services enable {api} --project={self.project_id}"
            self.run_command(cmd, f"Enabling {api}")
        
        return True
    
    def create_storage_buckets(self) -> bool:
        """Create GCS buckets for data and checkpoints."""
        buckets = [
            f"{self.project_id}-{self.config['bucket_name']}",
            f"{self.project_id}-{self.config['dataset_bucket']}"
        ]
        
        for bucket in buckets:
            print(f"\n🪣 Creating bucket: gs://{bucket}")
            
            # Check if exists
            check_cmd = f"gcloud storage ls gs://{bucket} 2>/dev/null"
            if subprocess.run(check_cmd, shell=True, capture_output=True).returncode == 0:
                print(f"✅ Bucket already exists: gs://{bucket}")
                continue
            
            # Create bucket
            create_cmd = f"gcloud storage buckets create gs://{bucket} --location={self.region}"
            self.run_command(create_cmd)
        
        # Create directory structure
        main_bucket = f"{self.project_id}-{self.config['bucket_name']}"
        directories = [
            "checkpoints/",
            "scripts/",
            "outputs/",
            "models/",
            "streaming-input/"
        ]
        
        for directory in directories:
            cmd = f"echo '' | gcloud storage cp - gs://{main_bucket}/{directory}.keep"
            self.run_command(cmd, f"Creating {directory}")
        
        return True
    
    def download_flower_dataset(self) -> bool:
        """Download and upload flower photos dataset to GCS."""
        print("\n🌸 Setting up Flower Photos Dataset...")
        
        dataset_bucket = f"{self.project_id}-{self.config['dataset_bucket']}"
        
        # Check if dataset already exists
        check_cmd = f"gcloud storage ls gs://{dataset_bucket}/flower_photos/ 2>/dev/null"
        if subprocess.run(check_cmd, shell=True, capture_output=True).returncode == 0:
            print("✅ Flower dataset already exists in GCS")
            return True
        
        # Download locally first
        download_script = """
        if [ ! -d "/tmp/flower_photos" ]; then
            echo "Downloading flower photos dataset..."
            cd /tmp
            wget -q https://storage.googleapis.com/download.tensorflow.org/example_images/flower_photos.tgz
            tar -xzf flower_photos.tgz
            rm flower_photos.tgz
        fi
        """
        self.run_command(download_script, "Downloading flower dataset")
        
        # Upload to GCS
        upload_cmd = f"gcloud storage cp -r /tmp/flower_photos gs://{dataset_bucket}/"
        self.run_command(upload_cmd, "Uploading flower dataset to GCS")
        
        return True
    
    def setup_pubsub(self) -> bool:
        """Setup Pub/Sub for streaming notifications."""
        topic = self.config["pubsub_topic"]
        subscription = self.config["pubsub_subscription"]
        
        print(f"\n📢 Setting up Pub/Sub...")
        
        # Create topic
        topic_cmd = f"gcloud pubsub topics create {topic} --project={self.project_id} 2>/dev/null || true"
        self.run_command(topic_cmd, "Creating Pub/Sub topic")
        
        # Create subscription
        sub_cmd = f"""
        gcloud pubsub subscriptions create {subscription} \
            --topic={topic} \
            --project={self.project_id} \
            2>/dev/null || true
        """
        self.run_command(sub_cmd, "Creating Pub/Sub subscription")
        
        # Setup bucket notifications for streaming input
        bucket = f"{self.project_id}-{self.config['bucket_name']}"
        notification_cmd = f"""
        gcloud storage buckets notifications create \
            gs://{bucket} \
            --topic=projects/{self.project_id}/topics/{topic} \
            --event-types=OBJECT_FINALIZE \
            --prefix=streaming-input/ \
            2>/dev/null || true
        """
        self.run_command(notification_cmd, "Setting up bucket notifications")
        
        return True
    
    def setup_bigquery(self) -> bool:
        """Create BigQuery dataset and table for results."""
        dataset = self.config["bigquery_dataset"]
        table = self.config["bigquery_table"]
        
        print(f"\n📊 Setting up BigQuery...")
        
        # Create dataset
        dataset_cmd = f"""
        bq mk --location={self.region} \
            --project_id={self.project_id} \
            {dataset} 2>/dev/null || true
        """
        self.run_command(dataset_cmd, "Creating BigQuery dataset")
        
        # Create table with schema matching PyTorch output
        schema = """
        image_path:STRING,
        label:STRING,
        predicted_class:STRING,
        predicted_description:STRING,
        confidence:FLOAT64,
        processed_at:TIMESTAMP
        """
        
        table_cmd = f"""
        bq mk --table \
            --project_id={self.project_id} \
            {dataset}.{table} \
            {schema} 2>/dev/null || true
        """
        self.run_command(table_cmd, "Creating BigQuery table")
        
        return True
    
    def create_dataproc_cluster(self) -> bool:
        """Create Dataproc cluster with PyTorch and required dependencies."""
        cluster_name = self.config["cluster_name"]
        bucket = f"{self.project_id}-{self.config['bucket_name']}"
        
        print(f"\n🖥️ Creating Dataproc cluster: {cluster_name}")
        
        # Check if exists
        check_cmd = f"""
        gcloud dataproc clusters describe {cluster_name} \
            --region={self.region} \
            --project={self.project_id} \
            --format=json 2>/dev/null
        """
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Cluster {cluster_name} already exists - skipping creation")
            # Show cluster info
            cluster_info = result.stdout
            print(f"📋 Cluster status: Active")
            return True
        
        # Create initialization script for PyTorch
        init_script = '''#!/bin/bash
# Install PyTorch and dependencies with complete environment rebuild
echo "=== COMPLETELY REBUILDING PYTHON ENVIRONMENT ==="

# Set environment variables to prevent caching issues
export PYTHONNOUSERSITE=1
export PIP_NO_CACHE_DIR=1

# Create a completely fresh conda environment
echo "Creating fresh conda environment..."
/opt/conda/default/bin/conda create -n pytorch_env python=3.8 -y
source /opt/conda/default/bin/activate pytorch_env

# Update conda and pip in new environment
conda update -n pytorch_env conda -y
pip install --upgrade pip

# Install numpy and pandas first with exact compatible versions
echo "Installing numpy and pandas with exact versions..."
conda install -y numpy=1.21.6 pandas=1.3.5 -c conda-forge

# Verify core packages work
python -c "import numpy; import pandas; print('Core packages OK')"

# Install PyTorch with CPU support (compatible with our numpy version)
echo "Installing PyTorch CPU..."
pip install torch==1.12.1+cpu torchvision==0.13.1+cpu -f https://download.pytorch.org/whl/torch_stable.html

# Install other required packages
echo "Installing other dependencies..."
pip install \
    pillow==9.5.0 \
    kafka-python==2.0.2 \
    google-cloud-storage==2.10.0

# Make this the default environment for Spark
echo "Setting up environment for Spark..."
echo 'export PATH="/opt/conda/envs/pytorch_env/bin:$PATH"' >> /etc/environment
echo 'export PYSPARK_PYTHON="/opt/conda/envs/pytorch_env/bin/python"' >> /etc/environment
echo 'export PYSPARK_DRIVER_PYTHON="/opt/conda/envs/pytorch_env/bin/python"' >> /etc/environment

# Create symlinks to make the new environment default
ln -sf /opt/conda/envs/pytorch_env/bin/python /usr/local/bin/python
ln -sf /opt/conda/envs/pytorch_env/bin/python /usr/local/bin/python3
ln -sf /opt/conda/envs/pytorch_env/bin/pip /usr/local/bin/pip

# Final comprehensive test
echo "=== FINAL VERIFICATION ==="
/opt/conda/envs/pytorch_env/bin/python -c "
import sys
print('Python path:', sys.executable)
import numpy
import pandas
import torch
import torchvision
import PIL
from google.cloud import storage
print('✅ ALL PACKAGES LOADED SUCCESSFULLY!')
print(f'numpy: {numpy.__version__}')
print(f'pandas: {pandas.__version__}')  
print(f'torch: {torch.__version__}')
print(f'pillow: {PIL.__version__}')

# Test numpy/pandas compatibility specifically
import pandas as pd
df = pd.DataFrame({'test': [1, 2, 3]})
print('✅ Pandas DataFrame creation works')
"

# Update Spark configuration to use new environment
echo "spark.pyspark.python /opt/conda/envs/pytorch_env/bin/python" >> /etc/spark/conf/spark-defaults.conf
echo "spark.pyspark.driver.python /opt/conda/envs/pytorch_env/bin/python" >> /etc/spark/conf/spark-defaults.conf

# Install Kafka on master node
if [[ "$(hostname)" == *"-m" ]]; then
    cd /opt
    wget -q https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
    tar -xzf kafka_2.13-3.6.0.tgz
    ln -s kafka_2.13-3.6.0 kafka
    
    # Start Kafka services
    /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
    sleep 10
    /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
    sleep 10
    
    # Create Kafka topic
    /opt/kafka/bin/kafka-topics.sh --create \
        --topic flower-images \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        2>/dev/null || true
fi

echo "PyTorch initialization complete"
'''
        
        # Save and upload init script
        init_script_path = "/tmp/pytorch_init.sh"
        with open(init_script_path, "w") as f:
            f.write(init_script)
        
        upload_cmd = f"gcloud storage cp {init_script_path} gs://{bucket}/scripts/pytorch_init.sh"
        self.run_command(upload_cmd, "Uploading initialization script")
        
        # Create cluster with Auto Zone for optimal resource allocation
        create_cmd = f"""
        gcloud dataproc clusters create {cluster_name} \
            --project={self.project_id} \
            --region={self.region} \
            --placement-auto-zone \
            --master-machine-type=e2-standard-2 \
            --master-boot-disk-size=30GB \
            --worker-machine-type=e2-standard-2 \
            --worker-boot-disk-size=30GB \
            --num-workers=2 \
            --image-version=2.1-debian11 \
            --initialization-actions=gs://{bucket}/scripts/pytorch_init.sh \
            --initialization-action-timeout=20m \
            --optional-components=JUPYTER,ZOOKEEPER \
            --properties="spark:spark.streaming.stopGracefullyOnShutdown=true,spark:spark.sql.streaming.metricsEnabled=true" \
            --enable-component-gateway \
            --max-idle=30m \
            --max-age=4h
        """
        
        return self.run_command(create_cmd, "Creating Dataproc cluster")
    
    def upload_scripts(self) -> bool:
        """Upload Python scripts to GCS."""
        bucket = f"{self.project_id}-{self.config['bucket_name']}"
        
        print(f"\n📤 Uploading scripts...")
        
        scripts = [
            "pytorch_streaming_classifier.py",
            "flower_kafka_producer.py",
            "lecture_10_dl.py"  # Original for reference
        ]
        
        for script in scripts:
            if os.path.exists(script):
                # Check if file already exists in GCS
                check_cmd = f"gcloud storage ls gs://{bucket}/scripts/{script} 2>/dev/null"
                if subprocess.run(check_cmd, shell=True, capture_output=True).returncode == 0:
                    print(f"✅ Script already exists: {script}")
                    continue
                
                # Upload the script
                cmd = f"gcloud storage cp {script} gs://{bucket}/scripts/"
                self.run_command(cmd, f"Uploading {script}")
            else:
                print(f"⚠️  Script not found locally: {script}")
        
        return True
    
    def submit_streaming_job(self) -> bool:
        """Submit the PyTorch streaming job to Dataproc."""
        cluster_name = self.config["cluster_name"]
        bucket = f"{self.project_id}-{self.config['bucket_name']}"
        dataset_bucket = f"{self.project_id}-{self.config['dataset_bucket']}"
        
        print(f"\n🚀 Submitting PyTorch streaming job...")
        
        # Job arguments
        job_args = [
            "--source", "file",  # Start with file monitoring
            "--input-path", f"gs://{dataset_bucket}/flower_photos/",
            "--checkpoint", f"gs://{bucket}/checkpoints/pytorch-streaming",
            "--output-path", f"gs://{bucket}/outputs/predictions",
            "--trigger-seconds", "10",
            "--model", "mobilenet_v2"
        ]
        
        # Submit job
        submit_cmd = f"""
        gcloud dataproc jobs submit pyspark \
            gs://{bucket}/scripts/pytorch_streaming_classifier.py \
            --cluster={cluster_name} \
            --region={self.region} \
            --project={self.project_id} \
            --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,spark.executor.memory=2g,spark.driver.memory=2g,spark.executor.instances=2" \
            -- {' '.join(job_args)}
        """
        
        return self.run_command(submit_cmd, "Submitting streaming job")
    
    def submit_kafka_streaming_job(self) -> bool:
        """Submit Kafka-based streaming job."""
        cluster_name = self.config["cluster_name"]
        bucket = f"{self.project_id}-{self.config['bucket_name']}"
        
        print(f"\n🚀 Submitting Kafka streaming job...")
        
        # Get master node internal IP using cluster describe (no zone needed)
        get_ip_cmd = f"""
        gcloud dataproc clusters describe {cluster_name} \
            --region={self.region} \
            --project={self.project_id} \
            --format='get(config.masterConfig.instanceReferences[0].privateIpAddress)'
        """
        result = subprocess.run(get_ip_cmd, shell=True, capture_output=True, text=True)
        master_ip = result.stdout.strip()
        
        job_args = [
            "--source", "kafka",
            "--kafka-servers", f"{master_ip}:9092",
            "--kafka-topic", "flower-images",
            "--checkpoint", f"gs://{bucket}/checkpoints/pytorch-kafka",
            "--output-path", f"gs://{bucket}/outputs/kafka-predictions",
            "--trigger-seconds", "5",
            "--model", "mobilenet_v2"
        ]
        
        submit_cmd = f"""
        gcloud dataproc jobs submit pyspark \
            gs://{bucket}/scripts/pytorch_streaming_classifier.py \
            --cluster={cluster_name} \
            --region={self.region} \
            --project={self.project_id} \
            --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,spark.executor.memory=2g,spark.driver.memory=2g" \
            -- {' '.join(job_args)}
        """
        
        return self.run_command(submit_cmd, "Submitting Kafka streaming job")
    
    def print_access_info(self) -> bool:
        """Print access information and monitoring commands."""
        cluster_name = self.config["cluster_name"]
        bucket = f"{self.project_id}-{self.config['bucket_name']}"
        dataset = self.config["bigquery_dataset"]
        table = self.config["bigquery_table"]
        
        print("\n" + "=" * 80)
        print("🎉 DEPLOYMENT COMPLETE!")
        print("=" * 80)
        
        print("\n📊 Monitor Results:")
        print(f"  BigQuery: {self.project_id}.{dataset}.{table}")
        print(f"  GCS Output: gs://{bucket}/outputs/")
        
        print("\n🖥️ Access Spark UI:")
        print(f"  gcloud compute ssh {cluster_name}-m --project={self.project_id} -- -L 4040:localhost:4040")
        print(f"  Then browse to: http://localhost:4040")
        
        print("\n📤 Stream New Images:")
        print(f"  Copy images to: gs://{bucket}/streaming-input/")
        print(f"  gcloud storage cp image.jpg gs://{bucket}/streaming-input/")
        
        print("\n🔍 Check Job Status:")
        print(f"  gcloud dataproc jobs list --region={self.region} --cluster={cluster_name}")
        
        print("\n🧹 Cleanup (when done):")
        print(f"  python {__file__} --cleanup")
        
        return True
    
    def cleanup(self) -> bool:
        """Clean up all resources."""
        print("\n🧹 Cleaning up resources...")
        
        cluster_name = self.config["cluster_name"]
        
        # Delete cluster
        cmd = f"""
        gcloud dataproc clusters delete {cluster_name} \
            --region={self.region} \
            --project={self.project_id} \
            --quiet
        """
        self.run_command(cmd, "Deleting Dataproc cluster")
        
        # Delete Pub/Sub resources
        self.run_command(
            f"gcloud pubsub subscriptions delete {self.config['pubsub_subscription']} --quiet",
            "Deleting subscription"
        )
        self.run_command(
            f"gcloud pubsub topics delete {self.config['pubsub_topic']} --quiet",
            "Deleting topic"
        )
        
        print("✅ Cleanup complete")
        return True
    
    def deploy_all(self) -> bool:
        """Run complete deployment."""
        steps = [
            ("Setting up GCP APIs", self.setup_gcp_apis),
            ("Creating storage buckets", self.create_storage_buckets),
            ("Downloading flower dataset", self.download_flower_dataset),
            ("Setting up Pub/Sub", self.setup_pubsub),
            ("Setting up BigQuery", self.setup_bigquery),
            ("Creating Dataproc cluster", self.create_dataproc_cluster),
            ("Uploading scripts", self.upload_scripts),
            ("Submitting streaming job", self.submit_streaming_job),
            ("Printing access info", self.print_access_info)
        ]
        
        for step_name, step_func in steps:
            print(f"\n{'='*80}")
            print(f"Step: {step_name}")
            print(f"{'='*80}")
            
            if not step_func():
                print(f"❌ Failed at: {step_name}")
                return False
        
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Deploy PyTorch Streaming Classifier on GCP"
    )
    parser.add_argument("--project-id", help="GCP Project ID")
    parser.add_argument("--region", default="asia-south1", help="GCP Region")
    parser.add_argument("--cleanup", action="store_true", help="Cleanup resources")
    parser.add_argument("--kafka-job", action="store_true", help="Submit Kafka streaming job")
    
    args = parser.parse_args()
    
    # Update config
    config = GCP_CONFIG.copy()
    if args.project_id:
        config["project_id"] = args.project_id
    if args.region:
        config["region"] = args.region
    
    # Initialize deployer
    deployer = PyTorchStreamingDeployer(config)
    
    try:
        if args.cleanup:
            deployer.cleanup()
        elif args.kafka_job:
            deployer.submit_kafka_streaming_job()
        else:
            deployer.deploy_all()
    except KeyboardInterrupt:
        print("\n⚠️ Deployment interrupted")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())