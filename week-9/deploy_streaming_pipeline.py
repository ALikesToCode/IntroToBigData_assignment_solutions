#!/usr/bin/env python3
"""
Enhanced Deployment Script for Real-Time Image Classification Pipeline
Course: Introduction to Big Data - Week 9
Author: Abhyudaya B Tharakan 22f3001492

This script deploys the complete real-time streaming architecture including:
- Dataproc cluster with optimized configurations
- Kafka cluster for real-time streaming
- Pub/Sub topics and subscriptions
- BigQuery tables for results storage
- GCS buckets for checkpoints and outputs
"""

import os
import sys
import json
import time
import subprocess
import argparse
from pathlib import Path
from typing import Dict, Any


# Configuration
DEFAULT_CONFIG = {
    "project_id": "steady-triumph-447006-f8",
    "region": "us-central1",
    "zone": "us-central1-b",
    "cluster_name": "week9-streaming-cluster",
    "bucket_name": "week9-streaming-data",
    "kafka_topic": "image-stream",
    "pubsub_topic": "gcs-image-notifications",
    "pubsub_subscription": "image-stream-sub",
    "bigquery_dataset": "ml_streaming",
    "bigquery_table": "image_predictions"
}


class StreamingPipelineDeployer:
    """Deploy and manage real-time image classification streaming pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.project_id = config["project_id"]
        self.region = config["region"]
        self.zone = config["zone"]
        
        print("=" * 70)
        print("Real-Time Image Classification Pipeline Deployment")
        print("=" * 70)
        print(f"Project: {self.project_id}")
        print(f"Region: {self.region}")
        print(f"Cluster: {config['cluster_name']}")
    
    def run_command(self, cmd: str, description: str = "") -> bool:
        """Execute shell command with logging."""
        if description:
            print(f"\n📋 {description}")
        print(f"🔧 Executing: {cmd}")
        print("-" * 60)
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error: {result.stderr}")
            return False
        else:
            if result.stdout.strip():
                print(result.stdout)
            print("✅ Success")
            return True
    
    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met."""
        print("\n🔍 Checking prerequisites...")
        
        # Check gcloud auth
        if not self.run_command("gcloud auth list --filter=status:ACTIVE --format='value(account)'"):
            print("❌ Please run: gcloud auth login")
            return False
        
        # Check project access
        if not self.run_command(f"gcloud config set project {self.project_id}"):
            print(f"❌ Cannot access project: {self.project_id}")
            return False
        
        # Enable required APIs
        apis = [
            "compute.googleapis.com",
            "dataproc.googleapis.com",
            "storage.googleapis.com",
            "pubsub.googleapis.com",
            "bigquery.googleapis.com"
        ]
        
        for api in apis:
            self.run_command(f"gcloud services enable {api}", f"Enabling {api}")
        
        print("✅ Prerequisites checked")
        return True
    
    def create_gcs_bucket(self) -> bool:
        """Create GCS bucket for data storage."""
        bucket_name = self.config["bucket_name"]
        full_bucket_name = f"{self.project_id}-{bucket_name}"
        
        print(f"\n🪣 Creating GCS bucket: {full_bucket_name}")
        
        # Check if bucket exists
        check_cmd = f"gcloud storage ls gs://{full_bucket_name} 2>/dev/null"
        if subprocess.run(check_cmd, shell=True, capture_output=True).returncode == 0:
            print(f"✅ Bucket gs://{full_bucket_name} already exists")
            return True
        
        # Create bucket
        create_cmd = f"gcloud storage buckets create gs://{full_bucket_name} --location={self.region}"
        if not self.run_command(create_cmd):
            return False
        
        # Create directory structure
        directories = ["checkpoints/", "models/", "outputs/", "input-images/"]
        for directory in directories:
            touch_cmd = f"echo '' | gcloud storage cp - gs://{full_bucket_name}/{directory}.keep"
            self.run_command(touch_cmd, f"Creating directory {directory}")
        
        # Update bucket name in config
        self.config["bucket_name"] = full_bucket_name
        return True
    
    def setup_pubsub(self) -> bool:
        """Setup Pub/Sub topics and subscriptions."""
        topic = self.config["pubsub_topic"]
        subscription = self.config["pubsub_subscription"]
        
        print(f"\n📢 Setting up Pub/Sub: {topic}")
        
        # Create topic
        topic_cmd = f"gcloud pubsub topics create {topic}"
        self.run_command(topic_cmd, "Creating Pub/Sub topic")
        
        # Create subscription
        sub_cmd = f"gcloud pubsub subscriptions create {subscription} --topic={topic}"
        self.run_command(sub_cmd, "Creating Pub/Sub subscription")
        
        # Setup bucket notification
        bucket_name = self.config["bucket_name"]
        notification_cmd = (
            f"gcloud storage buckets notifications create "
            f"gs://{bucket_name}/input-images/ "
            f"--topic=projects/{self.project_id}/topics/{topic} "
            f"--event-types=OBJECT_FINALIZE"
        )
        self.run_command(notification_cmd, "Setting up bucket notifications")
        
        return True
    
    def setup_bigquery(self) -> bool:
        """Setup BigQuery dataset and table."""
        dataset = self.config["bigquery_dataset"]
        table = self.config["bigquery_table"]
        
        print(f"\n📊 Setting up BigQuery: {dataset}.{table}")
        
        # Create dataset
        dataset_cmd = f"bq mk --location={self.region} {dataset}"
        self.run_command(dataset_cmd, "Creating BigQuery dataset")
        
        # Create table with schema
        schema = [
            "prediction_id:STRING",
            "label:STRING", 
            "confidence:FLOAT64",
            "model_name:STRING",
            "processing_time_ms:INTEGER",
            "image_size_bytes:INTEGER",
            "bucket:STRING",
            "object_name:STRING",
            "processed_at:TIMESTAMP"
        ]
        
        schema_str = ",".join(schema)
        table_cmd = f"bq mk --table {dataset}.{table} {schema_str}"
        self.run_command(table_cmd, "Creating BigQuery table")
        
        return True
    
    def create_dataproc_cluster(self) -> bool:
        """Create optimized Dataproc cluster for streaming."""
        cluster_name = self.config["cluster_name"]
        bucket_name = self.config["bucket_name"]
        
        print(f"\n🖥️ Creating Dataproc cluster: {cluster_name}")
        
        # Check if cluster exists
        check_cmd = (
            f"gcloud dataproc clusters describe {cluster_name} "
            f"--region={self.region} --format=json 2>/dev/null"
        )
        if subprocess.run(check_cmd, shell=True, capture_output=True).returncode == 0:
            print(f"✅ Cluster {cluster_name} already exists")
            return True
        
        # Prepare initialization script
        init_script = f"""#!/bin/bash
        # Install Python packages
        /opt/conda/default/bin/pip install kafka-python google-cloud-storage tensorflow pillow
        
        # Download Kafka
        cd /opt
        wget -q https://downloads.apache.org/kafka/2.13-3.6.0/kafka_2.13-3.6.0.tgz
        tar -xzf kafka_2.13-3.6.0.tgz
        ln -s kafka_2.13-3.6.0 kafka
        
        # Create Kafka systemd service
        cat > /etc/systemd/system/kafka.service << 'EOF'
        [Unit]
        Description=Apache Kafka
        After=network.target
        
        [Service]
        Type=simple
        User=root
        ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
        ExecStop=/opt/kafka/bin/kafka-server-stop.sh
        Restart=on-abnormal
        
        [Install]
        WantedBy=multi-user.target
        EOF
        
        # Start Kafka on master node
        if [[ "$(hostname)" == "{cluster_name}-m" ]]; then
            # Update Kafka config
            echo "advertised.listeners=PLAINTEXT://$(hostname -i):9092" >> /opt/kafka/config/server.properties
            echo "listeners=PLAINTEXT://0.0.0.0:9092" >> /opt/kafka/config/server.properties
            
            # Start Zookeeper and Kafka
            systemctl daemon-reload
            systemctl enable kafka
            /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &
            sleep 10
            systemctl start kafka
        fi
        """
        
        # Upload initialization script
        init_script_path = f"gs://{bucket_name}/scripts/kafka-init.sh"
        with open("/tmp/kafka-init.sh", "w") as f:
            f.write(init_script)
        
        upload_cmd = f"gcloud storage cp /tmp/kafka-init.sh {init_script_path}"
        if not self.run_command(upload_cmd, "Uploading initialization script"):
            return False
        
        # Create cluster
        cluster_cmd = f"""
        gcloud dataproc clusters create {cluster_name} \\
            --project={self.project_id} \\
            --region={self.region} \\
            --zone={self.zone} \\
            --master-machine-type=n1-standard-4 \\
            --master-boot-disk-size=100GB \\
            --worker-machine-type=n1-standard-4 \\
            --worker-boot-disk-size=50GB \\
            --num-workers=2 \\
            --image-version=2.1-debian11 \\
            --initialization-actions={init_script_path} \\
            --optional-components=ZOOKEEPER \\
            --properties="spark:spark.streaming.stopGracefullyOnShutdown=true,spark:spark.sql.streaming.metricsEnabled=true,spark:spark.sql.adaptive.enabled=true" \\
            --enable-ip-alias \\
            --max-idle=30m \\
            --max-age=3h
        """
        
        return self.run_command(cluster_cmd, "Creating Dataproc cluster")
    
    def upload_application_code(self) -> bool:
        """Upload application code to GCS."""
        bucket_name = self.config["bucket_name"]
        
        print(f"\n📤 Uploading application code")
        
        # List of files to upload
        files_to_upload = [
            "enhanced_streaming_image_classifier.py",
            "image_stream_simulator.py",
            "streaming_image_classifier.py"  # Original for comparison
        ]
        
        for file_name in files_to_upload:
            if os.path.exists(file_name):
                upload_cmd = f"gcloud storage cp {file_name} gs://{bucket_name}/apps/"
                self.run_command(upload_cmd, f"Uploading {file_name}")
        
        return True
    
    def create_kafka_topics(self) -> bool:
        """Create Kafka topics on the cluster."""
        cluster_name = self.config["cluster_name"]
        kafka_topic = self.config["kafka_topic"]
        
        print(f"\n📋 Creating Kafka topics")
        
        # SSH to master node and create topics
        ssh_cmd = f"""
        gcloud compute ssh {cluster_name}-m \\
            --zone={self.zone} \\
            --command='
            /opt/kafka/bin/kafka-topics.sh \\
                --create \\
                --topic {kafka_topic} \\
                --bootstrap-server localhost:9092 \\
                --partitions 3 \\
                --replication-factor 1
            
            /opt/kafka/bin/kafka-topics.sh \\
                --create \\
                --topic {kafka_topic}-results \\
                --bootstrap-server localhost:9092 \\
                --partitions 3 \\
                --replication-factor 1
            '
        """
        
        return self.run_command(ssh_cmd, "Creating Kafka topics")
    
    def deploy_streaming_job(self) -> bool:
        """Deploy the streaming job to Dataproc."""
        cluster_name = self.config["cluster_name"]
        bucket_name = self.config["bucket_name"]
        
        print(f"\n🚀 Deploying streaming job")
        
        # Job configuration
        job_args = [
            f"gs://{bucket_name}/apps/enhanced_streaming_image_classifier.py",
            "--input-source", "both",
            "--kafka-servers", f"{cluster_name}-m:9092",
            "--kafka-topic", self.config["kafka_topic"],
            "--project-id", self.project_id,
            "--pubsub-subscription", self.config["pubsub_subscription"],
            "--model-type", "mobilenet",
            "--confidence-threshold", "0.1",
            "--checkpoint", f"gs://{bucket_name}/checkpoints/streaming",
            "--trigger-seconds", "5",
            "--output-bq-table", f"{self.project_id}.{self.config['bigquery_dataset']}.{self.config['bigquery_table']}",
            "--output-kafka-topic", f"{self.config['kafka_topic']}-results",
            "--output-gcs-path", f"gs://{bucket_name}/outputs/predictions"
        ]
        
        # Submit job
        submit_cmd = f"""
        gcloud dataproc jobs submit pyspark \\
            --cluster={cluster_name} \\
            --region={self.region} \\
            --packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.google.cloud.spark:spark-pubsub_2.12:2.4.6,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \\
            --properties="spark.sql.shuffle.partitions=8,spark.streaming.stopGracefullyOnShutdown=true" \\
            -- {' '.join(job_args)}
        """
        
        return self.run_command(submit_cmd, "Submitting streaming job")
    
    def start_image_simulator(self) -> bool:
        """Start the image simulator locally."""
        cluster_name = self.config["cluster_name"]
        kafka_topic = self.config["kafka_topic"]
        bucket_name = self.config["bucket_name"]
        
        print(f"\n🎬 Starting image simulator")
        
        # Get external IP of master node
        get_ip_cmd = f"""
        gcloud compute instances describe {cluster_name}-m \\
            --zone={self.zone} \\
            --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
        """
        
        result = subprocess.run(get_ip_cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Failed to get master node IP")
            return False
        
        master_ip = result.stdout.strip()
        print(f"Master node IP: {master_ip}")
        
        # Run simulator
        simulator_cmd = f"""
        python image_stream_simulator.py \\
            --mode both \\
            --rate 2.0 \\
            --duration 300 \\
            --kafka-servers {master_ip}:9092 \\
            --kafka-topic {kafka_topic} \\
            --gcs-bucket {bucket_name} \\
            --gcs-prefix input-images/
        """
        
        print("Starting image simulator (run in separate terminal):")
        print(simulator_cmd)
        return True
    
    def cleanup_resources(self) -> bool:
        """Clean up deployed resources."""
        print(f"\n🧹 Cleaning up resources")
        
        cluster_name = self.config["cluster_name"]
        topic = self.config["pubsub_topic"]
        subscription = self.config["pubsub_subscription"]
        
        # Delete cluster
        delete_cluster_cmd = f"""
        gcloud dataproc clusters delete {cluster_name} \\
            --region={self.region} --quiet
        """
        self.run_command(delete_cluster_cmd, "Deleting Dataproc cluster")
        
        # Delete Pub/Sub resources
        self.run_command(f"gcloud pubsub subscriptions delete {subscription}", 
                        "Deleting Pub/Sub subscription")
        self.run_command(f"gcloud pubsub topics delete {topic}", 
                        "Deleting Pub/Sub topic")
        
        print("✅ Cleanup completed")
        return True
    
    def deploy_complete_pipeline(self) -> bool:
        """Deploy the complete streaming pipeline."""
        steps = [
            ("Checking prerequisites", self.check_prerequisites),
            ("Creating GCS bucket", self.create_gcs_bucket),
            ("Setting up Pub/Sub", self.setup_pubsub),
            ("Setting up BigQuery", self.setup_bigquery),
            ("Creating Dataproc cluster", self.create_dataproc_cluster),
            ("Uploading application code", self.upload_application_code),
            ("Creating Kafka topics", self.create_kafka_topics),
            ("Deploying streaming job", self.deploy_streaming_job),
            ("Providing simulator instructions", self.start_image_simulator)
        ]
        
        for step_name, step_func in steps:
            print(f"\n{'='*70}")
            print(f"Step: {step_name}")
            print(f"{'='*70}")
            
            if not step_func():
                print(f"❌ Failed at step: {step_name}")
                return False
            
            print(f"✅ Completed: {step_name}")
        
        print(f"\n{'='*70}")
        print("🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!")
        print(f"{'='*70}")
        print(f"📊 Monitor results in BigQuery: {self.project_id}.{self.config['bigquery_dataset']}.{self.config['bigquery_table']}")
        print(f"🪣 Check outputs in GCS: gs://{self.config['bucket_name']}/outputs/")
        print(f"⚡ Kafka results topic: {self.config['kafka_topic']}-results")
        
        return True


def main():
    parser = argparse.ArgumentParser(description="Deploy Real-Time Image Classification Pipeline")
    parser.add_argument("--config", help="Configuration JSON file")
    parser.add_argument("--project-id", help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--cleanup", action="store_true", help="Cleanup deployed resources")
    
    args = parser.parse_args()
    
    # Load configuration
    config = DEFAULT_CONFIG.copy()
    
    if args.config and os.path.exists(args.config):
        with open(args.config) as f:
            config.update(json.load(f))
    
    if args.project_id:
        config["project_id"] = args.project_id
    if args.region:
        config["region"] = args.region
    
    # Initialize deployer
    deployer = StreamingPipelineDeployer(config)
    
    try:
        if args.cleanup:
            deployer.cleanup_resources()
        else:
            deployer.deploy_complete_pipeline()
    except KeyboardInterrupt:
        print("\n\n⚠️ Deployment interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Deployment failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())