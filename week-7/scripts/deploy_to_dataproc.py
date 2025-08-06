#!/usr/bin/env python3
"""
Deploy Week 7 Streaming Pipeline to Google Cloud Dataproc
Course: Introduction to Big Data - Week 7

This script deploys the Kafka + Spark Streaming pipeline to Google Cloud Dataproc
for cloud-based execution of the real-time streaming assignment.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import time
import json
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

class DataprocStreamingDeployment:
    """
    Manages deployment of Kafka + Spark Streaming pipeline to Google Cloud Dataproc.
    """
    
    def __init__(self, project_id, region='us-central1', zone='us-central1-b'):
        """
        Initialize Dataproc deployment manager.
        
        Args:
            project_id: Google Cloud project ID
            region: GCP region for resources
            zone: GCP zone for cluster
        """
        self.project_id = project_id
        self.region = region
        self.zone = zone
        self.timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        
        # Resource names
        self.cluster_name = "week7-streaming-cluster"  # Fixed name for reuse
        self.bucket_name = f"{project_id}-week7-streaming-data"
        self.network_name = "default"
        
        # Script paths
        self.script_dir = Path(__file__).parent
        self.project_dir = self.script_dir.parent
        
        logger.info(f"🚀 Dataproc Streaming Deployment initialized")
        logger.info(f"   Project: {self.project_id}")
        logger.info(f"   Region: {self.region}")
        logger.info(f"   Zone: {self.zone}")
        logger.info(f"   Cluster: {self.cluster_name}")
        logger.info(f"   Bucket: {self.bucket_name}")
    
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
                timeout=600  # 10 minute timeout
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
    
    def check_prerequisites(self):
        """
        Check prerequisites for deployment.
        """
        logger.info("🔍 Checking deployment prerequisites...")
        
        # Check gcloud authentication
        result = self.run_command(
            "gcloud auth list --filter=status:ACTIVE --format='value(account)'",
            "Checking gcloud authentication"
        )
        
        if not result.stdout.strip():
            logger.error("❌ No active gcloud authentication")
            logger.error("Please run: gcloud auth login")
            raise RuntimeError("Authentication required")
        
        logger.info(f"✅ Authenticated as: {result.stdout.strip()}")
        
        # Set project
        self.run_command(f"gcloud config set project {self.project_id}", "Setting project")
        
        # Check project access
        self.run_command(f"gcloud projects describe {self.project_id}", "Verifying project access")
        
        logger.info("✅ Prerequisites validated")
    
    def enable_apis(self):
        """
        Enable required Google Cloud APIs.
        """
        apis = [
            'dataproc.googleapis.com',
            'compute.googleapis.com',
            'storage.googleapis.com',
            'pubsub.googleapis.com',
            'logging.googleapis.com'
        ]
        
        logger.info("🔌 Enabling required APIs...")
        
        for api in apis:
            self.run_command(f"gcloud services enable {api}", f"Enabling {api}")
            time.sleep(2)  # Rate limiting
        
        logger.info("✅ APIs enabled")
        time.sleep(10)  # Wait for API propagation
    
    def cluster_exists(self):
        """
        Check if the Dataproc cluster already exists.
        
        Returns:
            bool: True if cluster exists and is RUNNING, False otherwise
        """
        try:
            result = self.run_command(
                f"gcloud dataproc clusters describe {self.cluster_name} --region={self.region} --format='value(status.state)'", 
                f"Checking if cluster {self.cluster_name} exists",
                check=False
            )
            
            if result.returncode == 0:
                status = result.stdout.strip()
                logger.info(f"✅ Cluster {self.cluster_name} exists with status: {status}")
                return status == "RUNNING"
            else:
                logger.info(f"ℹ️ Cluster {self.cluster_name} does not exist")
                return False
                
        except Exception as e:
            logger.debug(f"Error checking cluster existence: {e}")
            return False
    
    def create_dataproc_cluster(self):
        """
        Create Dataproc cluster optimized for Kafka and Spark Streaming.
        """
        logger.info(f"🏗️ Creating Dataproc cluster: {self.cluster_name}")
        
        # Cluster configuration for streaming workloads (small size for testing)
        cluster_config = {
            'master': {
                'machine_type': 'e2-medium',  # 1 vCPU, 4GB RAM
                'boot_disk_size': '50GB',
                'boot_disk_type': 'pd-standard'
            },
            'worker': {
                'num_instances': 2,
                'machine_type': 'e2-medium',  # 1 vCPU, 4GB RAM
                'boot_disk_size': '50GB',
                'boot_disk_type': 'pd-standard'
            },
            'preemptible_workers': {
                'num_instances': 0  # No preemptible for streaming
            }
        }
        
        # Initialization scripts for Kafka
        init_script_uri = f"gs://{self.bucket_name}/scripts/kafka-init.sh"
        
        create_cmd = f"""
        gcloud dataproc clusters create {self.cluster_name} \\
            --region={self.region} \\
            --zone={self.zone} \\
            --master-machine-type={cluster_config['master']['machine_type']} \\
            --master-boot-disk-size={cluster_config['master']['boot_disk_size']} \\
            --num-workers={cluster_config['worker']['num_instances']} \\
            --worker-machine-type={cluster_config['worker']['machine_type']} \\
            --worker-boot-disk-size={cluster_config['worker']['boot_disk_size']} \\
            --image-version=2.1-debian11 \\
            --bucket={self.bucket_name} \\
            --optional-components=ZOOKEEPER \\
            --properties="spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
        """
        
        self.run_command(create_cmd, f"Creating cluster {self.cluster_name}")
        
        logger.info("✅ Dataproc cluster created successfully")
        
        # Wait for cluster to be ready
        self.wait_for_cluster_ready()
    
    def create_kafka_init_script(self):
        """
        Create Kafka initialization script for Dataproc nodes.
        """
        logger.info("📝 Creating Kafka initialization script...")
        
        init_script_content = '''#!/bin/bash
# Kafka initialization script for Dataproc cluster
set -e

# Update system
apt-get update

# Install Java 8 (required for Kafka)
apt-get install -y openjdk-8-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /etc/environment

# Download and install Kafka
cd /opt
wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar -xzf kafka_2.13-3.4.0.tgz
mv kafka_2.13-3.4.0 kafka
chown -R yarn:yarn /opt/kafka

# Create Kafka directories
mkdir -p /var/log/kafka
mkdir -p /var/lib/kafka-logs
chown -R yarn:yarn /var/log/kafka
chown -R yarn:yarn /var/lib/kafka-logs

# Configure Kafka
cat > /opt/kafka/config/server.properties << EOF
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$(hostname -I | awk '{print $1}'):9092
log.dirs=/var/lib/kafka-logs
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=24
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
default.replication.factor=1
min.insync.replicas=1
EOF

# Create systemd service for Kafka
cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=yarn
Group=yarn
Environment=JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
Environment=KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start Kafka service
systemctl daemon-reload
systemctl enable kafka
systemctl start kafka

# Wait for Kafka to start
sleep 30

# Create topic for streaming
/opt/kafka/bin/kafka-topics.sh --create --topic customer-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "Kafka initialization completed successfully"
'''
        
        # Save init script locally
        init_script_path = self.script_dir / "kafka-init.sh"
        with open(init_script_path, 'w') as f:
            f.write(init_script_content)
        
        # Upload to GCS
        gcs_path = f"gs://{self.bucket_name}/scripts/kafka-init.sh"
        self.run_command(f"gcloud storage cp {init_script_path} {gcs_path}", "Uploading Kafka init script")
        
        logger.info("✅ Kafka initialization script created and uploaded")
        
        return gcs_path
    
    def wait_for_cluster_ready(self):
        """
        Wait for Dataproc cluster to be ready.
        """
        logger.info("⏳ Waiting for cluster to be ready...")
        
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            try:
                result = self.run_command(
                    f"gcloud dataproc clusters describe {self.cluster_name} --region={self.region} --format='value(status.state)'",
                    f"Checking cluster status (attempt {attempt + 1})",
                    check=False
                )
                
                if result.returncode == 0 and "RUNNING" in result.stdout:
                    logger.info("✅ Cluster is ready")
                    return
                
                logger.info(f"   Cluster status: {result.stdout.strip()}")
                
            except Exception as e:
                logger.debug(f"   Status check error: {e}")
            
            attempt += 1
            time.sleep(30)  # Wait 30 seconds between checks
        
        raise TimeoutError("Cluster failed to become ready within timeout")
    
    def upload_application_files(self):
        """
        Upload producer and consumer applications to GCS.
        """
        logger.info("📤 Uploading application files to GCS...")
        
        # Install Kafka on cluster first
        self.setup_kafka_on_cluster()
        
        # Files to upload
        files_to_upload = [
            (self.project_dir / "producer" / "kafka_producer.py", "apps/kafka_producer.py"),
            (self.project_dir / "consumer" / "spark_streaming_consumer.py", "apps/spark_streaming_consumer.py"),
            (self.project_dir / "requirements.txt", "apps/requirements.txt")
        ]
        
        for local_path, gcs_path in files_to_upload:
            if local_path.exists():
                full_gcs_path = f"gs://{self.bucket_name}/{gcs_path}"
                self.run_command(f"gcloud storage cp {local_path} {full_gcs_path}", f"Uploading {local_path.name}")
            else:
                logger.warning(f"⚠️ File not found: {local_path}")
        
        logger.info("✅ Application files uploaded")
    
    def setup_kafka_on_cluster(self):
        """
        Setup Kafka on the Dataproc cluster using a simple job.
        """
        logger.info("📦 Setting up Kafka on cluster...")
        
        # Create a simple Python script to install and start Kafka
        kafka_setup_script = '''
import subprocess
import time
import os

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        print(f"Command: {cmd}")
        print(f"Output: {result.stdout}")
        if result.stderr:
            print(f"Error: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        print(f"Exception running {cmd}: {e}")
        return False

# Install dependencies
print("Installing Java and wget...")
run_command("sudo apt-get update || true")
run_command("sudo apt-get install -y openjdk-11-jdk wget")

# Set JAVA_HOME
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'

# Download and install Kafka
print("Downloading Kafka...")
if not os.path.exists("/opt/kafka"):
    run_command("cd /opt && sudo wget -q https://dlcdn.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz")
    run_command("cd /opt && sudo tar -xzf kafka_2.13-3.9.1.tgz")
    run_command("cd /opt && sudo mv kafka_2.13-3.9.1 kafka")
    run_command("sudo chown -R $USER:$USER /opt/kafka")

# Configure and start Kafka
print("Starting Kafka...")
os.chdir("/opt/kafka")

# Start Zookeeper (using the one from Dataproc)
print("Kafka setup completed - using Dataproc Zookeeper")

# Create a simple Kafka server properties for single node
with open("/opt/kafka/config/server.properties", "w") as f:
    f.write("""
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=/tmp/kafka-logs
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=24
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
default.replication.factor=1
min.insync.replicas=1
""")

# Start Kafka server
run_command("nohup bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &")
time.sleep(10)

# Create topic
run_command("bin/kafka-topics.sh --create --topic customer-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true")

print("Kafka setup completed successfully!")
'''

        # Write the setup script to a temporary file
        setup_script_path = "/tmp/kafka_setup.py"
        with open(setup_script_path, 'w') as f:
            f.write(kafka_setup_script)
        
        # Upload the setup script
        gcs_script_path = f"gs://{self.bucket_name}/scripts/kafka_setup.py"
        self.run_command(f"gcloud storage cp {setup_script_path} {gcs_script_path}", "Uploading Kafka setup script")
        
        try:
            # Submit the setup job
            setup_cmd = f"""
            gcloud dataproc jobs submit pyspark \\
                {gcs_script_path} \\
                --cluster={self.cluster_name} \\
                --region={self.region} \\
                --properties="spark.driver.memory=512m"
            """
            
            self.run_command(setup_cmd, "Setting up Kafka on cluster")
            logger.info("✅ Kafka setup completed")
            
        except Exception as e:
            logger.warning(f"⚠️ Kafka setup may have issues: {e}")
            logger.info("   Continuing with streaming jobs...")
            
        # Clean up local file
        os.remove(setup_script_path)
    
    def submit_streaming_jobs(self):
        """
        Submit producer and consumer jobs to the Dataproc cluster.
        """
        logger.info("🚀 Submitting streaming jobs to Dataproc...")
        
        # Submit producer job (PySpark job)
        producer_job_id = f"producer-job-{self.timestamp}"
        
        producer_cmd = f"""
        gcloud dataproc jobs submit pyspark \\
            gs://{self.bucket_name}/apps/kafka_producer.py \\
            --cluster={self.cluster_name} \\
            --region={self.region} \\
            --properties="spark.executor.memory=1g,spark.driver.memory=512m" \\
            -- \\
            --data-file=gs://{self.bucket_name}/input-data/customer_transactions_1200.csv \\
            --topic=customer-transactions \\
            --batch-size=10 \\
            --sleep-seconds=10 \\
            --max-records=1000
        """
        
        # Submit consumer job (Spark Streaming)
        consumer_job_id = f"consumer-job-{self.timestamp}"
        
        consumer_cmd = f"""
        gcloud dataproc jobs submit pyspark \\
            gs://{self.bucket_name}/apps/spark_streaming_consumer.py \\
            --cluster={self.cluster_name} \\
            --region={self.region} \\
            --properties="spark.executor.memory=1g,spark.driver.memory=512m,spark.sql.adaptive.enabled=true" \\
            -- \\
            --kafka-servers=localhost:9092 \\
            --topic=customer-transactions \\
            --window-duration=10 \\
            --slide-duration=5
        """
        
        # Submit jobs
        logger.info("📊 Submitting consumer job (Spark Streaming)...")
        self.run_command(consumer_cmd, f"Submitting consumer job {consumer_job_id}")
        
        # Wait a bit, then submit producer
        time.sleep(30)
        
        logger.info("📤 Submitting producer job...")
        self.run_command(producer_cmd, f"Submitting producer job {producer_job_id}")
        
        logger.info("✅ Jobs submitted successfully")
        
        return producer_job_id, consumer_job_id
    
    def monitor_jobs(self, producer_job_id, consumer_job_id):
        """
        Monitor running jobs and display status.
        
        Args:
            producer_job_id: Producer job ID
            consumer_job_id: Consumer job ID
        """
        logger.info("📊 Monitoring streaming jobs...")
        
        monitoring_duration = 600  # 10 minutes
        check_interval = 30       # 30 seconds
        
        start_time = time.time()
        
        while (time.time() - start_time) < monitoring_duration:
            try:
                # Check producer job status
                producer_result = self.run_command(
                    f"gcloud dataproc jobs describe {producer_job_id} --region={self.region} --format='value(status.state)'",
                    f"Checking producer job status",
                    check=False
                )
                
                # Check consumer job status
                consumer_result = self.run_command(
                    f"gcloud dataproc jobs describe {consumer_job_id} --region={self.region} --format='value(status.state)'",
                    f"Checking consumer job status",
                    check=False
                )
                
                producer_status = producer_result.stdout.strip() if producer_result.returncode == 0 else "UNKNOWN"
                consumer_status = consumer_result.stdout.strip() if consumer_result.returncode == 0 else "UNKNOWN"
                
                logger.info(f"📊 Job Status Update:")
                logger.info(f"   Producer: {producer_status}")
                logger.info(f"   Consumer: {consumer_status}")
                
                # Check if producer completed
                if producer_status in ["DONE", "ERROR", "CANCELLED"]:
                    logger.info(f"🏁 Producer job finished with status: {producer_status}")
                    break
                
                time.sleep(check_interval)
                
            except Exception as e:
                logger.warning(f"⚠️ Error monitoring jobs: {e}")
                time.sleep(check_interval)
        
        logger.info("✅ Job monitoring completed")
    
    def get_job_logs(self, job_id):
        """
        Retrieve and display job logs.
        
        Args:
            job_id: Dataproc job ID
        """
        try:
            logger.info(f"📋 Retrieving logs for job: {job_id}")
            
            result = self.run_command(
                f"gcloud dataproc jobs describe {job_id} --region={self.region} --format='value(driverOutputResourceUri)'",
                f"Getting log URI for job {job_id}",
                check=False
            )
            
            if result.returncode == 0 and result.stdout.strip():
                log_uri = result.stdout.strip()
                logger.info(f"   Log URI: {log_uri}")
                
                # Download and display logs
                self.run_command(f"gcloud storage cat {log_uri}", f"Displaying logs for {job_id}")
            else:
                logger.warning(f"⚠️ Could not retrieve logs for job {job_id}")
                
        except Exception as e:
            logger.error(f"❌ Error retrieving logs: {e}")
    
    def cleanup_resources(self):
        """
        Clean up created resources.
        """
        logger.info("🧹 Cleaning up resources...")
        
        try:
            # Delete cluster
            self.run_command(
                f"gcloud dataproc clusters delete {self.cluster_name} --region={self.region} --quiet",
                f"Deleting cluster {self.cluster_name}",
                check=False
            )
            
            logger.info("✅ Resources cleaned up")
            
        except Exception as e:
            logger.warning(f"⚠️ Error during cleanup: {e}")
    
    def deploy_complete_pipeline(self):
        """
        Deploy the complete streaming pipeline.
        """
        logger.info("🚀 Starting complete pipeline deployment...")
        
        try:
            # Prerequisites
            self.check_prerequisites()
            
            # Enable APIs
            self.enable_apis()
            
            # Check if cluster already exists
            if self.cluster_exists():
                logger.info("♻️ Using existing cluster")
            else:
                logger.info("🆕 Creating new cluster")
                # Create Kafka initialization script
                self.create_kafka_init_script()
                
                # Create Dataproc cluster
                self.create_dataproc_cluster()
            
            # Upload application files
            self.upload_application_files()
            
            # Submit streaming jobs
            producer_job_id, consumer_job_id = self.submit_streaming_jobs()
            
            # Monitor jobs
            self.monitor_jobs(producer_job_id, consumer_job_id)
            
            # Get job logs
            self.get_job_logs(producer_job_id)
            self.get_job_logs(consumer_job_id)
            
            logger.info("🎉 Pipeline deployment completed successfully!")
            
            # Print access information
            self.print_access_information()
            
        except Exception as e:
            logger.error(f"❌ Pipeline deployment failed: {e}")
            raise
    
    def print_access_information(self):
        """
        Print information for accessing the deployed pipeline.
        """
        print(f"\n{'='*80}")
        print("🎉 WEEK 7 STREAMING PIPELINE DEPLOYED!")
        print(f"{'='*80}")
        print(f"Project: {self.project_id}")
        print(f"Region: {self.region}")
        print(f"Cluster: {self.cluster_name}")
        print(f"Data Bucket: gs://{self.bucket_name}")
        print(f"\n📋 MONITORING COMMANDS:")
        print(f"View cluster:")
        print(f"  gcloud dataproc clusters describe {self.cluster_name} --region={self.region}")
        print(f"\nList jobs:")
        print(f"  gcloud dataproc jobs list --region={self.region} --cluster={self.cluster_name}")
        print(f"\nAccess cluster UI:")
        print(f"  gcloud dataproc clusters describe {self.cluster_name} --region={self.region} --format='value(config.gceClusterConfig.masterInstanceName)'")
        print(f"\nCleanup when done:")
        print(f"  gcloud dataproc clusters delete {self.cluster_name} --region={self.region}")
        print(f"{'='*80}\n")

def main():
    """
    Main function for Dataproc deployment.
    """
    parser = argparse.ArgumentParser(description='Deploy Week 7 Streaming Pipeline to Dataproc')
    parser.add_argument('--project-id', required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--region', default='us-central1',
                       help='GCP region (default: us-central1)')
    parser.add_argument('--zone', default='us-central1-b',
                       help='GCP zone (default: us-central1-b)')
    parser.add_argument('--cleanup-only', action='store_true',
                       help='Only cleanup existing resources')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Create deployment manager
        deployment = DataprocStreamingDeployment(
            project_id=args.project_id,
            region=args.region,
            zone=args.zone
        )
        
        if args.cleanup_only:
            deployment.cleanup_resources()
        else:
            deployment.deploy_complete_pipeline()
        
    except KeyboardInterrupt:
        logger.info("👋 Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Deployment failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()