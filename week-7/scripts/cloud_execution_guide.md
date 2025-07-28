# Week 7: Google Cloud Execution Guide

## Cloud-Only Deployment Instructions

This guide provides step-by-step instructions for executing Week 7's Kafka + Spark Streaming assignment entirely on Google Cloud Platform using Dataproc clusters.

## Architecture Overview (Cloud-Native)

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   GCS Bucket    │    │   Dataproc       │    │   Dataproc      │
│   (Input Data)  │───▶│   Cluster        │───▶│   Spark         │
│                 │    │   (Kafka)        │    │   Streaming     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                               │                         │
                               ▼                         ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │   Kafka Topics   │    │   Real-time     │
                       │   (Streaming)    │    │   Analytics     │
                       └──────────────────┘    └─────────────────┘
```

## Prerequisites

### 1. Google Cloud Project Setup
```bash
# Create or select a GCP project
gcloud projects create your-bigdata-project-2025  # If creating new
gcloud config set project your-bigdata-project-2025

# Enable billing for the project (required for Dataproc)
# Do this through the Google Cloud Console: console.cloud.google.com
```

### 2. Authentication Setup
```bash
# Authenticate with Google Cloud
gcloud auth login
gcloud auth application-default login

# Verify authentication
gcloud auth list
gcloud config list
```

### 3. Required APIs and Permissions
```bash
# Enable required APIs (automated in deployment script)
gcloud services enable dataproc.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable logging.googleapis.com
```

## Step-by-Step Cloud Execution

### Phase 1: Data Preparation (5 minutes)

**Step 1: Generate and Upload Data to GCS**
```bash
cd week-7

# Generate data and upload to GCS
python scripts/upload_data_to_gcs.py \
    --project-id YOUR_PROJECT_ID \
    --generate-data \
    --list-bucket

# Expected output:
# ✅ Generated 1200 records and saved to data/customer_transactions_1200.csv
# ✅ Upload completed successfully
# GCS location: gs://YOUR_PROJECT_ID-week7-streaming-data/input-data/customer_transactions_1200.csv
```

**Verify Data Upload:**
```bash
gsutil ls gs://YOUR_PROJECT_ID-week7-streaming-data/
gsutil cat gs://YOUR_PROJECT_ID-week7-streaming-data/input-data/customer_transactions_1200.csv | head -5
```

### Phase 2: Cloud Infrastructure Deployment (10-15 minutes)

**Step 2: Deploy Complete Streaming Pipeline**
```bash
# Deploy entire pipeline to Dataproc
python scripts/deploy_to_dataproc.py \
    --project-id YOUR_PROJECT_ID \
    --region us-central1 \
    --verbose

# This will:
# 1. Create Dataproc cluster with Kafka pre-installed
# 2. Upload producer and consumer applications
# 3. Submit streaming jobs
# 4. Monitor execution
```

**Expected Deployment Output:**
```
🚀 Dataproc Streaming Deployment initialized
   Project: your-bigdata-project-2025
   Region: us-central1
   Cluster: week7-streaming-cluster-20250727-143052

🔍 Checking deployment prerequisites...
✅ Authenticated as: your-email@university.edu
✅ Prerequisites validated

🔌 Enabling required APIs...
✅ APIs enabled

📝 Creating Kafka initialization script...
✅ Kafka initialization script created and uploaded

🏗️ Creating Dataproc cluster: week7-streaming-cluster-20250727-143052
✅ Dataproc cluster created successfully

📤 Uploading application files to GCS...
✅ Application files uploaded

🚀 Submitting streaming jobs to Dataproc...
📊 Submitting consumer job (Spark Streaming)...
📤 Submitting producer job...
✅ Jobs submitted successfully

📊 Monitoring streaming jobs...
📊 Job Status Update:
   Producer: RUNNING
   Consumer: RUNNING
```

### Phase 3: Monitor Real-time Processing (15-20 minutes)

**Step 3: Monitor Jobs via Google Cloud Console**

1. **Open Cloud Console**: https://console.cloud.google.com
2. **Navigate to Dataproc**: Menu → Big Data → Dataproc → Clusters
3. **Select Your Cluster**: `week7-streaming-cluster-TIMESTAMP`
4. **View Jobs**: Click "Jobs" tab to see running producer and consumer
5. **Check Logs**: Click on individual jobs to view real-time logs

**Step 4: Monitor via Command Line**
```bash
# List running jobs
gcloud dataproc jobs list \
    --region=us-central1 \
    --cluster=week7-streaming-cluster-TIMESTAMP

# Get job details
gcloud dataproc jobs describe PRODUCER_JOB_ID --region=us-central1
gcloud dataproc jobs describe CONSUMER_JOB_ID --region=us-central1

# View job logs
gcloud dataproc jobs wait PRODUCER_JOB_ID --region=us-central1
gcloud dataproc jobs wait CONSUMER_JOB_ID --region=us-central1
```

**Step 5: SSH into Master Node (Optional)**
```bash
# SSH into cluster master node
gcloud compute ssh week7-streaming-cluster-TIMESTAMP-m \
    --zone=us-central1-b

# Check Kafka status
sudo systemctl status kafka

# List Kafka topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Monitor Kafka console consumer
/opt/kafka/bin/kafka-console-consumer.sh \
    --topic customer-transactions \
    --from-beginning \
    --bootstrap-server localhost:9092
```

### Phase 4: Results Verification (5 minutes)

**Step 6: Verify Streaming Results**

**Producer Output (Expected):**
```
📤 Sending batch 1 with 10 records...
✅ Batch 1 sent successfully
   Records in batch: 10
   Total records sent: 10
⏰ Sleeping for 10 seconds before next batch...

📤 Sending batch 2 with 10 records...
✅ Batch 2 sent successfully
   Records in batch: 10
   Total records sent: 20
```

**Consumer Output (Expected):**
```
============================================================
📄 PROCESSING BATCH 0 (Consumer Batch #1)
============================================================
|window_start       |window_end         |record_count|total_transaction_value|
|2025-07-27 14:35:05|2025-07-27 14:35:15|10          |13759.60               |

📊 Records in this batch: 10
📊 Total records processed: 10

📈 BATCH STATISTICS:
   Window: 2025-07-27 14:35:05 to 2025-07-27 14:35:15
   Records: 10
   Unique customers: 10
   Total transaction value: $13,759.60
   Average transaction value: $1375.96
```

**Step 7: Download Job Results**
```bash
# Download job output logs
gcloud dataproc jobs describe PRODUCER_JOB_ID \
    --region=us-central1 \
    --format='value(driverOutputResourceUri)' | \
    xargs gsutil cat > producer_output.log

gcloud dataproc jobs describe CONSUMER_JOB_ID \
    --region=us-central1 \
    --format='value(driverOutputResourceUri)' | \
    xargs gsutil cat > consumer_output.log

# View downloaded logs
head -50 producer_output.log
head -50 consumer_output.log
```

## Cloud Resource Management

### Monitoring Dashboard
```bash
# View cluster details
gcloud dataproc clusters describe week7-streaming-cluster-TIMESTAMP \
    --region=us-central1

# Check cluster resource usage
gcloud compute instances list --filter="name~week7-streaming-cluster"

# Monitor GCS bucket usage
gsutil du -sh gs://YOUR_PROJECT_ID-week7-streaming-data/
```

### Cost Management
```bash
# Check current resource usage
gcloud compute instances list --format="table(name,machineType,status,zone)"

# Estimate costs
echo "Cluster cost: ~$2-4/hour for e2-standard-4 instances"
echo "Storage cost: ~$0.02/GB/month for GCS"
echo "Network cost: ~$0.01/GB for data transfer"
```

### Cleanup After Completion
```bash
# Stop all jobs (if still running)
gcloud dataproc jobs cancel PRODUCER_JOB_ID --region=us-central1
gcloud dataproc jobs cancel CONSUMER_JOB_ID --region=us-central1

# Delete Dataproc cluster
gcloud dataproc clusters delete week7-streaming-cluster-TIMESTAMP \
    --region=us-central1 \
    --quiet

# Optionally delete GCS bucket (if no longer needed)
gsutil rm -r gs://YOUR_PROJECT_ID-week7-streaming-data/
```

## Troubleshooting Guide

### Common Issues and Solutions

**Issue 1: Cluster Creation Timeout**
```
Error: Cluster creation timed out
```
**Solution:**
```bash
# Check quotas
gcloud compute project-info describe --format="yaml(quotas)"

# Request quota increase if needed
# Use Google Cloud Console: IAM & Admin → Quotas
```

**Issue 2: Job Submission Failed**
```
Error: Failed to submit job to cluster
```
**Solution:**
```bash
# Check cluster status
gcloud dataproc clusters describe CLUSTER_NAME --region=us-central1

# Verify cluster is RUNNING
# Check initialization script logs
gcloud compute ssh CLUSTER_NAME-m --zone=us-central1-b
sudo journalctl -u kafka
```

**Issue 3: Kafka Connection Issues**
```
Error: KafkaTimeoutException
```
**Solution:**
```bash
# SSH into cluster and check Kafka
gcloud compute ssh CLUSTER_NAME-m --zone=us-central1-b

# Check Kafka service
sudo systemctl status kafka
sudo systemctl restart kafka

# Verify topic creation
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Issue 4: Out of Memory Errors**
```
Error: Java heap space
```
**Solution:**
```bash
# Increase cluster memory by recreating with larger instances
python scripts/deploy_to_dataproc.py \
    --project-id YOUR_PROJECT_ID \
    --machine-type e2-standard-8  # Larger instance type
```

## Performance Optimization

### Cluster Sizing Recommendations
```bash
# For testing (minimal cost):
# Master: e2-standard-2 (2 vCPU, 8GB RAM)
# Workers: 2x e2-standard-2

# For production (better performance):
# Master: e2-standard-4 (4 vCPU, 16GB RAM)  
# Workers: 3x e2-standard-4

# For large datasets:
# Master: e2-standard-8 (8 vCPU, 32GB RAM)
# Workers: 4x e2-standard-8
```

### Monitoring Performance
```bash
# View job metrics
gcloud dataproc jobs describe JOB_ID \
    --region=us-central1 \
    --format="yaml(status,placement,driverControlFilesUri)"

# Access Spark UI (port forwarding)
gcloud compute ssh CLUSTER_NAME-m \
    --zone=us-central1-b \
    --ssh-flag="-L 8080:localhost:8080"
# Then open http://localhost:8080 in browser
```

## Learning Outcomes

### Cloud Platform Skills
- **Dataproc Cluster Management**: Creation, configuration, and monitoring
- **Cloud Storage Integration**: Data upload, management, and access patterns
- **Job Orchestration**: Submitting and monitoring distributed jobs
- **Resource Management**: Cost optimization and cleanup procedures

### Production Deployment
- **Infrastructure as Code**: Automated deployment scripts
- **Monitoring and Logging**: Cloud-native observability
- **Scaling Strategies**: Horizontal and vertical scaling approaches
- **Security Best Practices**: IAM, network security, and access control

This cloud-native approach demonstrates enterprise-grade real-time streaming architecture using Google Cloud Platform's managed services for scalable, production-ready big data processing.