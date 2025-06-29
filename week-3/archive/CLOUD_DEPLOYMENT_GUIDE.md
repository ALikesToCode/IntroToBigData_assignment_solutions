# Google Cloud Dataproc Deployment Guide
## Week-3 Spark Click Analysis Application

This guide provides step-by-step instructions for deploying your Apache Spark click analysis application to Google Cloud Platform using Dataproc.

## 🚀 Quick Start

### Prerequisites
1. Google Cloud Platform account with billing enabled
2. `gcloud` CLI installed and configured
3. Required APIs enabled (Dataproc, Storage, Compute Engine)

### One-Command Deployment
```bash
# Set your configuration
export PROJECT_ID="your-actual-project-id"
export BUCKET_NAME="your-unique-bucket-name"

# Make deployment script executable and run
chmod +x gcloud_deploy.sh
./gcloud_deploy.sh deploy
```

## 📋 Detailed Setup Instructions

### Step 1: Environment Setup
```bash
# Authenticate with Google Cloud
gcloud auth login

# Set your project ID
export PROJECT_ID="your-project-id"
gcloud config set project $PROJECT_ID

# Set bucket name (must be globally unique)
export BUCKET_NAME="spark-analysis-$(date +%Y%m%d)-unique"
```

### Step 2: Enable Required APIs
```bash
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable compute.googleapis.com
```

### Step 3: Create GCS Bucket
```bash
gsutil mb -l us-central1 gs://$BUCKET_NAME
```

### Step 4: Upload Application Files
```bash
# Upload input data
gsutil cp data.txt gs://$BUCKET_NAME/input/

# Upload Spark applications
gsutil cp click_analysis_spark.py gs://$BUCKET_NAME/scripts/
gsutil cp click_analysis_spark_cloud.py gs://$BUCKET_NAME/scripts/
gsutil cp requirements.txt gs://$BUCKET_NAME/scripts/
```

### Step 5: Create Dataproc Cluster
```bash
gcloud dataproc clusters create spark-click-analysis-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-2 \
    --worker-machine-type=n1-standard-2 \
    --num-workers=2 \
    --image-version=2.1-debian11 \
    --enable-autoscaling \
    --max-workers=4 \
    --bucket=$BUCKET_NAME \
    --optional-components=JUPYTER \
    --enable-ip-alias \
    --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
    --metadata="PIP_PACKAGES=matplotlib seaborn pandas jupyter"
```

### Step 6: Submit Spark Job
```bash
# Submit the cloud-optimized version
gcloud dataproc jobs submit pyspark gs://$BUCKET_NAME/scripts/click_analysis_spark_cloud.py \
    --cluster=spark-click-analysis-cluster \
    --region=us-central1 \
    --args="--input=gs://$BUCKET_NAME/input/data.txt" \
    --args="--output=gs://$BUCKET_NAME/output/cloud_results.txt"
```

## 🔧 Advanced Configuration

### Cluster Customization
```bash
# For larger datasets, use more powerful machines
gcloud dataproc clusters create spark-click-analysis-large \
    --region=us-central1 \
    --master-machine-type=n1-standard-4 \
    --worker-machine-type=n1-standard-4 \
    --num-workers=4 \
    --preemptible-workers=2 \
    --disk-size=100GB \
    --enable-autoscaling \
    --max-workers=8 \
    --secondary-worker-type=preemptible
```

### Cost Optimization
```bash
# Use preemptible instances for cost savings
gcloud dataproc clusters create spark-click-analysis-preemptible \
    --region=us-central1 \
    --master-machine-type=n1-standard-2 \
    --worker-machine-type=n1-standard-2 \
    --num-workers=2 \
    --num-preemptible-workers=2 \
    --preemptible-worker-disk-size=50GB \
    --enable-autoscaling \
    --max-workers=6
```

## 📊 Jupyter Notebook Setup

### Access Jupyter on Dataproc
1. **Create SSH Tunnel**:
   ```bash
   gcloud compute ssh spark-click-analysis-cluster-m \
       --zone=us-central1-a \
       -- -L 8888:localhost:8888
   ```

2. **Open Jupyter in Browser**:
   - Navigate to: `http://localhost:8888`
   - Upload the notebook file: `click_analysis_notebook.ipynb`

3. **Update Notebook Configuration**:
   ```python
   # In the notebook, update these variables:
   BUCKET_NAME = "your-actual-bucket-name"
   INPUT_PATH = f"gs://{BUCKET_NAME}/input/data.txt"
   OUTPUT_PATH = f"gs://{BUCKET_NAME}/output/notebook_results"
   ```

### Alternative: Direct Jupyter Access
```bash
# Get cluster master external IP
MASTER_IP=$(gcloud compute instances describe spark-click-analysis-cluster-m \
    --zone=us-central1-a \
    --format='value(networkInterfaces[0].accessConfigs[0].natIP)')

# Access Jupyter directly (requires firewall rules)
echo "Jupyter available at: http://$MASTER_IP:8888"
```

## 📈 Monitoring and Management

### Monitor Job Progress
```bash
# List all jobs
gcloud dataproc jobs list --region=us-central1

# Describe specific job
gcloud dataproc jobs describe JOB_ID --region=us-central1

# View job logs
gcloud logging read "resource.type=dataproc_cluster" --limit=50
```

### Access Spark UI
```bash
# Get master instance IP
MASTER_IP=$(gcloud compute instances describe spark-click-analysis-cluster-m \
    --zone=us-central1-a \
    --format='value(networkInterfaces[0].accessConfigs[0].natIP)')

echo "Spark UI: http://$MASTER_IP:4040"
echo "Spark History Server: http://$MASTER_IP:18080"
```

### SSH to Cluster
```bash
# SSH to master node
gcloud compute ssh spark-click-analysis-cluster-m --zone=us-central1-a

# SSH to worker node
gcloud compute ssh spark-click-analysis-cluster-w-0 --zone=us-central1-a
```

## 📁 Results and Output

### View Results in GCS
```bash
# List output files
gsutil ls gs://$BUCKET_NAME/output/

# View text results
gsutil cat gs://$BUCKET_NAME/output/cloud_results.txt/part-00000

# Download all results
gsutil -m cp -r gs://$BUCKET_NAME/output/ ./cloud_results/
```

### Query Results with BigQuery
```sql
-- Create external table in BigQuery
CREATE OR REPLACE EXTERNAL TABLE `your-project.your_dataset.click_analysis`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket/output/*.parquet']
);

-- Query the results
SELECT 
  time_interval,
  click_count,
  unique_users,
  percentage
FROM `your-project.your_dataset.click_analysis`
ORDER BY click_count DESC;
```

## 🧹 Cleanup Resources

### Automated Cleanup
```bash
./gcloud_deploy.sh cleanup
```

### Manual Cleanup
```bash
# Delete Dataproc cluster
gcloud dataproc clusters delete spark-click-analysis-cluster --region=us-central1

# Delete GCS bucket
gsutil -m rm -r gs://$BUCKET_NAME

# List any remaining resources
gcloud compute instances list
gcloud dataproc clusters list --region=us-central1
```

## 💰 Cost Estimation

### Typical Costs (us-central1)
- **Master node** (n1-standard-2): ~$0.10/hour
- **Worker nodes** (2x n1-standard-2): ~$0.20/hour
- **Storage** (GCS): ~$0.02/GB/month
- **Network**: Minimal for this workload

**Total estimated cost**: ~$0.30/hour + storage

### Cost Optimization Tips
1. Use preemptible instances (60-91% discount)
2. Enable cluster autoscaling
3. Set idle deletion timeout
4. Use appropriate machine types for workload
5. Store data in appropriate GCS storage classes

## 🔍 Troubleshooting

### Common Issues

1. **Authentication Errors**:
   ```bash
   gcloud auth application-default login
   gcloud auth list
   ```

2. **Permission Denied**:
   ```bash
   # Check IAM roles
   gcloud projects get-iam-policy $PROJECT_ID
   
   # Add required roles
   gcloud projects add-iam-policy-binding $PROJECT_ID \
       --member="user:your-email@domain.com" \
       --role="roles/dataproc.admin"
   ```

3. **Job Failures**:
   ```bash
   # Check job logs
   gcloud dataproc jobs describe JOB_ID --region=us-central1
   
   # View detailed logs
   gcloud logging read "resource.type=dataproc_cluster AND severity>=ERROR"
   ```

4. **Network Issues**:
   ```bash
   # Check firewall rules
   gcloud compute firewall-rules list
   
   # Create rule for Jupyter (if needed)
   gcloud compute firewall-rules create allow-jupyter \
       --allow tcp:8888 \
       --source-ranges 0.0.0.0/0 \
       --description "Allow Jupyter notebook access"
   ```

## 📚 Additional Resources

### Documentation
- [Dataproc Documentation](https://cloud.google.com/dataproc/docs)
- [Spark on Dataproc](https://cloud.google.com/dataproc/docs/guides/spark)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

### Sample Commands
```bash
# Check cluster status
gcloud dataproc clusters describe spark-click-analysis-cluster --region=us-central1

# Scale cluster
gcloud dataproc clusters update spark-click-analysis-cluster \
    --region=us-central1 \
    --num-workers=4

# Submit additional jobs
gcloud dataproc jobs submit pyspark your-script.py \
    --cluster=spark-click-analysis-cluster \
    --region=us-central1
```

### Integration with Other GCP Services
- **BigQuery**: For data warehousing and analytics
- **Dataflow**: For stream processing
- **AI Platform**: For machine learning
- **Cloud Composer**: For workflow orchestration
- **Data Studio**: For visualization and dashboards

## 🎯 Success Metrics

Your deployment is successful when you can:
- ✅ Create Dataproc cluster without errors
- ✅ Submit and run Spark jobs successfully
- ✅ Access Jupyter notebooks via SSH tunnel
- ✅ View results in Google Cloud Storage
- ✅ Monitor jobs through Cloud Console
- ✅ Access Spark UI for performance monitoring

## 🚀 Next Steps

1. **Scale to larger datasets** using BigQuery public datasets
2. **Implement real-time processing** with Dataflow
3. **Add machine learning** capabilities with MLlib
4. **Create dashboards** using Data Studio
5. **Automate workflows** with Cloud Composer
6. **Set up monitoring** with Cloud Monitoring and alerting 