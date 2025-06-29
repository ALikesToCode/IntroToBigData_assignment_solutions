#!/bin/bash

# Google Cloud Dataproc Deployment Script for Week-3 Spark Analysis
# This script automates the deployment of the click analysis Spark application to GCP

set -e  # Exit on any error

# Configuration Variables
PROJECT_ID="${PROJECT_ID:-your-project-id}"
CLUSTER_NAME="spark-click-analysis-cluster"
REGION="us-central1"
ZONE="us-central1-a"
BUCKET_NAME="${BUCKET_NAME:-your-bucket-name-unique}"
WORKER_COUNT=0
WORKER_MACHINE_TYPE="e2-standard-2"
MASTER_MACHINE_TYPE="e2-standard-4"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}    Google Cloud Dataproc Deployment - Week 3 Spark      ${NC}"
echo -e "${BLUE}============================================================${NC}"

# Function to check if gcloud is authenticated
check_gcloud_auth() {
    echo -e "${YELLOW}Checking Google Cloud authentication...${NC}"
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "."; then
        echo -e "${RED}Error: Not authenticated with Google Cloud${NC}"
        echo "Please run: gcloud auth login"
        exit 1
    fi
    echo -e "${GREEN}✓ Google Cloud authentication verified${NC}"
}

# Function to set project
set_project() {
    echo -e "${YELLOW}Setting Google Cloud project...${NC}"
    if [ "$PROJECT_ID" = "your-project-id" ]; then
        echo -e "${RED}Error: Please set PROJECT_ID environment variable${NC}"
        echo "Example: export PROJECT_ID=your-actual-project-id"
        exit 1
    fi
    
    gcloud config set project $PROJECT_ID
    echo -e "${GREEN}✓ Project set to: $PROJECT_ID${NC}"
}

# Function to enable required APIs
enable_apis() {
    echo -e "${YELLOW}Enabling required Google Cloud APIs...${NC}"
    gcloud services enable dataproc.googleapis.com
    gcloud services enable storage.googleapis.com
    gcloud services enable compute.googleapis.com
    echo -e "${GREEN}✓ Required APIs enabled${NC}"
}

# Function to create GCS bucket
create_bucket() {
    echo -e "${YELLOW}Creating Google Cloud Storage bucket...${NC}"
    if [ "$BUCKET_NAME" = "your-bucket-name-unique" ]; then
        echo -e "${RED}Error: Please set BUCKET_NAME environment variable${NC}"
        echo "Example: export BUCKET_NAME=my-spark-analysis-bucket-unique"
        exit 1
    fi
    
    if gcloud storage ls gs://$BUCKET_NAME >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Bucket gs://$BUCKET_NAME already exists${NC}"
    else
        gcloud storage buckets create gs://$BUCKET_NAME --location=$REGION
        echo -e "${GREEN}✓ Created bucket: gs://$BUCKET_NAME${NC}"
    fi
}

# Function to upload files to GCS
upload_files() {
    echo -e "${YELLOW}Uploading application files to GCS...${NC}"
    
    # Create directory structure in bucket and upload files
    gcloud storage cp data.txt gs://$BUCKET_NAME/input/
    gcloud storage cp click_analysis_spark_cloud.py gs://$BUCKET_NAME/scripts/
    gcloud storage cp click_analysis_spark.py gs://$BUCKET_NAME/scripts/
    gcloud storage cp requirements.txt gs://$BUCKET_NAME/scripts/
    gcloud storage cp test_spark_local.py gs://$BUCKET_NAME/scripts/
    gcloud storage cp click_analysis_notebook.ipynb gs://$BUCKET_NAME/notebooks/
    
    echo -e "${GREEN}✓ Files uploaded to GCS:${NC}"
    echo "  - Input data: gs://$BUCKET_NAME/input/data.txt"
    echo "  - Cloud Spark script: gs://$BUCKET_NAME/scripts/click_analysis_spark_cloud.py"
    echo "  - Local Spark script: gs://$BUCKET_NAME/scripts/click_analysis_spark.py"
    echo "  - Jupyter notebook: gs://$BUCKET_NAME/notebooks/click_analysis_notebook.ipynb"
    echo "  - Requirements: gs://$BUCKET_NAME/scripts/requirements.txt"
}

# Function to create Dataproc cluster
create_cluster() {
    echo -e "${YELLOW}Creating Dataproc cluster...${NC}"
    
    if gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Cluster $CLUSTER_NAME already exists${NC}"
    else
        gcloud dataproc clusters create $CLUSTER_NAME \
            --region=$REGION \
            --zone=$ZONE \
            --master-machine-type=$MASTER_MACHINE_TYPE \
            --worker-machine-type=$WORKER_MACHINE_TYPE \
            --num-workers=$WORKER_COUNT \
            --image-version=2.1-debian11 \
            --bucket=$BUCKET_NAME \
            --optional-components=JUPYTER \
            --enable-component-gateway \
            --initialization-actions=gs://goog-dataproc-initialization-actions-$REGION/python/pip-install.sh \
            --metadata="PIP_PACKAGES=matplotlib seaborn pandas jupyter" \
            --tags=spark-cluster
            
        echo -e "${GREEN}✓ Dataproc cluster created: $CLUSTER_NAME${NC}"
    fi
}

# Function to submit Spark job
submit_job() {
    echo -e "${YELLOW}Submitting Spark job to cluster...${NC}"
    
    JOB_ID="click-analysis-job-$(date +%Y%m%d-%H%M%S)"
    
    gcloud dataproc jobs submit pyspark gs://$BUCKET_NAME/scripts/click_analysis_spark_cloud.py \
        --cluster=$CLUSTER_NAME \
        --region=$REGION \
        --properties="spark.submit.deployMode=client,spark.app.name=ClickAnalysisCloud" \
        --async \
        -- --input "gs://$BUCKET_NAME/input/data.txt" --output "gs://$BUCKET_NAME/output/"
    
    echo -e "${GREEN}✓ Spark job submitted successfully${NC}"
    echo -e "${BLUE}Monitor job progress:${NC}"
    echo "gcloud dataproc jobs list --cluster=$CLUSTER_NAME --region=$REGION"
    echo "gcloud dataproc jobs describe <JOB_ID> --region=$REGION"
}

# Function to create Jupyter tunnel
setup_jupyter() {
    echo -e "${YELLOW}Setting up Jupyter notebook access...${NC}"
    
    # Get cluster master instance
    MASTER_INSTANCE=$(gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION --format="value(config.masterConfig.instanceNames[0])")
    
    echo -e "${GREEN}✓ Jupyter notebook will be available via SSH tunnel${NC}"
    echo -e "${BLUE}To access Jupyter notebook:${NC}"
    echo "1. Run this command to create SSH tunnel:"
    echo "   gcloud compute ssh $MASTER_INSTANCE --zone=$ZONE -- -L 8888:localhost:8888"
    echo "2. Open browser to: http://localhost:8888"
    echo "3. Upload the click_analysis_notebook.ipynb file"
}

# Function to show cluster info
show_cluster_info() {
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}                 DEPLOYMENT COMPLETE                      ${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${GREEN}Cluster Information:${NC}"
    echo "  Name: $CLUSTER_NAME"
    echo "  Region: $REGION"
    echo "  Bucket: gs://$BUCKET_NAME"
    echo ""
    echo -e "${GREEN}Useful Commands:${NC}"
    echo "  Monitor jobs:    gcloud dataproc jobs list --region=$REGION"
    echo "  View logs:       gcloud logging read 'resource.type=dataproc_cluster'"
    echo "  SSH to master:   gcloud compute ssh $CLUSTER_NAME-m --zone=$ZONE"
    echo "  Delete cluster:  gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION"
    echo ""
    echo -e "${GREEN}Access Points:${NC}"
    echo "  Spark UI:        http://$(gcloud compute instances describe $CLUSTER_NAME-m --zone=$ZONE --format='value(networkInterfaces[0].accessConfigs[0].natIP)'):4040"
    echo "  Jupyter:         SSH tunnel required (see setup_jupyter function)"
    echo "  GCS Console:     https://console.cloud.google.com/storage/browser/$BUCKET_NAME"
}

# Function to cleanup resources
cleanup() {
    echo -e "${YELLOW}Cleaning up resources...${NC}"
    read -p "Delete Dataproc cluster? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
        echo -e "${GREEN}✓ Cluster deleted${NC}"
    fi
    
    read -p "Delete GCS bucket? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        gcloud storage rm -r gs://$BUCKET_NAME
        echo -e "${GREEN}✓ Bucket deleted${NC}"
    fi
}

# Main execution
main() {
    case "${1:-deploy}" in
        "deploy")
            check_gcloud_auth
            set_project
            enable_apis
            create_bucket
            upload_files
            create_cluster
            submit_job
            setup_jupyter
            show_cluster_info
            ;;
        "cleanup")
            cleanup
            ;;
        "info")
            show_cluster_info
            ;;
        "jupyter")
            setup_jupyter
            ;;
        *)
            echo "Usage: $0 {deploy|cleanup|info|jupyter}"
            echo "  deploy  - Full deployment (default)"
            echo "  cleanup - Delete cluster and bucket"
            echo "  info    - Show cluster information"
            echo "  jupyter - Show Jupyter setup instructions"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 