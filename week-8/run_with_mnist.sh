#!/bin/bash
# Script to setup MNIST data and run Decision Tree CrossValidator on Dataproc
# Author: Abhyudaya B Tharakan 22f3001492

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-steady-triumph-447006-f8}"
REGION="${REGION:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-week8-ml-cluster}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-week8-ml}"

echo "=================================="
echo "MNIST Decision Tree CrossValidator"
echo "=================================="
echo "Project: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Cluster: $CLUSTER_NAME"

# Step 1: Setup MNIST data in GCS
echo ""
echo "📥 Setting up MNIST dataset..."
python setup_mnist_data.py --project-id $PROJECT_ID --bucket $BUCKET_NAME

# Step 2: Upload updated script to GCS
echo ""
echo "📤 Uploading script to GCS..."
gsutil cp src/decision_trees_crossvalidator.py gs://$BUCKET_NAME/scripts/

# Step 3: Submit job with MNIST data paths
echo ""
echo "🚀 Submitting job to Dataproc..."
gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/scripts/decision_trees_crossvalidator.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --properties="spark.executor.memory=2g,spark.driver.memory=2g" \
    -- \
    --train-path gs://$BUCKET_NAME/mnist/mnist_train.txt \
    --test-path gs://$BUCKET_NAME/mnist/mnist_test.txt \
    --cv-folds 3 \
    --output-report gs://$BUCKET_NAME/output/analysis_report.txt

echo ""
echo "✅ Job submitted successfully!"
echo "Monitor progress in the GCP Console"