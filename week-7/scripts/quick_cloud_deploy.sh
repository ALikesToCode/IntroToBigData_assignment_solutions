#!/bin/bash
"""
Quick Cloud Deployment Script for Week 7
Course: Introduction to Big Data - Week 7

This script provides one-command deployment of the entire Week 7 streaming pipeline
to Google Cloud Platform using Dataproc.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[$(date +'%H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if project ID is provided
if [ $# -eq 0 ]; then
    print_error "Usage: $0 <PROJECT_ID> [REGION]"
    print_error "Example: $0 my-bigdata-project-2025 us-central1"
    exit 1
fi

PROJECT_ID=$1
REGION=${2:-us-central1}

print_status "🚀 Starting Week 7 Cloud Deployment"
print_status "   Project ID: $PROJECT_ID"
print_status "   Region: $REGION"

# Check prerequisites
print_status "🔍 Checking prerequisites..."

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI is not installed. Please install Google Cloud SDK first."
    exit 1
fi

# Check if authenticated
ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null)
if [ -z "$ACTIVE_ACCOUNT" ]; then
    print_error "Not authenticated with Google Cloud. Please run:"
    print_error "  gcloud auth login"
    print_error "  gcloud auth application-default login"
    exit 1
fi

print_success "Authenticated as: $ACTIVE_ACCOUNT"

# Set project
print_status "⚙️ Setting up project configuration..."
gcloud config set project $PROJECT_ID
print_success "Project set to: $PROJECT_ID"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

print_success "Prerequisites validated"

# Step 1: Generate and upload data
print_status "📊 Step 1: Generating and uploading data to GCS..."
python3 scripts/upload_data_to_gcs.py \
    --project-id $PROJECT_ID \
    --generate-data \
    --verbose

if [ $? -eq 0 ]; then
    print_success "Data generated and uploaded to GCS"
else
    print_error "Failed to upload data to GCS"
    exit 1
fi

# Step 2: Deploy to Dataproc
print_status "🏗️ Step 2: Deploying streaming pipeline to Dataproc..."
python3 scripts/deploy_to_dataproc.py \
    --project-id $PROJECT_ID \
    --region $REGION \
    --verbose

if [ $? -eq 0 ]; then
    print_success "Pipeline deployed successfully to Dataproc"
else
    print_error "Failed to deploy pipeline to Dataproc"
    exit 1
fi

# Success message
print_status "🎉 Week 7 Cloud Deployment Completed Successfully!"
print_status ""
print_status "📋 Next Steps:"
print_status "1. Monitor jobs in Google Cloud Console:"
print_status "   https://console.cloud.google.com/dataproc/clusters?project=$PROJECT_ID"
print_status ""
print_status "2. View job logs:"
print_status "   gcloud dataproc jobs list --region=$REGION"
print_status ""
print_status "3. Clean up resources when done:"
print_status "   python3 scripts/deploy_to_dataproc.py --project-id $PROJECT_ID --cleanup-only"
print_status ""
print_status "💡 The streaming pipeline will run for approximately 17 minutes"
print_status "   (1000 records ÷ 10 records/batch × 10 seconds/batch = ~17 minutes)"