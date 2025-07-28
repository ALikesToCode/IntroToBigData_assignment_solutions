#!/bin/bash
"""
Quick Cloud Deployment Script for Week 6
Course: Introduction to Big Data - Week 6

One-command deployment of the Week 6 real-time line counting pipeline
to Google Cloud Platform.

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

print_status "🚀 Starting Week 6 Cloud Deployment"
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

# Deploy pipeline
print_status "☁️ Deploying Week 6 real-time line counting pipeline..."
print_status "   This includes automatic IAM configuration for GCS triggers..."
python3 scripts/deploy_to_gcloud.py \
    --project-id $PROJECT_ID \
    --region $REGION \
    --verbose

if [ $? -eq 0 ]; then
    print_success "Pipeline deployed successfully!"
else
    print_error "Failed to deploy pipeline"
    exit 1
fi

# Success message
print_status "🎉 Week 6 Cloud Deployment Completed Successfully!"
print_status ""
print_status "📋 Quick Test:"
print_status "1. Upload a test file:"
print_status "   echo -e 'Line 1\\nLine 2\\nLine 3' > test.txt"
print_status "   gsutil cp test.txt gs://$PROJECT_ID-week6-linecount-bucket/"
print_status ""
print_status "2. Watch the results in real-time:"
print_status "   gcloud functions logs read file-upload-processor --region=$REGION --limit=5"
print_status ""
print_status "3. Monitor the subscriber VM:"
print_status "   gcloud compute ssh week6-subscriber-vm-* --zone=$REGION-b"
print_status "   sudo journalctl -u subscriber -f"
print_status ""
print_status "💡 The pipeline will process files in real-time as you upload them!"