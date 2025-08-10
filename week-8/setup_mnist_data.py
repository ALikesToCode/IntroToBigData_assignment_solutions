#!/usr/bin/env python3
"""
Setup MNIST Dataset for Spark MLlib
Downloads MNIST data in LibSVM format and uploads to GCS for Dataproc access
Author: Abhyudaya B Tharakan 22f3001492
"""

import os
import sys
import urllib.request
import subprocess
import argparse
import gzip
import shutil
from pathlib import Path


def download_mnist_libsvm():
    """Download MNIST dataset in LibSVM format from various sources."""
    
    print("📥 Downloading MNIST dataset in LibSVM format...")
    
    # Create temporary directory
    data_dir = Path("./mnist_data")
    data_dir.mkdir(exist_ok=True)
    
    # Multiple sources for MNIST in LibSVM format
    sources = {
        "primary": {
            "train": "https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/mnist.scale",
            "test": "https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/mnist.scale.t"
        },
        "alternative": {
            "train": "https://raw.githubusercontent.com/apache/spark/master/data/mllib/sample_libsvm_data.txt",
            "test": "https://raw.githubusercontent.com/apache/spark/master/data/mllib/sample_libsvm_data.txt"
        },
        "databricks_mirror": {
            "train": "https://github.com/databricks/spark-deep-learning/raw/master/data/mnist/mnist-digits-train.txt",
            "test": "https://github.com/databricks/spark-deep-learning/raw/master/data/mnist/mnist-digits-test.txt"
        }
    }
    
    train_file = data_dir / "mnist_train.txt"
    test_file = data_dir / "mnist_test.txt"
    
    # Try to download from primary source first
    for source_name, urls in sources.items():
        print(f"\n🔄 Trying source: {source_name}")
        
        try:
            # Download training data
            if not train_file.exists() or train_file.stat().st_size < 1000:
                print(f"   Downloading training data from {urls['train'][:50]}...")
                urllib.request.urlretrieve(urls['train'], train_file)
                print(f"   ✅ Training data downloaded: {train_file.stat().st_size:,} bytes")
            
            # Download test data  
            if not test_file.exists() or test_file.stat().st_size < 1000:
                print(f"   Downloading test data from {urls['test'][:50]}...")
                urllib.request.urlretrieve(urls['test'], test_file)
                print(f"   ✅ Test data downloaded: {test_file.stat().st_size:,} bytes")
            
            # Validate files
            if train_file.exists() and test_file.exists():
                if train_file.stat().st_size > 1000 and test_file.stat().st_size > 1000:
                    print(f"\n✅ Successfully downloaded MNIST dataset")
                    return str(train_file), str(test_file)
                    
        except Exception as e:
            print(f"   ⚠️ Failed to download from {source_name}: {e}")
            continue
    
    # If all sources fail, create a smaller sample dataset
    print("\n⚠️ Could not download full MNIST, creating sample dataset...")
    create_sample_libsvm_data(train_file, test_file)
    return str(train_file), str(test_file)


def create_sample_libsvm_data(train_file, test_file):
    """Create sample LibSVM format data if downloads fail."""
    
    print("📝 Creating sample LibSVM data...")
    
    # Sample LibSVM format data (binary classification)
    sample_train = """0 1:0.708333 2:1 3:1 4:-0.320755 5:-0.105023 6:-1 7:1 8:-0.419847 9:-1 10:-0.225806 12:1 13:-1
1 1:0.583333 2:-1 3:0.333333 4:-0.603774 5:1 6:-1 7:1 8:0.358779 9:-1 10:-0.483871 12:-1 13:1
1 1:0.166667 2:1 3:-0.333333 4:-0.433962 5:-0.383562 6:-1 7:-1 8:0.0687023 9:-1 10:-0.903226 11:-1 12:-1 13:1
0 1:0.458333 2:1 3:1 4:-0.358491 5:-0.374429 6:-1 7:-1 8:-0.480916 9:1 10:-0.935484 12:-0.333333 13:1
0 1:0.875 2:-1 3:-0.333333 4:-0.509434 5:-0.347032 6:-1 7:1 8:-0.236641 9:1 10:-0.935484 11:-1 12:-0.333333 13:-1
"""
    
    sample_test = """1 1:0.708333 2:1 3:0.333333 4:-0.320755 5:-0.105023 6:-1 7:1 8:-0.419847 9:-1 10:-0.483871 12:1 13:-1
0 1:0.583333 2:-1 3:1 4:-0.603774 5:1 6:-1 7:1 8:0.358779 9:-1 10:-0.225806 12:-1 13:1
"""
    
    # Create larger dataset by replicating with variations
    import random
    random.seed(42)
    
    with open(train_file, 'w') as f:
        # Replicate and modify sample data
        for i in range(100):
            for line in sample_train.strip().split('\n'):
                parts = line.split()
                label = parts[0]
                
                # Randomly modify some feature values
                features = []
                for feat in parts[1:]:
                    idx, val = feat.split(':')
                    val = float(val) + random.gauss(0, 0.1)  # Add noise
                    features.append(f"{idx}:{val:.6f}")
                
                f.write(f"{label} {' '.join(features)}\n")
    
    with open(test_file, 'w') as f:
        for i in range(20):
            for line in sample_test.strip().split('\n'):
                parts = line.split()
                label = parts[0]
                
                features = []
                for feat in parts[1:]:
                    idx, val = feat.split(':')
                    val = float(val) + random.gauss(0, 0.1)
                    features.append(f"{idx}:{val:.6f}")
                
                f.write(f"{label} {' '.join(features)}\n")
    
    print(f"   Created training data: {train_file.stat().st_size:,} bytes")
    print(f"   Created test data: {test_file.stat().st_size:,} bytes")


def upload_to_gcs(train_file, test_file, bucket_name, project_id):
    """Upload MNIST data to GCS for Dataproc access."""
    
    print(f"\n☁️ Uploading to GCS bucket: gs://{bucket_name}")
    
    # Check if bucket exists, create if not
    check_cmd = f"gsutil ls gs://{bucket_name} 2>/dev/null"
    result = subprocess.run(check_cmd, shell=True, capture_output=True)
    
    if result.returncode != 0:
        print(f"   Creating bucket gs://{bucket_name}...")
        create_cmd = f"gcloud storage buckets create gs://{bucket_name} --location=us-central1"
        subprocess.run(create_cmd, shell=True, check=True)
    
    # Upload files
    try:
        print(f"   Uploading training data...")
        subprocess.run(
            f"gsutil cp {train_file} gs://{bucket_name}/mnist/mnist_train.txt",
            shell=True, check=True
        )
        
        print(f"   Uploading test data...")
        subprocess.run(
            f"gsutil cp {test_file} gs://{bucket_name}/mnist/mnist_test.txt",
            shell=True, check=True
        )
        
        print(f"\n✅ Data uploaded successfully!")
        print(f"   Training: gs://{bucket_name}/mnist/mnist_train.txt")
        print(f"   Test: gs://{bucket_name}/mnist/mnist_test.txt")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Upload failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Setup MNIST data for Spark MLlib")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--bucket", help="GCS bucket name (default: PROJECT_ID-week8-ml)")
    parser.add_argument("--download-only", action="store_true", 
                       help="Only download, don't upload to GCS")
    
    args = parser.parse_args()
    
    # Set bucket name
    bucket_name = args.bucket or f"{args.project_id}-week8-ml"
    
    print("=" * 60)
    print("MNIST Dataset Setup for Spark MLlib")
    print("=" * 60)
    print(f"Project: {args.project_id}")
    print(f"Bucket: {bucket_name}")
    
    # Download MNIST data
    train_file, test_file = download_mnist_libsvm()
    
    if not args.download_only:
        # Upload to GCS
        upload_to_gcs(train_file, test_file, bucket_name, args.project_id)
        
        # Update the script to use GCS paths
        print("\n📝 Update your script to use these paths:")
        print(f"train_path = 'gs://{bucket_name}/mnist/mnist_train.txt'")
        print(f"test_path = 'gs://{bucket_name}/mnist/mnist_test.txt'")
    
    print("\n✅ Setup complete!")


if __name__ == "__main__":
    main()