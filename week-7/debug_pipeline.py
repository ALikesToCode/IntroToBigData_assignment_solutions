#!/usr/bin/env python3
"""
Debug Week 7 Streaming Pipeline
Check producer status, Kafka topics, and data flow
"""

import subprocess
import json
import time

def run_command(cmd, description):
    """Run a command and return the result."""
    print(f"\n🔍 {description}")
    print(f"   Command: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            if result.stdout.strip():
                print(f"   ✅ Output:\n{result.stdout}")
            else:
                print("   ✅ Command successful (no output)")
        else:
            print(f"   ❌ Error (code {result.returncode}):\n{result.stderr}")
        return result
    except subprocess.TimeoutExpired:
        print(f"   ⏰ Command timed out after 30 seconds")
        return None
    except Exception as e:
        print(f"   💥 Exception: {e}")
        return None

def main():
    project_id = "steady-triumph-447006-f8"  # Update this to your project
    cluster_name = "week7-streaming-cluster"
    region = "us-central1"
    
    print("=" * 80)
    print("🔧 DEBUGGING WEEK 7 STREAMING PIPELINE")
    print("=" * 80)
    
    # 1. Check if cluster exists
    run_command(
        f"gcloud dataproc clusters describe {cluster_name} --region={region} --format='table(clusterName,status.state,config.masterConfig.machineTypeUri,config.workerConfig.numInstances)'",
        "Check cluster status"
    )
    
    # 2. List running jobs
    run_command(
        f"gcloud dataproc jobs list --cluster={cluster_name} --region={region} --format='table(reference.jobId,status.state,driverOutputResourceUri)' --limit=10",
        "List recent Dataproc jobs"
    )
    
    # 3. Check active jobs
    run_command(
        f"gcloud dataproc jobs list --cluster={cluster_name} --region={region} --filter='status.state=RUNNING' --format='table(reference.jobId,jobType,status.state,placement.clusterName)'",
        "Check currently running jobs"
    )
    
    # 4. Check Kafka topics on the cluster
    run_command(
        f'gcloud compute ssh {cluster_name}-m --zone=us-central1-b --command="cd /opt/kafka 2>/dev/null && bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null || echo \'Kafka not found or not running\'"',
        "Check Kafka topics on cluster"
    )
    
    # 5. Check if Kafka is running
    run_command(
        f'gcloud compute ssh {cluster_name}-m --zone=us-central1-b --command="ps aux | grep kafka | head -5"',
        "Check if Kafka process is running"
    )
    
    # 6. Check recent job logs (if any jobs exist)
    print(f"\n📋 Checking recent job logs...")
    jobs_result = subprocess.run(
        f"gcloud dataproc jobs list --cluster={cluster_name} --region={region} --format='value(reference.jobId)' --limit=3",
        shell=True, capture_output=True, text=True
    )
    
    if jobs_result.returncode == 0 and jobs_result.stdout.strip():
        job_ids = jobs_result.stdout.strip().split('\n')
        for job_id in job_ids[:2]:  # Check last 2 jobs
            if job_id.strip():
                run_command(
                    f"gcloud dataproc jobs describe {job_id.strip()} --region={region} --format='value(status.state,status.details)'",
                    f"Check job {job_id.strip()} status"
                )
    
    # 7. Check bucket contents
    bucket_name = f"{project_id}-week7-streaming-data"
    run_command(
        f"gcloud storage ls gs://{bucket_name}/input-data/",
        "Check input data in bucket"
    )
    
    run_command(
        f"gcloud storage ls gs://{bucket_name}/apps/",
        "Check uploaded application files"
    )
    
    print("\n" + "=" * 80)
    print("🎯 DEBUGGING RECOMMENDATIONS:")
    print("=" * 80)
    print("1. If no jobs are running:")
    print("   → The deployment script may have failed to submit jobs")
    print("   → Try running the deployment script again")
    print()
    print("2. If producer job failed:")
    print("   → Check if input data file exists in GCS bucket")
    print("   → Producer needs customer_transactions_1200.csv")
    print()
    print("3. If consumer job failed:")
    print("   → Check if Kafka is running on the cluster")
    print("   → Consumer needs Kafka topic 'customer-transactions'")
    print()
    print("4. If both jobs are running but no data flows:")
    print("   → Producer may have finished before consumer started")
    print("   → Consumer uses 'latest' offset, misses historical data")
    print("   → Try restarting consumer with 'earliest' offset")
    print()
    print("To restart pipeline:")
    print(f"python scripts/deploy_to_dataproc.py --project-id {project_id}")

if __name__ == "__main__":
    main()