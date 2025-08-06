#!/usr/bin/env python3
"""
Submit jobs with minimal memory settings for e2-medium cluster
Designed to work with existing cluster constraints
"""

import subprocess
import time

def run_command(cmd, description):
    print(f"\n🔧 {description}")
    result = subprocess.run(cmd, shell=True, text=True)
    if result.returncode == 0:
        print(f"✅ Success")
        return True
    else:
        print(f"❌ Failed")
        return False

def main():
    project_id = "spheric-gecko-467914-f8"
    cluster_name = "week7-streaming-cluster"  
    region = "us-central1"
    bucket_name = f"{project_id}-week7-streaming-data"
    
    print("🚀 SUBMITTING MINIMAL MEMORY JOBS FOR E2-MEDIUM CLUSTER")
    print("=" * 60)
    
    # Submit consumer with MINIMAL memory settings
    print(f"\n📊 Submitting consumer with minimal memory...")
    consumer_cmd = f"""
    gcloud dataproc jobs submit pyspark \\
        gs://{bucket_name}/apps/spark_streaming_consumer.py \\
        --cluster={cluster_name} \\
        --region={region} \\
        --properties="spark.executor.memory=512m,spark.driver.memory=256m,spark.executor.instances=1,spark.executor.cores=1" \\
        -- \\
        --kafka-servers=localhost:9092 \\
        --topic=customer-transactions \\
        --window-duration=10 \\
        --slide-duration=5
    """
    
    if run_command(consumer_cmd, "Submit minimal consumer"):
        print(f"✅ Consumer submitted successfully")
        
        # Wait for consumer to start
        print(f"\n⏰ Waiting 45 seconds for consumer to initialize...")
        time.sleep(45)
        
        # Submit producer with minimal settings
        print(f"\n📤 Submitting producer with minimal memory...")
        producer_cmd = f"""
        gcloud dataproc jobs submit pyspark \\
            gs://{bucket_name}/apps/kafka_producer.py \\
            --cluster={cluster_name} \\
            --region={region} \\
            --properties="spark.executor.memory=512m,spark.driver.memory=256m,spark.executor.instances=1,spark.executor.cores=1" \\
            -- \\
            --data-file=gs://{bucket_name}/input-data/customer_transactions_1200.csv \\
            --topic=customer-transactions \\
            --batch-size=5 \\
            --sleep-seconds=15 \\
            --max-records=50
        """
        
        if run_command(producer_cmd, "Submit minimal producer"):
            print(f"✅ Both jobs submitted with minimal memory settings!")
            print(f"\n📋 Monitor jobs with:")
            print(f"gcloud dataproc jobs list --cluster={cluster_name} --region={region}")
        else:
            print(f"❌ Producer submission failed")
    else:
        print(f"❌ Consumer submission failed - cluster may need more memory")
        print(f"\n💡 RECOMMENDATION: Delete current cluster and create new e2-standard-2 cluster")
        print(f"   gcloud dataproc clusters delete {cluster_name} --region={region} --quiet")
        print(f"   python scripts/deploy_to_dataproc.py --project-id {project_id}")

if __name__ == "__main__":
    main()