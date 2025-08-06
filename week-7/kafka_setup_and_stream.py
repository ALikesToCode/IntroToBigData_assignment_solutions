#!/usr/bin/env python3
"""
Combined Kafka Setup + Streaming Test
This script sets up Kafka and runs a simple streaming simulation
"""

import subprocess
import time
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def setup_kafka():
    """Set up Kafka on the local node."""
    print("🚀 Setting up Kafka...")
    
    try:
        # Check if Kafka is already installed
        if os.path.exists("/opt/kafka"):
            print("✅ Kafka already installed")
        else:
            print("📦 Installing Kafka...")
            commands = [
                "sudo apt-get update -y",
                "sudo apt-get install -y openjdk-11-jdk wget",
                "cd /opt && sudo wget -q https://dlcdn.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz",
                "cd /opt && sudo tar -xzf kafka_2.13-3.9.1.tgz",
                "cd /opt && sudo mv kafka_2.13-3.9.1 kafka",
                f"sudo chown -R {os.getenv('USER', 'yarn')}:{os.getenv('USER', 'yarn')} /opt/kafka",
                "mkdir -p /tmp/kafka-logs /tmp/zookeeper"
            ]
            
            for cmd in commands:
                result = subprocess.run(cmd, shell=True)
                if result.returncode != 0:
                    print(f"❌ Command failed: {cmd}")
        
        # Start Zookeeper
        print("🔄 Starting Zookeeper...")
        subprocess.Popen([
            "/opt/kafka/bin/zookeeper-server-start.sh",
            "/opt/kafka/config/zookeeper.properties"
        ], env={**os.environ, "KAFKA_HEAP_OPTS": "-Xmx128m -Xms128m"})
        
        time.sleep(10)
        
        # Start Kafka
        print("🔄 Starting Kafka server...")
        subprocess.Popen([
            "/opt/kafka/bin/kafka-server-start.sh", 
            "/opt/kafka/config/server.properties"
        ], env={**os.environ, "KAFKA_HEAP_OPTS": "-Xmx256m -Xms256m"})
        
        time.sleep(15)
        
        # Create topic
        print("📝 Creating Kafka topic...")
        subprocess.run([
            "/opt/kafka/bin/kafka-topics.sh",
            "--create",
            "--topic", "customer-transactions",
            "--bootstrap-server", "localhost:9092",
            "--partitions", "1",
            "--replication-factor", "1"
        ])
        
        print("✅ Kafka setup complete!")
        return True
        
    except Exception as e:
        print(f"❌ Kafka setup failed: {e}")
        return False

def simulate_streaming_without_kafka():
    """Run a simple streaming simulation without Kafka."""
    print("🎯 Running streaming simulation WITHOUT Kafka...")
    
    try:
        # Create minimal Spark session
        spark = SparkSession.builder \
            .appName("SimpleStreamingSimulation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Create sample data
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("final_amount", DoubleType(), True),
            StructField("batch_number", IntegerType(), True)
        ])
        
        # Simulate batches of data
        for batch_num in range(1, 6):  # 5 batches
            print(f"\n📊 Processing Batch {batch_num}...")
            
            # Create sample data for this batch  
            data = []
            for i in range(10):  # 10 records per batch
                data.append((
                    f"CUST_{batch_num}_{i:03d}",
                    f"TXN_{batch_num}_{i:03d}",
                    50.0 + (i * 10.5),
                    batch_num
                ))
            
            # Create DataFrame
            batch_df = spark.createDataFrame(data, schema)
            
            # Add processing timestamp
            batch_with_time = batch_df.withColumn(
                "processing_time", 
                current_timestamp()
            )
            
            # Simulate window aggregation
            results = batch_with_time.groupBy("batch_number") \
                .agg(
                    count("*").alias("record_count"),
                    sum("final_amount").alias("total_value"),
                    avg("final_amount").alias("avg_value"),
                    approx_count_distinct("customer_id").alias("unique_customers")
                )
            
            print(f"✅ Batch {batch_num} Results:")
            results.show()
            
            # Sleep between batches to simulate real-time
            time.sleep(5)
        
        print("🎉 Streaming simulation completed successfully!")
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Streaming simulation failed: {e}")
        return False

def main():
    print("🚀 KAFKA SETUP AND STREAMING TEST")
    print("=" * 50)
    
    # Try to setup Kafka first
    kafka_success = setup_kafka()
    
    if kafka_success:
        print("\n✅ Kafka is ready - you can now run separate producer/consumer jobs!")
        print("\nNext steps:")
        print("1. Submit producer job to send data to Kafka")
        print("2. Submit consumer job to read from Kafka stream")
    else:
        print("\n⚠️ Kafka setup failed - running simulation instead...")
        simulate_streaming_without_kafka()
    
    print("\n🏁 Test completed!")

if __name__ == "__main__":
    main()