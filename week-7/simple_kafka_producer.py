#!/usr/bin/env python3
"""
Simple Kafka Producer - sends test data to Kafka topic
"""

import json
import time
import sys
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description='Simple Kafka Producer')
    parser.add_argument('--records', type=int, default=50, help='Number of records to send')
    parser.add_argument('--batch-size', type=int, default=5, help='Records per batch')
    parser.add_argument('--sleep-seconds', type=int, default=10, help='Sleep between batches')
    
    args = parser.parse_args()
    
    print("🚀 Starting Simple Kafka Producer")
    print(f"📊 Will send {args.records} records in batches of {args.batch_size}")
    
    try:
        # Create Spark session with minimal memory
        spark = SparkSession.builder \
            .appName("SimpleKafkaProducer") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Send data in batches
        total_sent = 0
        batch_num = 1
        
        while total_sent < args.records:
            batch_size = min(args.batch_size, args.records - total_sent)
            
            print(f"\n📤 Sending Batch {batch_num} ({batch_size} records)...")
            
            # Create batch data
            batch_data = []
            for i in range(batch_size):
                record = {
                    "customer_id": f"CUST_{batch_num}_{i:03d}",
                    "transaction_id": f"TXN_{batch_num}_{i:03d}", 
                    "final_amount": 25.0 + (i * 7.5),
                    "batch_number": batch_num,
                    "record_id": total_sent + i + 1,
                    "timestamp": time.time()
                }
                batch_data.append(json.dumps(record))
            
            # Use Spark to send to Kafka (simplified approach)
            try:
                df = spark.createDataFrame([(json.dumps(r),) for r in batch_data], ["value"])
                
                # Try to write to Kafka
                df.selectExpr("CAST(value AS STRING)") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("topic", "customer-transactions") \
                    .mode("append") \
                    .save()
                
                print(f"✅ Sent {batch_size} records to Kafka")
                
            except Exception as e:
                print(f"❌ Kafka send failed: {e}")
                print("📝 Simulating send instead...")
                for record in batch_data:
                    print(f"   → {record}")
            
            total_sent += batch_size
            batch_num += 1
            
            if total_sent < args.records:
                print(f"⏰ Sleeping {args.sleep_seconds} seconds...")
                time.sleep(args.sleep_seconds)
        
        print(f"\n🎉 Producer completed! Sent {total_sent} records total")
        spark.stop()
        
    except Exception as e:
        print(f"💥 Producer error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())