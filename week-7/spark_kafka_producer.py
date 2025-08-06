#!/usr/bin/env python3
"""
Spark-native Kafka Producer for Week 7 Real-time Streaming Assignment
Uses Spark's built-in Kafka integration instead of kafka-python library

Author: Abhyudaya B Tharakan 22f3001492
Date: August 2025
"""

import csv
import json
import time
import logging
import argparse
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the Spark-based Kafka producer.
    """
    parser = argparse.ArgumentParser(description='Spark Kafka Producer for Customer Transactions')
    parser.add_argument('--data-file', required=True, help='Path to the CSV data file')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='customer-transactions', help='Kafka topic name')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of records per batch')
    parser.add_argument('--sleep-seconds', type=int, default=10, help='Sleep time between batches')
    parser.add_argument('--max-records', type=int, default=100, help='Maximum number of records to send')
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting Spark Kafka Producer")
    logger.info(f"📁 Data file: {args.data_file}")
    logger.info(f"🔗 Kafka servers: {args.kafka_servers}")
    logger.info(f"📋 Topic: {args.topic}")
    logger.info(f"📦 Batch size: {args.batch_size}")
    logger.info(f"⏰ Sleep between batches: {args.sleep_seconds}s")
    logger.info(f"📊 Max records: {args.max_records}")
    
    try:
        # Create Spark session with Kafka support
        spark = SparkSession.builder \
            .appName("SparkKafkaProducer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session created")
        
        # Read CSV data
        logger.info("📖 Reading CSV data...")
        df = spark.read.csv(args.data_file, header=True, inferSchema=True)
        total_records = df.count()
        logger.info(f"📊 Total records available: {total_records}")
        
        # Limit to max_records
        limited_df = df.limit(args.max_records)
        actual_records = limited_df.count()
        logger.info(f"📊 Records to process: {actual_records}")
        
        # Add producer metadata
        enhanced_df = limited_df.withColumn("producer_timestamp", unix_timestamp()) \
                               .withColumn("producer_timestamp_str", 
                                         date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        
        # Collect data for batching
        data_rows = enhanced_df.collect()
        logger.info(f"📦 Data collected for batching")
        
        # Send data in batches
        total_sent = 0
        batch_num = 1
        
        for i in range(0, len(data_rows), args.batch_size):
            batch_data = data_rows[i:i + args.batch_size]
            
            logger.info(f"\n{'='*60}")
            logger.info(f"📤 SENDING BATCH {batch_num} ({len(batch_data)} records)")
            logger.info(f"{'='*60}")
            
            # Convert batch to JSON strings for Kafka
            batch_json = []
            for row in batch_data:
                # Convert Row to dictionary then to JSON
                row_dict = row.asDict()
                # Add batch metadata
                row_dict['batch_number'] = batch_num
                row_dict['batch_position'] = len(batch_json) + 1
                
                json_str = json.dumps(row_dict, default=str)
                batch_json.append((json_str,))
            
            # Create DataFrame with JSON strings
            batch_df = spark.createDataFrame(batch_json, ["value"])
            
            try:
                # Write to Kafka using Spark's native integration
                logger.info(f"🔄 Writing batch {batch_num} to Kafka topic '{args.topic}'...")
                
                batch_df.selectExpr("CAST(value AS STRING) as value") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", args.kafka_servers) \
                    .option("topic", args.topic) \
                    .mode("append") \
                    .save()
                
                total_sent += len(batch_data)
                logger.info(f"✅ Batch {batch_num} sent successfully!")
                logger.info(f"📊 Records sent: {len(batch_data)}")
                logger.info(f"📊 Total sent: {total_sent}/{actual_records}")
                
                # Show sample of what was sent
                logger.info("📋 Sample records sent:")
                for j, row in enumerate(batch_data[:3]):  # Show first 3
                    logger.info(f"   {j+1}. Customer: {row.customer_id}, Amount: ${row.final_amount}")
                
            except Exception as e:
                logger.error(f"❌ Failed to send batch {batch_num} to Kafka: {e}")
                logger.info("📝 This could be because Kafka is not running on localhost:9092")
                logger.info("   Check if Kafka is installed and started on the cluster")
                break
            
            batch_num += 1
            
            # Sleep between batches (except for the last batch)
            if i + args.batch_size < len(data_rows):
                logger.info(f"⏰ Sleeping for {args.sleep_seconds} seconds...")
                time.sleep(args.sleep_seconds)
        
        # Final statistics
        logger.info(f"\n{'='*60}")
        logger.info(f"🎉 PRODUCER COMPLETED!")
        logger.info(f"{'='*60}")
        logger.info(f"📊 Total records sent: {total_sent}")
        logger.info(f"📦 Total batches sent: {batch_num - 1}")
        logger.info(f"📋 Topic: {args.topic}")
        logger.info(f"🔗 Kafka servers: {args.kafka_servers}")
        
        if total_sent == 0:
            logger.warning("⚠️ No records were sent - check Kafka connection")
        
        spark.stop()
        logger.info("✅ Spark session stopped")
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()