#!/usr/bin/env python3
"""
Simple Kafka Consumer - reads from Kafka and shows aggregated results
"""

import time
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    parser = argparse.ArgumentParser(description='Simple Kafka Consumer')
    parser.add_argument('--window-duration', type=int, default=30, help='Window duration in seconds')
    parser.add_argument('--slide-duration', type=int, default=10, help='Slide duration in seconds')
    
    args = parser.parse_args()
    
    print("🚀 Starting Simple Kafka Consumer")
    print(f"📊 Window: {args.window_duration}s, Slide: {args.slide_duration}s")
    
    try:
        # Create Spark session with streaming support
        spark = SparkSession.builder \
            .appName("SimpleKafkaConsumer") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka-checkpoint") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        print("📡 Connecting to Kafka stream...")
        
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "customer-transactions") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✅ Connected to Kafka!")
        
        # Parse JSON messages
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("final_amount", DoubleType(), True),
            StructField("batch_number", IntegerType(), True),
            StructField("record_id", IntegerType(), True),
            StructField("timestamp", DoubleType(), True)
        ])
        
        # Parse and process stream
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Add processing timestamp
        with_time = parsed_df.withColumn(
            "processing_time", 
            current_timestamp()
        )
        
        print("🔄 Starting windowed aggregation...")
        
        # Windowed aggregation
        windowed_counts = with_time \
            .withWatermark("processing_time", "1 minute") \
            .groupBy(
                window(col("processing_time"), f"{args.window_duration} seconds", f"{args.slide_duration} seconds")
            ) \
            .agg(
                count("*").alias("record_count"),
                sum("final_amount").alias("total_value"),
                avg("final_amount").alias("avg_value"),
                approx_count_distinct("customer_id").alias("unique_customers"),
                max("batch_number").alias("max_batch"),
                min("batch_number").alias("min_batch")
            )
        
        # Output query
        query = windowed_counts.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime=f"{args.slide_duration} seconds") \
            .start()
        
        print("📊 Streaming query started - watching for data...")
        print("   Press Ctrl+C to stop")
        
        # Wait for termination
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⚠️ Stopping consumer...")
            query.stop()
            
        print("✅ Consumer stopped")
        spark.stop()
        
    except Exception as e:
        print(f"💥 Consumer error: {e}")
        # If Kafka connection fails, show a helpful message
        if "Connection refused" in str(e) or "could not be established" in str(e):
            print("\n🔧 KAFKA CONNECTION FAILED")
            print("Kafka is not running on localhost:9092")
            print("\nTo start Kafka:")
            print("1. SSH to cluster: gcloud compute ssh week7-streaming-cluster-m --zone=us-central1-b")  
            print("2. Setup Kafka: bash /path/to/setup_kafka_manual.sh")
            print("3. Start services manually")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())