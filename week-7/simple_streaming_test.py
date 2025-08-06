#!/usr/bin/env python3
"""
Simple Streaming Test for Week 7 - No Kafka Required
This creates a basic streaming simulation that can run on small Dataproc instances.

Author: Week 7 Big Data Assignment
"""

import time
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse

def create_spark_session():
    """Create Spark session with minimal memory requirements."""
    return SparkSession.builder \
        .appName("SimpleStreamingTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def simulate_streaming_data(spark, data_file):
    """Simulate streaming by reading CSV data in batches."""
    
    print("📊 Reading data file...")
    df = spark.read.csv(data_file, header=True, inferSchema=True)
    
    print(f"📄 Total records: {df.count()}")
    print("📋 Schema:")
    df.printSchema()
    
    # Show sample data
    print("🔍 Sample data:")
    df.show(5, truncate=False)
    
    # Simulate windowed processing
    print("\n🔄 Simulating windowed aggregations...")
    
    # Group by product category (simulating windowed aggregation)
    category_stats = df.groupBy("product_category") \
        .agg(
            count("*").alias("record_count"),
            sum("final_amount").alias("total_value"),
            avg("final_amount").alias("avg_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy(desc("record_count"))
    
    print("📈 Category-wise statistics (simulating window aggregation):")
    category_stats.show(10)
    
    # Simulate time-based windowing
    print("\n⏰ Simulating time-based windows...")
    
    # Add processing timestamp
    df_with_time = df.withColumn("processing_time", current_timestamp())
    
    # Show final results
    print(f"✅ Successfully processed {df.count()} records")
    print("🎯 Streaming simulation completed!")
    
    return True

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Simple Streaming Test')
    parser.add_argument('--data-file', required=True, help='Path to CSV data file')
    
    args = parser.parse_args()
    
    print("🚀 Starting Simple Streaming Test...")
    print(f"📁 Data file: {args.data_file}")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        print("✅ Spark session created")
        
        # Simulate streaming
        success = simulate_streaming_data(spark, args.data_file)
        
        if success:
            print("🎉 Test completed successfully!")
        else:
            print("❌ Test failed")
            
    except Exception as e:
        print(f"💥 Error: {e}")
        return 1
    
    finally:
        if 'spark' in locals():
            spark.stop()
            print("🔒 Spark session stopped")
    
    return 0

if __name__ == '__main__':
    exit(main())