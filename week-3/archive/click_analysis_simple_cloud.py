#!/usr/bin/env python3

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum as spark_sum, when, hour, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging

def main():
    # Fixed paths for GCS
    input_path = "gs://spark-click-analysis-20250629-231200-unique/input/data.txt"
    output_path = "gs://spark-click-analysis-20250629-231200-unique/output/"
    
    print("=" * 60)
    print("User Click Data Analysis with Apache Spark - Cloud Version")
    print("=" * 60)
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print("-" * 60)
    
    try:
        # Initialize Spark session
        print("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("CloudClickAnalysis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        print(f"Spark version: {spark.version}")
        print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
        print("-" * 60)
        
        # Define schema for the data
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("time", StringType(), True),
            StructField("user_id", StringType(), True)
        ])
        
        print("Loading click data from GCS...")
        
        # Read the data from GCS
        df = spark.read \
            .option("delimiter", " ") \
            .option("header", "false") \
            .schema(schema) \
            .csv(input_path)
        
        # Filter out invalid records
        df_valid = df.filter(
            col("date").isNotNull() & 
            col("time").isNotNull() & 
            col("user_id").isNotNull() &
            col("date").rlike(r"^\d{4}-\d{2}-\d{2}$") &
            col("time").rlike(r"^\d{2}:\d{2}:\d{2}$")
        )
        
        total_records = df.count()
        valid_records = df_valid.count()
        
        print(f"Total records loaded: {total_records}")
        print(f"Valid records: {valid_records}")
        print(f"Invalid records filtered: {total_records - valid_records}")
        print("-" * 60)
        
        if valid_records > 0:
            # Create timestamp column
            df_with_timestamp = df_valid.withColumn(
                "timestamp",
                col("date").cast("timestamp")
            ).withColumn(
                "hour",
                hour(col("timestamp"))
            )
            
            # Time-based analysis
            print("Performing time-based analysis...")
            
            # Define time intervals (6-hour blocks)
            df_with_intervals = df_with_timestamp.withColumn(
                "time_interval",
                when((col("hour") >= 0) & (col("hour") < 6), "00-06")
                .when((col("hour") >= 6) & (col("hour") < 12), "06-12")
                .when((col("hour") >= 12) & (col("hour") < 18), "12-18")
                .when((col("hour") >= 18) & (col("hour") < 24), "18-24")
                .otherwise("Unknown")
            )
            
            # Count clicks per time interval
            interval_analysis = df_with_intervals.groupBy("time_interval") \
                .agg(count("*").alias("click_count")) \
                .orderBy(desc("click_count"))
            
            # User activity analysis
            user_analysis = df_valid.groupBy("user_id") \
                .agg(count("*").alias("click_count")) \
                .orderBy(desc("click_count"))
            
            # Show results
            print("\n📊 TIME INTERVAL ANALYSIS:")
            print("=" * 40)
            interval_results = interval_analysis.collect()
            total_clicks = sum(row.click_count for row in interval_results)
            
            for row in interval_results:
                percentage = (row.click_count / total_clicks) * 100 if total_clicks > 0 else 0
                print(f"{row.time_interval}: {row.click_count} clicks ({percentage:.1f}%)")
            
            print(f"\nTotal clicks analyzed: {total_clicks}")
            
            print("\n👥 TOP USER ACTIVITY:")
            print("=" * 40)
            top_users = user_analysis.limit(5).collect()
            for i, row in enumerate(top_users, 1):
                print(f"{i}. User {row.user_id}: {row.click_count} clicks")
            
            # Save results to GCS
            print(f"\n💾 Saving results to {output_path}")
            
            # Save interval analysis
            interval_analysis.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{output_path}/time_intervals")
            
            # Save user analysis  
            user_analysis.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{output_path}/user_activity")
                
            print("✅ Results saved successfully!")
            
        else:
            print("❌ No valid records found to analyze")
        
        print("-" * 60)
        print("🛑 Stopping Spark session...")
        spark.stop()
        print("✅ Analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 