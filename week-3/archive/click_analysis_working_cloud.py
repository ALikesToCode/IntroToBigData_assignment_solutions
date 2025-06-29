#!/usr/bin/env python3

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, when, split, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

def main():
    # Fixed paths for GCS
    input_path = "gs://spark-click-analysis-20250629-231200-unique/input/data.txt"
    output_path = "gs://spark-click-analysis-20250629-231200-unique/output/"
    
    print("=" * 70)
    print("🚀 User Click Data Analysis with Apache Spark - Cloud Version")
    print("=" * 70)
    print(f"📂 Input: {input_path}")
    print(f"💾 Output: {output_path}")
    print("-" * 70)
    
    try:
        # Initialize Spark session
        print("⚡ Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("CloudClickAnalysis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        print(f"✅ Spark version: {spark.version}")
        print(f"🌐 Spark UI: {spark.sparkContext.uiWebUrl}")
        print("-" * 70)
        
        print("📊 Loading click data from GCS...")
        
        # Read the data as text first
        df_raw = spark.read.text(input_path)
        
        print(f"📈 Raw records loaded: {df_raw.count()}")
        
        # Parse the data format: "10-Jan 11:10 1001"
        df_parsed = df_raw.select(
            split(col("value"), " ").alias("parts")
        ).select(
            col("parts")[0].alias("date_part"),
            col("parts")[1].alias("time_part"), 
            col("parts")[2].alias("user_id")
        ).filter(
            col("date_part").isNotNull() & 
            col("time_part").isNotNull() & 
            col("user_id").isNotNull()
        )
        
        # Extract hour from time (format: "11:10")
        df_with_hour = df_parsed.withColumn(
            "hour",
            split(col("time_part"), ":")[0].cast("int")
        )
        
        valid_records = df_with_hour.count()
        
        print(f"✅ Valid records: {valid_records}")
        print("-" * 70)
        
        if valid_records > 0:
            # Show sample data
            print("📋 Sample data:")
            df_with_hour.select("date_part", "time_part", "user_id", "hour").show(5, False)
            
            # Time-based analysis (6-hour intervals)
            print("⏰ Performing time-based analysis...")
            
            df_with_intervals = df_with_hour.withColumn(
                "time_interval",
                when((col("hour") >= 0) & (col("hour") < 6), "00-06 (Night)")
                .when((col("hour") >= 6) & (col("hour") < 12), "06-12 (Morning)")
                .when((col("hour") >= 12) & (col("hour") < 18), "12-18 (Afternoon)")
                .when((col("hour") >= 18) & (col("hour") < 24), "18-24 (Evening)")
                .otherwise("Unknown")
            )
            
            # Count clicks per time interval
            interval_analysis = df_with_intervals.groupBy("time_interval") \
                .agg(count("*").alias("click_count")) \
                .orderBy(desc("click_count"))
            
            # User activity analysis
            user_analysis = df_with_hour.groupBy("user_id") \
                .agg(count("*").alias("click_count")) \
                .orderBy(desc("click_count"))
            
            # Show results
            print("\n📊 TIME INTERVAL ANALYSIS:")
            print("=" * 50)
            interval_results = interval_analysis.collect()
            total_clicks = sum(row.click_count for row in interval_results)
            
            for row in interval_results:
                percentage = (row.click_count / total_clicks) * 100 if total_clicks > 0 else 0
                print(f"🕐 {row.time_interval}: {row.click_count} clicks ({percentage:.1f}%)")
            
            print(f"\n📈 Total clicks analyzed: {total_clicks}")
            
            print("\n👥 TOP USER ACTIVITY:")
            print("=" * 50)
            top_users = user_analysis.limit(10).collect()
            for i, row in enumerate(top_users, 1):
                print(f"{i:2d}. 👤 User {row.user_id}: {row.click_count} clicks")
            
            # Save results to GCS
            print(f"\n💾 Saving results to {output_path}")
            
            try:
                # Save interval analysis as single CSV file
                interval_analysis.coalesce(1).write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .csv(f"{output_path}/time_intervals")
                
                # Save user analysis as single CSV file
                user_analysis.coalesce(1).write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .csv(f"{output_path}/user_activity")
                
                # Save summary statistics as text
                summary_data = [
                    f"Total Records: {valid_records}",
                    f"Total Clicks: {total_clicks}",
                    f"Unique Users: {user_analysis.count()}",
                    "",
                    "Time Interval Distribution:"
                ]
                
                for row in interval_results:
                    percentage = (row.click_count / total_clicks) * 100 if total_clicks > 0 else 0
                    summary_data.append(f"{row.time_interval}: {row.click_count} clicks ({percentage:.1f}%)")
                
                summary_df = spark.createDataFrame([(line,) for line in summary_data], ["summary"])
                summary_df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("header", "false") \
                    .text(f"{output_path}/summary")
                    
                print("✅ Results saved successfully!")
                
                # Display final summary
                print("\n🎯 ANALYSIS SUMMARY:")
                print("=" * 50)
                print(f"📊 Dataset: {valid_records} click records")
                print(f"👥 Users: {user_analysis.count()} unique users")
                print(f"⏰ Peak time: {interval_results[0].time_interval} ({interval_results[0].click_count} clicks)")
                print(f"👤 Most active user: {top_users[0].user_id} ({top_users[0].click_count} clicks)")
                
            except Exception as save_error:
                print(f"⚠️  Warning: Could not save some results: {save_error}")
                
        else:
            print("❌ No valid records found to analyze")
        
        print("-" * 70)
        print("🛑 Stopping Spark session...")
        spark.stop()
        print("🎉 Analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 