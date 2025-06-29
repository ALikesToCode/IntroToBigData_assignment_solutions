#!/usr/bin/env python3
"""
Google Cloud Dataproc Spark Application: Click Data Analysis
Optimized for cloud deployment with GCS integration
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os
import argparse
import logging
from datetime import datetime

# Configure logging for cloud environment
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_time_interval(time_str):
    """Categorize time into 6-hour intervals."""
    try:
        hour = int(time_str.split(':')[0])
        if 0 <= hour < 6:
            return "0-6"
        elif 6 <= hour < 12:
            return "6-12"
        elif 12 <= hour < 18:
            return "12-18"
        elif 18 <= hour < 24:
            return "18-24"
        else:
            return "invalid"
    except:
        return "invalid"

def parse_line(line):
    """Parse a data line into components."""
    try:
        parts = line.strip().split()
        if len(parts) == 3:
            date_part, time_part, user_id = parts
            interval = get_time_interval(time_part)
            if interval != "invalid":
                return (date_part, time_part, user_id, interval)
    except:
        pass
    return None

def create_spark_session(app_name="CloudClickAnalysis"):
    """Create optimized Spark session for cloud deployment."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def analyze_click_data(spark, input_path, output_path):
    """Main analysis function for click data."""
    logger.info(f"Starting click data analysis")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    
    # Load and process data using RDD approach
    logger.info("Loading data from GCS...")
    raw_rdd = spark.sparkContext.textFile(input_path)
    total_lines = raw_rdd.count()
    logger.info(f"Total lines loaded: {total_lines}")
    
    # Process data
    logger.info("Processing data...")
    parsed_rdd = raw_rdd.map(parse_line).filter(lambda x: x is not None)
    valid_records = parsed_rdd.count()
    logger.info(f"Valid records: {valid_records}")
    
    # Create DataFrame for advanced analysis
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("time_interval", StringType(), True)
    ])
    
    df = spark.createDataFrame(parsed_rdd, schema)
    df.cache()  # Cache for multiple operations
    
    # Basic aggregation using RDD
    interval_rdd = parsed_rdd.map(lambda x: (x[3], 1))
    click_counts = interval_rdd.reduceByKey(lambda a, b: a + b).collect()
    results_dict = dict(click_counts)
    
    # Advanced analysis using DataFrame
    logger.info("Performing advanced analysis...")
    df.createOrReplaceTempView("click_data")
    
    # Comprehensive analysis query
    analysis_results = spark.sql("""
        SELECT 
            time_interval,
            COUNT(*) as click_count,
            COUNT(DISTINCT user_id) as unique_users,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
            ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_clicks_per_user,
            MIN(time) as earliest_time,
            MAX(time) as latest_time
        FROM click_data 
        GROUP BY time_interval 
        ORDER BY time_interval
    """)
    
    # User activity analysis
    user_analysis = spark.sql("""
        SELECT 
            user_id,
            COUNT(*) as total_clicks,
            COUNT(DISTINCT time_interval) as active_intervals,
            COLLECT_LIST(time_interval) as intervals
        FROM click_data 
        GROUP BY user_id 
        ORDER BY total_clicks DESC
        LIMIT 10
    """)
    
    # Generate comprehensive results
    logger.info("Generating results...")
    results = []
    results.append("=" * 80)
    results.append("GOOGLE CLOUD DATAPROC - USER CLICK DATA ANALYSIS")
    results.append("=" * 80)
    results.append(f"Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    results.append(f"Input data source: {input_path}")
    results.append(f"Total records processed: {total_lines}")
    results.append(f"Valid records analyzed: {valid_records}")
    results.append(f"Success rate: {(valid_records/total_lines)*100:.1f}%")
    results.append("")
    
    # Basic click count analysis
    results.append("CLICK COUNT BY TIME INTERVAL")
    results.append("-" * 40)
    for interval in ["0-6", "6-12", "12-18", "18-24"]:
        count = results_dict.get(interval, 0)
        percentage = (count / valid_records * 100) if valid_records > 0 else 0
        results.append(f"{interval:>6}: {count:>3} clicks ({percentage:>5.1f}%)")
    results.append("")
    
    # Advanced analysis results
    results.append("DETAILED ANALYSIS RESULTS")
    results.append("-" * 40)
    analysis_data = analysis_results.collect()
    for row in analysis_data:
        results.append(f"Interval {row['time_interval']}:")
        results.append(f"  Clicks: {row['click_count']}")
        results.append(f"  Unique Users: {row['unique_users']}")
        results.append(f"  Percentage: {row['percentage']}%")
        results.append(f"  Avg Clicks/User: {row['avg_clicks_per_user']}")
        results.append(f"  Time Range: {row['earliest_time']} - {row['latest_time']}")
        results.append("")
    
    # Top users analysis
    results.append("TOP 10 MOST ACTIVE USERS")
    results.append("-" * 40)
    user_data = user_analysis.collect()
    for i, row in enumerate(user_data, 1):
        results.append(f"{i:>2}. User {row['user_id']}: {row['total_clicks']} clicks across {row['active_intervals']} intervals")
    results.append("")
    
    # Peak activity insights
    peak_interval = max(results_dict.items(), key=lambda x: x[1])
    results.append("BUSINESS INSIGHTS")
    results.append("-" * 40)
    results.append(f"Peak Activity Period: {peak_interval[0]} hours")
    results.append(f"Peak Activity Count: {peak_interval[1]} clicks")
    results.append(f"Peak Represents: {(peak_interval[1]/valid_records)*100:.1f}% of total activity")
    results.append("")
    results.append("RECOMMENDATIONS:")
    results.append(f"- Schedule marketing campaigns during {peak_interval[0]} hours")
    results.append(f"- Optimize server capacity for {peak_interval[0]} peak demand")
    results.append("- Plan maintenance during low-activity periods (0-6 hours)")
    results.append("- Consider user engagement strategies for off-peak hours")
    results.append("")
    
    # Spark execution details
    results.append("SPARK EXECUTION DETAILS")
    results.append("-" * 40)
    results.append(f"Spark Version: {spark.version}")
    results.append(f"Application ID: {spark.sparkContext.applicationId}")
    results.append(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")
    results.append(f"Executor Memory: {spark.sparkContext.getConf().get('spark.executor.memory', 'default')}")
    results.append("=" * 80)
    
    # Save results to GCS
    logger.info(f"Saving results to: {output_path}")
    result_text = "\n".join(results)
    
    # Convert to RDD and save
    result_rdd = spark.sparkContext.parallelize([result_text])
    result_rdd.saveAsTextFile(output_path)
    
    # Also save structured data as Parquet
    parquet_path = output_path.replace("txt", "parquet")
    logger.info(f"Saving structured data to: {parquet_path}")
    analysis_results.write.mode("overwrite").parquet(parquet_path)
    
    # Save user analysis
    user_parquet_path = output_path.replace("output", "user_analysis").replace("txt", "parquet")
    logger.info(f"Saving user analysis to: {user_parquet_path}")
    user_analysis.write.mode("overwrite").parquet(user_parquet_path)
    
    logger.info("Analysis completed successfully!")
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("CLICK ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"Total Events: {valid_records}")
    print(f"Peak Period: {peak_interval[0]} hours ({peak_interval[1]} clicks)")
    print(f"Success Rate: {(valid_records/total_lines)*100:.1f}%")
    print("=" * 60)
    
    return results_dict

def main():
    parser = argparse.ArgumentParser(description="Cloud Click Data Analysis with Apache Spark")
    parser.add_argument("--input", default="gs://your-bucket/input/data.txt", 
                       help="Input data path in GCS")
    parser.add_argument("--output", default="gs://your-bucket/output/analysis_results.txt",
                       help="Output path in GCS")
    parser.add_argument("--app-name", default="CloudClickAnalysis",
                       help="Spark application name")
    
    args = parser.parse_args()
    
    # Create Spark session
    logger.info("Initializing Spark session...")
    spark = create_spark_session(args.app_name)
    spark.sparkContext.setLogLevel("WARN")  # Reduce verbose logging
    
    try:
        # Run analysis
        results = analyze_click_data(spark, args.input, args.output)
        
        logger.info("Application completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Clean up
        logger.info("Stopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    sys.exit(main()) 