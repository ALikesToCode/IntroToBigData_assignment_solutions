#!/usr/bin/env python3
"""
SCD Type II Implementation using PySpark
Course: Introduction to Big Data - Week 4 Assignment

This script implements Slowly Changing Dimensions Type II logic for customer master data
using PySpark DataFrame operations (no SparkSQL).

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, max as spark_max, row_number, coalesce, 
    date_format, to_date, current_date, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, DateType
)
from pyspark.sql.window import Window
import logging

def setup_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def create_spark_session(app_name="SCD_Type2_Implementation"):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_existing_data(spark, file_path):
    """Load existing customer dimension data"""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading existing customer data from: {file_path}")
    
    try:
        # Define explicit schema for existing data
        existing_schema = StructType([
            StructField("customer_key", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("customer_name", StringType(), False),
            StructField("address", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("email", StringType(), False),
            StructField("effective_start_date", StringType(), False),
            StructField("effective_end_date", StringType(), False),
            StructField("is_current", BooleanType(), False)
        ])
        
        df = spark.read \
            .option("header", "true") \
            .schema(existing_schema) \
            .csv(file_path)
        
        # Convert date columns and trim whitespace
        df = df.withColumn("effective_start_date", to_date(trim(col("effective_start_date")), "yyyy-MM-dd")) \
              .withColumn("effective_end_date", to_date(trim(col("effective_end_date")), "yyyy-MM-dd"))
        
        logger.info(f"Loaded {df.count()} existing customer records")
        return df
    except Exception as e:
        logger.error(f"Error loading existing data: {str(e)}")
        raise

def load_new_data(spark, file_path):
    """Load new source customer data"""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading new customer data from: {file_path}")
    
    try:
        # Define explicit schema for new data
        new_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("customer_name", StringType(), False),
            StructField("address", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("email", StringType(), False),
            StructField("source_date", StringType(), False)
        ])
        
        df = spark.read \
            .option("header", "true") \
            .schema(new_schema) \
            .csv(file_path)
        
        # Convert date column and trim whitespace
        df = df.withColumn("source_date", to_date(trim(col("source_date")), "yyyy-MM-dd"))
        
        logger.info(f"Loaded {df.count()} new customer records")
        return df
    except Exception as e:
        logger.error(f"Error loading new data: {str(e)}")
        raise

def get_current_records(existing_df):
    """Filter to get only current records from existing dimension"""
    return existing_df.filter(col("is_current") == True)

def identify_record_types(current_df, new_df):
    """
    Identify different types of records:
    1. Unchanged records
    2. Changed records (existing customers with modifications)
    3. New records (new customers)
    """
    logger = logging.getLogger(__name__)
    
    # Join current records with new records to identify changes
    joined_df = current_df.alias("curr").join(
        new_df.alias("new"), 
        col("curr.customer_id") == col("new.customer_id"), 
        "full_outer"
    )
    
    # Identify unchanged records (all attributes match)
    unchanged_condition = (
        (col("curr.customer_id").isNotNull()) &
        (col("new.customer_id").isNotNull()) &
        (col("curr.customer_name") == col("new.customer_name")) &
        (col("curr.address") == col("new.address")) &
        (col("curr.phone") == col("new.phone")) &
        (col("curr.email") == col("new.email"))
    )
    
    # Identify changed records (customer exists but attributes changed)
    changed_condition = (
        (col("curr.customer_id").isNotNull()) &
        (col("new.customer_id").isNotNull()) &
        ~unchanged_condition
    )
    
    # Identify new records (customer doesn't exist in current dimension)
    new_condition = (
        (col("curr.customer_id").isNull()) &
        (col("new.customer_id").isNotNull())
    )
    
    unchanged_records = joined_df.filter(unchanged_condition).select("curr.*")
    changed_records = joined_df.filter(changed_condition)
    new_records = joined_df.filter(new_condition).select("new.*")
    
    logger.info(f"Identified {unchanged_records.count()} unchanged records")
    logger.info(f"Identified {changed_records.count()} changed records")
    logger.info(f"Identified {new_records.count()} new records")
    
    return unchanged_records, changed_records, new_records

def get_next_surrogate_key(existing_df):
    """Get the next available surrogate key"""
    max_key = existing_df.agg(spark_max("customer_key")).collect()[0][0]
    return max_key + 1 if max_key else 1

def process_unchanged_records(unchanged_df):
    """Process unchanged records - keep them as is"""
    logger = logging.getLogger(__name__)
    logger.info("Processing unchanged records...")
    return unchanged_df

def process_changed_records(changed_df, existing_df, source_date):
    """
    Process changed records using SCD Type II:
    1. Close current record (set end date, is_current = false)
    2. Create new record with changes
    """
    logger = logging.getLogger(__name__)
    logger.info("Processing changed records...")
    
    # Get customer IDs that have changes
    changed_customer_ids = changed_df.select("curr.customer_id").distinct().rdd.map(lambda row: row[0]).collect()
    
    # Close current records for changed customers
    closed_records = existing_df.filter(
        (col("customer_id").isin(changed_customer_ids)) & (col("is_current") == True)
    ).withColumn("effective_end_date", lit(source_date).cast(DateType())) \
     .withColumn("is_current", lit(False))
    
    # Get next surrogate key
    next_key = get_next_surrogate_key(existing_df)
    
    # Create new records for changed customers
    window_spec = Window.orderBy("customer_id")
    new_changed_records = changed_df.select(
        "new.customer_id",
        "new.customer_name", 
        "new.address",
        "new.phone",
        "new.email",
        "new.source_date"
    ).withColumn(
        "customer_key", 
        lit(next_key) + row_number().over(window_spec) - 1
    ).withColumn("effective_start_date", col("source_date").cast(DateType())) \
     .withColumn("effective_end_date", lit("9999-12-31").cast(DateType())) \
     .withColumn("is_current", lit(True)) \
     .drop("source_date")
    
    return closed_records, new_changed_records

def process_new_records(new_records_df, existing_df):
    """Process completely new customer records"""
    logger = logging.getLogger(__name__)
    logger.info("Processing new customer records...")
    
    if new_records_df.count() == 0:
        return existing_df.limit(0)  # Return empty DataFrame with same schema
    
    # Get next surrogate key
    next_key = get_next_surrogate_key(existing_df)
    
    # Create window for row numbering
    window_spec = Window.orderBy("customer_id")
    
    # Create new records with surrogate keys
    processed_new_records = new_records_df.withColumn(
        "customer_key", 
        lit(next_key) + row_number().over(window_spec) - 1
    ).withColumn("effective_start_date", col("source_date").cast(DateType())) \
     .withColumn("effective_end_date", lit("9999-12-31").cast(DateType())) \
     .withColumn("is_current", lit(True)) \
     .drop("source_date")
    
    return processed_new_records

def combine_results(unchanged_df, closed_df, new_changed_df, new_df, existing_df):
    """Combine all processed records into final result"""
    logger = logging.getLogger(__name__)
    logger.info("Combining all processed records...")
    
    # Start with unchanged records
    result_df = unchanged_df
    
    # Add records for customers that didn't change
    unchanged_customer_ids = unchanged_df.select("customer_id").distinct().rdd.map(lambda row: row[0]).collect()
    other_existing = existing_df.filter(~col("customer_id").isin(unchanged_customer_ids))
    result_df = result_df.union(other_existing)
    
    # Add closed records
    if closed_df.count() > 0:
        result_df = result_df.union(closed_df)
    
    # Add new records for changed customers
    if new_changed_df.count() > 0:
        result_df = result_df.union(new_changed_df)
    
    # Add completely new customer records
    if new_df.count() > 0:
        result_df = result_df.union(new_df)
    
    # Sort by customer_id and effective_start_date for better readability
    result_df = result_df.orderBy("customer_id", "effective_start_date")
    
    logger.info(f"Final result contains {result_df.count()} total records")
    return result_df

def save_results(df, output_path):
    """Save the final results"""
    logger = logging.getLogger(__name__)
    logger.info(f"Saving results to: {output_path}")
    
    try:
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        logger.info("Results saved successfully")
    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")
        raise

def print_summary(final_df):
    """Print summary statistics"""
    logger = logging.getLogger(__name__)
    
    total_records = final_df.count()
    current_records = final_df.filter(col("is_current") == True).count()
    historical_records = final_df.filter(col("is_current") == False).count()
    unique_customers = final_df.select("customer_id").distinct().count()
    
    logger.info("=== SCD Type II Processing Summary ===")
    logger.info(f"Total records in dimension: {total_records}")
    logger.info(f"Current records: {current_records}")
    logger.info(f"Historical records: {historical_records}")
    logger.info(f"Unique customers: {unique_customers}")
    
    print("\n=== SCD Type II Processing Summary ===")
    print(f"Total records in dimension: {total_records}")
    print(f"Current records: {current_records}")
    print(f"Historical records: {historical_records}")
    print(f"Unique customers: {unique_customers}")

def main():
    """Main execution function"""
    logger = setup_logging()
    logger.info("Starting SCD Type II implementation")
    
    # Parse command line arguments
    if len(sys.argv) != 4:
        print("Usage: python scd_type2_implementation.py <existing_data_path> <new_data_path> <output_path>")
        sys.exit(1)
    
    existing_data_path = sys.argv[1]
    new_data_path = sys.argv[2]
    output_path = sys.argv[3]
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load data
        existing_df = load_existing_data(spark, existing_data_path)
        new_df = load_new_data(spark, new_data_path)
        
        # Get current records from existing dimension
        current_df = get_current_records(existing_df)
        
        # Process date from new data (using first record's date)
        source_date_str = new_df.select("source_date").first()[0]
        
        # Identify different types of records
        unchanged_records, changed_records, new_records = identify_record_types(current_df, new_df)
        
        # Process unchanged records
        processed_unchanged = process_unchanged_records(unchanged_records)
        
        # Process changed records
        if changed_records.count() > 0:
            closed_records, new_changed_records = process_changed_records(
                changed_records, existing_df, source_date_str
            )
        else:
            closed_records = existing_df.limit(0)
            new_changed_records = existing_df.limit(0)
        
        # Process new records
        processed_new_records = process_new_records(new_records, existing_df)
        
        # Combine all results
        final_df = combine_results(
            processed_unchanged, closed_records, new_changed_records, 
            processed_new_records, existing_df
        )
        
        # Save results
        save_results(final_df, output_path)
        
        # Print summary
        print_summary(final_df)
        
        # Show sample of final results
        print("\n=== Sample of Final Results ===")
        final_df.show(20, truncate=False)
        
        logger.info("SCD Type II implementation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 