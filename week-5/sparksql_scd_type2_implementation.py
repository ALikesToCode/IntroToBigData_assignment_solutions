#!/usr/bin/env python3
"""
SCD Type II Implementation using SparkSQL
Course: Introduction to Big Data - Week 5 Assignment

This script implements Slowly Changing Dimensions Type II (SCD Type II) 
for customer master data using SparkSQL on Dataproc cluster.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import sys
import logging
import argparse
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, BooleanType
)

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def create_spark_session(app_name="SparkSQL_SCD_Type2_Implementation"):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_and_create_tables(spark, existing_data_path, new_data_path):
    """Load data and create temporary tables for SQL queries"""
    logger = logging.getLogger(__name__)
    logger.info("Loading data and creating temporary tables...")
    
    try:
        # Define schemas
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
        
        new_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("customer_name", StringType(), False),
            StructField("address", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("email", StringType(), False),
            StructField("source_date", StringType(), False)
        ])
        
        # Load existing customer dimension data
        existing_df = spark.read \
            .option("header", "true") \
            .schema(existing_schema) \
            .csv(existing_data_path)
        
        # Convert date columns for existing data
        existing_df.createOrReplaceTempView("existing_raw")
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW existing_dimension AS
            SELECT 
                customer_key,
                customer_id,
                customer_name,
                address,
                phone,
                email,
                TO_DATE(TRIM(effective_start_date), 'yyyy-MM-dd') as effective_start_date,
                TO_DATE(TRIM(effective_end_date), 'yyyy-MM-dd') as effective_end_date,
                is_current
            FROM existing_raw
        """)
        
        # Load new source data
        new_df = spark.read \
            .option("header", "true") \
            .schema(new_schema) \
            .csv(new_data_path)
        
        # Convert date column for new data
        new_df.createOrReplaceTempView("new_raw")
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW new_source AS
            SELECT 
                customer_id,
                customer_name,
                address,
                phone,
                email,
                TO_DATE(TRIM(source_date), 'yyyy-MM-dd') as source_date
            FROM new_raw
        """)
        
        # Create current records view
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW current_dimension AS
            SELECT * FROM existing_dimension WHERE is_current = true
        """)
        
        logger.info("Data loaded and temporary tables created successfully")
        
        # Log record counts
        existing_count = spark.sql("SELECT COUNT(*) FROM existing_dimension").collect()[0][0]
        new_count = spark.sql("SELECT COUNT(*) FROM new_source").collect()[0][0]
        current_count = spark.sql("SELECT COUNT(*) FROM current_dimension").collect()[0][0]
        
        logger.info(f"Existing dimension records: {existing_count}")
        logger.info(f"New source records: {new_count}")
        logger.info(f"Current dimension records: {current_count}")
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def get_next_surrogate_key(spark):
    """Get the next available surrogate key using SQL"""
    result = spark.sql("""
        SELECT COALESCE(MAX(customer_key), 0) + 1 as next_key 
        FROM existing_dimension
    """).collect()[0][0]
    
    return result

def identify_record_types(spark):
    """Identify unchanged, changed, and new records using SQL"""
    logger = logging.getLogger(__name__)
    logger.info("Identifying record types using SparkSQL...")
    
    # Create a comprehensive comparison view
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW record_comparison AS
        SELECT 
            curr.customer_key,
            curr.customer_id as curr_customer_id,
            curr.customer_name as curr_customer_name,
            curr.address as curr_address,
            curr.phone as curr_phone,
            curr.email as curr_email,
            curr.effective_start_date as curr_effective_start_date,
            curr.effective_end_date as curr_effective_end_date,
            curr.is_current as curr_is_current,
            new.customer_id as new_customer_id,
            new.customer_name as new_customer_name,
            new.address as new_address,
            new.phone as new_phone,
            new.email as new_email,
            new.source_date as new_source_date,
            CASE 
                WHEN curr.customer_id IS NULL AND new.customer_id IS NOT NULL THEN 'NEW'
                WHEN curr.customer_id IS NOT NULL AND new.customer_id IS NOT NULL AND
                     (curr.customer_name != new.customer_name OR 
                      curr.address != new.address OR 
                      curr.phone != new.phone OR 
                      curr.email != new.email) THEN 'CHANGED'
                WHEN curr.customer_id IS NOT NULL AND new.customer_id IS NOT NULL AND
                     curr.customer_name = new.customer_name AND 
                     curr.address = new.address AND 
                     curr.phone = new.phone AND 
                     curr.email = new.email THEN 'UNCHANGED'
                ELSE 'UNKNOWN'
            END as record_type
        FROM current_dimension curr
        FULL OUTER JOIN new_source new ON curr.customer_id = new.customer_id
    """)
    
    # Create views for each record type
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW unchanged_records AS
        SELECT 
            customer_key, curr_customer_id as customer_id, curr_customer_name as customer_name,
            curr_address as address, curr_phone as phone, curr_email as email,
            curr_effective_start_date as effective_start_date,
            curr_effective_end_date as effective_end_date,
            curr_is_current as is_current
        FROM record_comparison 
        WHERE record_type = 'UNCHANGED'
    """)
    
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW changed_records AS
        SELECT * FROM record_comparison WHERE record_type = 'CHANGED'
    """)
    
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW new_records AS
        SELECT 
            new_customer_id as customer_id, new_customer_name as customer_name,
            new_address as address, new_phone as phone, new_email as email,
            new_source_date as source_date
        FROM record_comparison 
        WHERE record_type = 'NEW'
    """)
    
    # Log counts
    unchanged_count = spark.sql("SELECT COUNT(*) FROM unchanged_records").collect()[0][0]
    changed_count = spark.sql("SELECT COUNT(*) FROM changed_records").collect()[0][0]
    new_count = spark.sql("SELECT COUNT(*) FROM new_records").collect()[0][0]
    
    logger.info(f"Unchanged records: {unchanged_count}")
    logger.info(f"Changed records: {changed_count}")
    logger.info(f"New records: {new_count}")

def process_unchanged_records(spark):
    """Process unchanged records - they remain as is"""
    logger = logging.getLogger(__name__)
    logger.info("Processing unchanged records...")
    
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW processed_unchanged AS
        SELECT * FROM unchanged_records
    """)

def process_changed_records(spark):
    """Process changed records using SQL - close old, create new"""
    logger = logging.getLogger(__name__)
    logger.info("Processing changed records...")
    
    # Get source date from new records
    source_date_result = spark.sql("SELECT DISTINCT new_source_date FROM changed_records LIMIT 1").collect()
    if len(source_date_result) > 0:
        source_date = source_date_result[0][0]
    else:
        source_date = None
    
    if source_date:
        # Close existing records for changed customers
        spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW closed_records AS
            SELECT 
                customer_key,
                curr_customer_id as customer_id,
                curr_customer_name as customer_name,
                curr_address as address,
                curr_phone as phone,
                curr_email as email,
                curr_effective_start_date as effective_start_date,
                DATE'{source_date}' as effective_end_date,
                false as is_current
            FROM changed_records
        """)
        
        # Get next surrogate key for new versions
        next_key = get_next_surrogate_key(spark)
        
        # Create new records for changed customers
        spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW new_changed_records AS
            SELECT 
                {next_key} + ROW_NUMBER() OVER (ORDER BY new_customer_id) - 1 as customer_key,
                new_customer_id as customer_id,
                new_customer_name as customer_name,
                new_address as address,
                new_phone as phone,
                new_email as email,
                new_source_date as effective_start_date,
                DATE'9999-12-31' as effective_end_date,
                true as is_current
            FROM changed_records
        """)
    else:
        # Create empty views if no changed records
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW closed_records AS
            SELECT * FROM existing_dimension WHERE 1=0
        """)
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW new_changed_records AS
            SELECT * FROM existing_dimension WHERE 1=0
        """)

def process_new_records(spark):
    """Process completely new customer records using SQL"""
    logger = logging.getLogger(__name__)
    logger.info("Processing new customer records...")
    
    # Check if there are new records
    new_count = spark.sql("SELECT COUNT(*) FROM new_records").collect()[0][0]
    
    if new_count > 0:
        # Get next surrogate key - need to account for keys used by changed records
        changed_count = spark.sql("SELECT COUNT(*) FROM changed_records").collect()[0][0]
        next_key = get_next_surrogate_key(spark) + changed_count
        
        spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW processed_new_records AS
            SELECT 
                {next_key} + ROW_NUMBER() OVER (ORDER BY customer_id) - 1 as customer_key,
                customer_id,
                customer_name,
                address,
                phone,
                email,
                source_date as effective_start_date,
                DATE'9999-12-31' as effective_end_date,
                true as is_current
            FROM new_records
        """)
    else:
        # Create empty view if no new records
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW processed_new_records AS
            SELECT * FROM existing_dimension WHERE 1=0
        """)

def combine_all_results(spark):
    """Combine all processed records using SQL UNION"""
    logger = logging.getLogger(__name__)
    logger.info("Combining all processed records...")
    
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW final_dimension AS
        SELECT 
            customer_key, customer_id, customer_name, address, phone, email,
            effective_start_date, effective_end_date, is_current
        FROM (
            -- Keep all historical records that are not for customers being updated
            SELECT * FROM existing_dimension 
            WHERE customer_id NOT IN (
                SELECT DISTINCT curr_customer_id FROM changed_records WHERE curr_customer_id IS NOT NULL
            )
            
            UNION ALL
            
            -- Add closed records for changed customers
            SELECT * FROM closed_records
            
            UNION ALL
            
            -- Add new versions for changed customers
            SELECT * FROM new_changed_records
            
            UNION ALL
            
            -- Add completely new customer records
            SELECT * FROM processed_new_records
        )
        ORDER BY customer_id, effective_start_date
    """)

def save_results(spark, output_path):
    """Save final results using SQL"""
    logger = logging.getLogger(__name__)
    logger.info(f"Saving results to: {output_path}")
    
    try:
        final_df = spark.sql("SELECT * FROM final_dimension")
        
        # Show sample data
        logger.info("Sample of final results:")
        final_df.show(10, truncate=False)
        
        # Save results
        final_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        logger.info("Results saved successfully")
        return final_df
        
    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")
        raise

def print_summary(spark):
    """Print processing summary using SQL"""
    logger = logging.getLogger(__name__)
    
    # Get summary statistics using SQL
    summary_stats = spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_current = true THEN 1 END) as current_records,
            COUNT(CASE WHEN is_current = false THEN 1 END) as historical_records,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM final_dimension
    """).collect()[0]
    
    total_records = summary_stats['total_records']
    current_records = summary_stats['current_records']
    historical_records = summary_stats['historical_records']
    unique_customers = summary_stats['unique_customers']
    
    summary_text = f"""
=== SparkSQL SCD Type II Processing Summary ===
Total records in dimension: {total_records}
Current records: {current_records}
Historical records: {historical_records}
Unique customers: {unique_customers}
    """
    
    logger.info(summary_text)
    print(summary_text)

def main():
    """Main function to orchestrate SparkSQL SCD Type II implementation"""
    logger = setup_logging()
    logger.info("Starting SparkSQL SCD Type II implementation")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='SparkSQL SCD Type II Implementation')
    parser.add_argument('--existing_data_path', default="data/customer_existing.csv",
                       help='Path to existing customer data')
    parser.add_argument('--new_data_path', default="data/customer_new.csv",
                       help='Path to new customer data')
    parser.add_argument('--output_path', default="output/customer_dimension_updated",
                       help='Output path for results')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = create_spark_session()
    
    try:
        logger.info(f"Using paths:")
        logger.info(f"  Existing data: {args.existing_data_path}")
        logger.info(f"  New data: {args.new_data_path}")
        logger.info(f"  Output: {args.output_path}")
        
        # Step 1: Load data and create tables
        load_and_create_tables(spark, args.existing_data_path, args.new_data_path)
        
        # Step 2: Identify record types
        identify_record_types(spark)
        
        # Step 3: Process unchanged records
        process_unchanged_records(spark)
        
        # Step 4: Process changed records
        process_changed_records(spark)
        
        # Step 5: Process new records
        process_new_records(spark)
        
        # Step 6: Combine all results
        combine_all_results(spark)
        
        # Step 7: Save results
        final_df = save_results(spark, args.output_path)
        
        # Step 8: Print summary
        print_summary(spark)
        
        logger.info("SparkSQL SCD Type II implementation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
