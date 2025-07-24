#!/usr/bin/env python3
"""
Test script for data loading functions
"""

import logging
import sys
from pyspark.sql import SparkSession

# Import functions from our implementation
from sparksql_scd_type2_implementation import (
    setup_logging, 
    create_spark_session,
    load_existing_customer_data,
    load_new_customer_data,
    register_temporary_views
)

def test_data_loading():
    """Test the data loading and temporary view creation functions"""
    
    # Setup logging
    setup_logging(debug=True)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting data loading test")
    
    # Create Spark session
    spark = None
    try:
        spark = create_spark_session("Test-Data-Loading", debug=False)
        
        # Test loading existing customer data
        logger.info("Testing existing customer data loading...")
        existing_df = load_existing_customer_data(spark, "week-4/data/customer_existing.csv")
        logger.info(f"Existing data schema: {existing_df.schema}")
        logger.info("Sample existing data:")
        existing_df.show(5)
        
        # Test loading new customer data
        logger.info("Testing new customer data loading...")
        new_df = load_new_customer_data(spark, "week-4/data/customer_new.csv")
        logger.info(f"New data schema: {new_df.schema}")
        logger.info("Sample new data:")
        new_df.show(5)
        
        # Test temporary view registration
        logger.info("Testing temporary view registration...")
        register_temporary_views(spark, existing_df, new_df)
        
        # Test SQL queries on views
        logger.info("Testing SQL queries on temporary views...")
        existing_count = spark.sql("SELECT COUNT(*) as count FROM existing_customers").collect()[0].count
        new_count = spark.sql("SELECT COUNT(*) as count FROM new_customers").collect()[0].count
        
        logger.info(f"Existing customers view: {existing_count} records")
        logger.info(f"New customers view: {new_count} records")
        
        # Test a simple join query
        logger.info("Testing join query...")
        join_result = spark.sql("""
            SELECT e.customer_id, e.customer_name, e.is_current, n.source_date
            FROM existing_customers e
            INNER JOIN new_customers n ON e.customer_id = n.customer_id
            WHERE e.is_current = true
        """)
        
        logger.info("Join query results:")
        join_result.show()
        
        logger.info("Data loading test completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    test_data_loading()