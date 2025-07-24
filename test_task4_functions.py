#!/usr/bin/env python3
"""
Test script for Task 4: SQL-based record processing functions
"""

import logging
import sys
from pyspark.sql import SparkSession

# Import functions from the main implementation
from sparksql_scd_type2_implementation import (
    setup_logging, create_spark_session, load_existing_customer_data,
    load_new_customer_data, register_temporary_views,
    process_unchanged_records, process_changed_records, process_new_records
)

def test_sql_processing_functions():
    """Test the SQL-based record processing functions"""
    
    # Setup logging
    setup_logging(debug=True)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting test of SQL-based record processing functions")
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session("Test-SQL-Processing-Functions", debug=True)
        
        # Load test data
        existing_df = load_existing_customer_data(spark, "week-4/data/customer_existing.csv")
        new_df = load_new_customer_data(spark, "week-4/data/customer_new.csv")
        
        # Register temporary views
        register_temporary_views(spark, existing_df, new_df)
        
        # Test individual processing functions
        logger.info("=" * 60)
        logger.info("Testing process_unchanged_records function")
        unchanged_df = process_unchanged_records(spark, debug=True)
        unchanged_count = unchanged_df.count()
        logger.info(f"Unchanged records function test completed: {unchanged_count} records")
        
        logger.info("=" * 60)
        logger.info("Testing process_changed_records function")
        changed_existing_df, changed_new_df = process_changed_records(spark, debug=True)
        changed_existing_count = changed_existing_df.count()
        changed_new_count = changed_new_df.count()
        logger.info(f"Changed records function test completed: {changed_existing_count} existing, {changed_new_count} new")
        
        logger.info("=" * 60)
        logger.info("Testing process_new_records function")
        new_records_df = process_new_records(spark, debug=True)
        new_records_count = new_records_df.count()
        logger.info(f"New records function test completed: {new_records_count} records")
        
        # Summary
        total_processed = unchanged_count + changed_existing_count + new_records_count
        logger.info("=" * 60)
        logger.info("SQL-based record processing functions test summary:")
        logger.info(f"  - Unchanged records: {unchanged_count}")
        logger.info(f"  - Changed records: {changed_existing_count}")
        logger.info(f"  - New records: {new_records_count}")
        logger.info(f"  - Total processed: {total_processed}")
        logger.info("All SQL-based record processing functions executed successfully!")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = test_sql_processing_functions()
    sys.exit(0 if success else 1)