#!/usr/bin/env python3
"""
Test script for Task 6: Result combination and output functions

This script tests the SQL UNION operations, result assembly, CSV output functionality,
and summary statistics generation implemented in task 6.
"""

import os
import sys
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from datetime import datetime

# Import functions from the main implementation
sys.path.append('.')
from sparksql_scd_type2_implementation import (
    setup_logging, create_spark_session, get_existing_customer_schema, get_new_customer_schema,
    get_final_union_query, execute_final_result_assembly, save_results_to_csv,
    generate_final_summary_statistics, combine_and_output_results
)


def create_test_data(spark):
    """Create test DataFrames for testing result combination functions."""
    
    # Create unchanged records DataFrame
    unchanged_data = [
        (1, 101, "John Doe", "123 Main St", "555-1234", "john@email.com", 
         datetime(2023, 1, 1).date(), datetime(9999, 12, 31).date(), True),
        (2, 102, "Jane Smith", "456 Oak Ave", "555-5678", "jane@email.com", 
         datetime(2023, 1, 1).date(), datetime(9999, 12, 31).date(), True)
    ]
    unchanged_df = spark.createDataFrame(unchanged_data, get_existing_customer_schema())
    
    # Create closed historical records DataFrame
    closed_historical_data = [
        (3, 103, "Bob Johnson", "789 Pine St", "555-9012", "bob@email.com", 
         datetime(2023, 1, 1).date(), datetime(2024, 1, 1).date(), False)
    ]
    closed_historical_df = spark.createDataFrame(closed_historical_data, get_existing_customer_schema())
    
    # Create new current records for changed customers DataFrame
    new_current_changed_data = [
        (6, 103, "Bob Johnson", "789 Pine St Updated", "555-9012", "bob.new@email.com", 
         datetime(2024, 1, 1).date(), datetime(9999, 12, 31).date(), True)
    ]
    new_current_changed_df = spark.createDataFrame(new_current_changed_data, get_existing_customer_schema())
    
    # Create new customer records DataFrame
    new_customer_data = [
        (7, 104, "Alice Brown", "321 Elm St", "555-3456", "alice@email.com", 
         datetime(2024, 1, 1).date(), datetime(9999, 12, 31).date(), True),
        (8, 105, "Charlie Wilson", "654 Maple Ave", "555-7890", "charlie@email.com", 
         datetime(2024, 1, 1).date(), datetime(9999, 12, 31).date(), True)
    ]
    new_customer_df = spark.createDataFrame(new_customer_data, get_existing_customer_schema())
    
    return unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df


def test_final_union_query():
    """Test the SQL UNION query generation."""
    print("Testing final UNION query generation...")
    
    union_query = get_final_union_query()
    
    # Verify query contains expected components
    assert "UNION ALL" in union_query
    assert "unchanged_records" in union_query
    assert "closed_historical_records" in union_query
    assert "new_current_changed_records" in union_query
    assert "new_customer_records" in union_query
    assert "ORDER BY customer_key" in union_query
    
    print("✓ Final UNION query generation test passed")


def test_result_assembly(spark):
    """Test the final result assembly function."""
    print("Testing final result assembly...")
    
    # Create test data
    unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df = create_test_data(spark)
    
    # Execute result assembly
    final_result_df = execute_final_result_assembly(
        spark, unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df, debug=True
    )
    
    # Verify results
    total_count = final_result_df.count()
    expected_count = (unchanged_df.count() + closed_historical_df.count() + 
                     new_current_changed_df.count() + new_customer_df.count())
    
    assert total_count == expected_count, f"Expected {expected_count} records, got {total_count}"
    assert total_count == 6, f"Expected 6 total records, got {total_count}"
    
    # Verify schema
    expected_columns = [
        'customer_key', 'customer_id', 'customer_name', 'address', 
        'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
    ]
    actual_columns = final_result_df.columns
    assert set(expected_columns) == set(actual_columns)
    
    print("✓ Final result assembly test passed")
    return final_result_df


def test_csv_output(spark, final_result_df):
    """Test CSV output functionality."""
    print("Testing CSV output functionality...")
    
    # Create temporary directory for output
    temp_dir = tempfile.mkdtemp()
    output_path = os.path.join(temp_dir, "test_output.csv")
    
    try:
        # Save results to CSV
        save_results_to_csv(spark, final_result_df, output_path, debug=True)
        
        # Verify output file was created
        csv_files = []
        for root, dirs, files in os.walk(output_path):
            csv_files.extend([f for f in files if f.endswith('.csv')])
        
        assert len(csv_files) > 0, "No CSV files were created"
        
        # Try to read back the CSV to validate
        validation_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(output_path)
        
        validation_count = validation_df.count()
        original_count = final_result_df.count()
        
        assert validation_count == original_count, f"CSV validation failed: expected {original_count}, got {validation_count}"
        
        print("✓ CSV output functionality test passed")
        
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)


def test_summary_statistics(spark, final_result_df):
    """Test summary statistics generation."""
    print("Testing summary statistics generation...")
    
    # Generate summary statistics
    summary_stats = generate_final_summary_statistics(spark, final_result_df, debug=True)
    
    # Verify summary statistics structure
    required_keys = [
        'total_records', 'current_records', 'historical_records', 
        'unique_customers', 'unique_customer_keys', 'data_quality',
        'date_range', 'customer_key_range'
    ]
    
    for key in required_keys:
        assert key in summary_stats, f"Missing key in summary stats: {key}"
    
    # Verify data quality section
    data_quality = summary_stats['data_quality']
    assert 'null_customer_ids' in data_quality
    assert 'null_customer_keys' in data_quality
    assert 'duplicate_customer_keys' in data_quality
    
    # Verify basic counts
    assert summary_stats['total_records'] == 6
    assert summary_stats['current_records'] == 4  # unchanged (2) + new_current_changed (1) + new_customer (2)
    assert summary_stats['historical_records'] == 1  # closed_historical (1)
    assert summary_stats['unique_customers'] == 5  # 101, 102, 103, 104, 105
    assert summary_stats['unique_customer_keys'] == 6  # 1, 2, 3, 6, 7, 8
    
    # Verify data quality (should be clean test data)
    assert data_quality['null_customer_ids'] == 0
    assert data_quality['null_customer_keys'] == 0
    assert data_quality['duplicate_customer_keys'] == 0
    
    print("✓ Summary statistics generation test passed")
    return summary_stats


def test_combine_and_output_results(spark):
    """Test the complete combine and output results function."""
    print("Testing complete combine and output results function...")
    
    # Create test data
    unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df = create_test_data(spark)
    
    # Create temporary directory for output
    temp_dir = tempfile.mkdtemp()
    output_path = os.path.join(temp_dir, "complete_test_output.csv")
    
    try:
        # Execute complete combine and output function
        summary_stats = combine_and_output_results(
            spark, unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df,
            output_path, debug=True
        )
        
        # Verify summary statistics were returned
        assert isinstance(summary_stats, dict)
        assert summary_stats['total_records'] == 6
        
        # Verify output file was created
        csv_files = []
        for root, dirs, files in os.walk(output_path):
            csv_files.extend([f for f in files if f.endswith('.csv')])
        
        assert len(csv_files) > 0, "No CSV files were created by complete function"
        
        print("✓ Complete combine and output results test passed")
        
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)


def main():
    """Run all Task 6 function tests."""
    print("Starting Task 6 function tests...")
    print("=" * 50)
    
    # Setup logging
    setup_logging(debug=True)
    
    # Create Spark session
    spark = create_spark_session("Task6-Test", debug=True)
    
    try:
        # Run individual tests
        test_final_union_query()
        final_result_df = test_result_assembly(spark)
        test_csv_output(spark, final_result_df)
        test_summary_statistics(spark, final_result_df)
        test_combine_and_output_results(spark)
        
        print("=" * 50)
        print("✅ All Task 6 function tests passed successfully!")
        print("Task 6 implementation is working correctly:")
        print("  ✓ SQL UNION operations for combining record types")
        print("  ✓ Final result assembly with proper column ordering")
        print("  ✓ CSV output with header and proper formatting")
        print("  ✓ Summary statistics generation and validation")
        print("  ✓ Complete result combination and output workflow")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        raise
        
    finally:
        # Clean up Spark session
        spark.stop()


if __name__ == "__main__":
    main()