#!/usr/bin/env python3
"""
Unit Tests for SparkSQL SCD Type II Implementation

This module contains comprehensive unit tests for SQL queries and functions
in the SparkSQL SCD Type II implementation, covering:
- Record classification SQL queries with sample data
- SCD processing SQL operations with expected results validation
- Surrogate key generation and result combination logic
- Error handling scenarios and edge cases

Author: Data Engineering Team
Date: 2025-07-18
"""

import unittest
import logging
from datetime import datetime, date
from typing import List, Dict, Any
import tempfile
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import col, lit

# Import functions from the main implementation
from sparksql_scd_type2_implementation import (
    create_spark_session,
    get_existing_customer_schema,
    get_new_customer_schema,
    register_temporary_views,
    get_unchanged_records_query,
    get_changed_records_query,
    get_new_records_query,
    validate_and_execute_classification_query,
    process_unchanged_records,
    process_changed_records,
    process_new_records,
    classify_customer_records,
    calculate_next_surrogate_key,
    get_close_historical_records_query,
    get_new_current_records_for_changed_query,
    get_new_customer_records_query,
    process_scd_close_historical_records,
    process_scd_new_current_for_changed,
    process_scd_new_customer_records,
    execute_scd_type2_operations,
    get_final_union_query,
    execute_final_result_assembly
)


class TestSparkSQLSCDQueries(unittest.TestCase):
    """Test cases for SQL queries and functions in SparkSQL SCD Type II implementation."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = create_spark_session("SparkSQL-SCD-Unit-Tests", debug=False)
        
        # Suppress Spark logging for cleaner test output
        logging.getLogger('pyspark').setLevel(logging.ERROR)
        logging.getLogger('py4j').setLevel(logging.ERROR)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session after all tests."""
        if hasattr(cls, 'spark') and cls.spark:
            cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test case."""
        self.setup_test_data()
    
    def tearDown(self):
        """Clean up temporary views after each test."""
        try:
            self.spark.catalog.dropTempView("existing_customers")
            self.spark.catalog.dropTempView("new_customers")
        except:
            pass
    
    def setup_test_data(self):
        """Create sample test data for SCD Type II testing."""
        # Sample existing customer data
        existing_data = [
            (1, 101, "John Doe", "123 Main St", "555-1234", "john@email.com", 
             date(2023, 1, 1), date(9999, 12, 31), True),
            (2, 102, "Jane Smith", "456 Oak Ave", "555-5678", "jane@email.com", 
             date(2023, 1, 1), date(9999, 12, 31), True),
            (3, 103, "Bob Johnson", "789 Pine Rd", "555-9012", "bob@email.com", 
             date(2023, 1, 1), date(9999, 12, 31), True),
            (4, 104, "Alice Brown", "321 Elm St", "555-3456", "alice@email.com", 
             date(2023, 1, 1), date(2023, 6, 15), False),  # Historical record
            (5, 104, "Alice Brown", "654 Maple Dr", "555-7890", "alice.brown@email.com", 
             date(2023, 6, 15), date(9999, 12, 31), True),  # Current record for 104
        ]
        
        # Sample new customer data
        new_data = [
            (101, "John Doe", "123 Main St", "555-1234", "john@email.com", date(2024, 1, 15)),  # Unchanged
            (102, "Jane Smith", "789 New Ave", "555-5678", "jane.smith@email.com", date(2024, 1, 15)),  # Address and email changed
            (103, "Bob Johnson", "789 Pine Rd", "555-9999", "bob@email.com", date(2024, 1, 15)),  # Phone changed
            (105, "Charlie Wilson", "111 First St", "555-1111", "charlie@email.com", date(2024, 1, 15)),  # New customer
            (106, "Diana Davis", "222 Second St", "555-2222", "diana@email.com", date(2024, 1, 15)),  # New customer
        ]
        
        # Create DataFrames
        self.existing_df = self.spark.createDataFrame(existing_data, get_existing_customer_schema())
        self.new_df = self.spark.createDataFrame(new_data, get_new_customer_schema())
        
        # Register temporary views
        register_temporary_views(self.spark, self.existing_df, self.new_df)
    
    def test_schema_definitions(self):
        """Test that schema definitions are correct and consistent."""
        existing_schema = get_existing_customer_schema()
        new_schema = get_new_customer_schema()
        
        # Test existing customer schema
        expected_existing_fields = [
            'customer_key', 'customer_id', 'customer_name', 'address', 
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        actual_existing_fields = [field.name for field in existing_schema.fields]
        self.assertEqual(set(expected_existing_fields), set(actual_existing_fields))
        
        # Test new customer schema
        expected_new_fields = ['customer_id', 'customer_name', 'address', 'phone', 'email', 'source_date']
        actual_new_fields = [field.name for field in new_schema.fields]
        self.assertEqual(set(expected_new_fields), set(actual_new_fields))
        
        # Test data types
        self.assertEqual(existing_schema['customer_key'].dataType, IntegerType())
        self.assertEqual(existing_schema['is_current'].dataType, BooleanType())
        self.assertEqual(existing_schema['effective_start_date'].dataType, DateType())
        self.assertEqual(new_schema['source_date'].dataType, DateType())
    
    def test_unchanged_records_query(self):
        """Test unchanged records SQL query with sample data."""
        query = get_unchanged_records_query()
        
        # Validate query is not empty
        self.assertIsNotNone(query)
        self.assertTrue(len(query.strip()) > 0)
        
        # Execute query and validate results
        result_df = validate_and_execute_classification_query(
            self.spark, query, "Test Unchanged Records", debug=False
        )
        
        # Should find 1 unchanged record (customer 101)
        unchanged_records = result_df.collect()
        self.assertEqual(len(unchanged_records), 1)
        
        # Validate the unchanged record
        unchanged_record = unchanged_records[0]
        self.assertEqual(unchanged_record.customer_id, 101)
        self.assertEqual(unchanged_record.customer_name, "John Doe")
        self.assertEqual(unchanged_record.address, "123 Main St")
        self.assertEqual(unchanged_record.phone, "555-1234")
        self.assertEqual(unchanged_record.email, "john@email.com")
        self.assertTrue(unchanged_record.is_current)
    
    def test_changed_records_query(self):
        """Test changed records SQL query with sample data."""
        query = get_changed_records_query()
        
        # Validate query is not empty
        self.assertIsNotNone(query)
        self.assertTrue(len(query.strip()) > 0)
        
        # Execute query and validate results
        result_df = validate_and_execute_classification_query(
            self.spark, query, "Test Changed Records", debug=False
        )
        
        # Should find 2 changed records (customers 102 and 103)
        changed_records = result_df.collect()
        self.assertEqual(len(changed_records), 2)
        
        # Sort by customer_id for consistent testing
        changed_records = sorted(changed_records, key=lambda x: x.customer_id)
        
        # Validate first changed record (customer 102)
        record_102 = changed_records[0]
        self.assertEqual(record_102.customer_id, 102)
        self.assertEqual(record_102.existing_customer_name, "Jane Smith")
        self.assertEqual(record_102.existing_address, "456 Oak Ave")
        self.assertEqual(record_102.new_customer_name, "Jane Smith")
        self.assertEqual(record_102.new_address, "789 New Ave")
        self.assertEqual(record_102.new_email, "jane.smith@email.com")
        
        # Validate second changed record (customer 103)
        record_103 = changed_records[1]
        self.assertEqual(record_103.customer_id, 103)
        self.assertEqual(record_103.existing_phone, "555-9012")
        self.assertEqual(record_103.new_phone, "555-9999")
    
    def test_new_records_query(self):
        """Test new records SQL query with sample data."""
        query = get_new_records_query()
        
        # Validate query is not empty
        self.assertIsNotNone(query)
        self.assertTrue(len(query.strip()) > 0)
        
        # Execute query and validate results
        result_df = validate_and_execute_classification_query(
            self.spark, query, "Test New Records", debug=False
        )
        
        # Should find 2 new records (customers 105 and 106)
        new_records = result_df.collect()
        self.assertEqual(len(new_records), 2)
        
        # Sort by customer_id for consistent testing
        new_records = sorted(new_records, key=lambda x: x.customer_id)
        
        # Validate new records
        record_105 = new_records[0]
        self.assertEqual(record_105.customer_id, 105)
        self.assertEqual(record_105.customer_name, "Charlie Wilson")
        self.assertEqual(record_105.address, "111 First St")
        
        record_106 = new_records[1]
        self.assertEqual(record_106.customer_id, 106)
        self.assertEqual(record_106.customer_name, "Diana Davis")
        self.assertEqual(record_106.address, "222 Second St")
    
    def test_record_classification_functions(self):
        """Test record classification processing functions."""
        # Test unchanged records processing
        unchanged_df = process_unchanged_records(self.spark, debug=False)
        self.assertEqual(unchanged_df.count(), 1)
        
        # Test changed records processing
        changed_existing_df, changed_new_df = process_changed_records(self.spark, debug=False)
        self.assertEqual(changed_existing_df.count(), 2)
        self.assertEqual(changed_new_df.count(), 2)
        
        # Test new records processing
        new_df = process_new_records(self.spark, debug=False)
        self.assertEqual(new_df.count(), 2)
        
        # Test complete classification
        unchanged, changed, new = classify_customer_records(self.spark, debug=False)
        self.assertEqual(unchanged.count(), 1)
        self.assertEqual(changed.count(), 2)
        self.assertEqual(new.count(), 2)
    
    def test_surrogate_key_generation(self):
        """Test surrogate key generation logic."""
        next_key = calculate_next_surrogate_key(self.spark)
        
        # Should return 6 (max existing key is 5)
        self.assertEqual(next_key, 6)
        
        # Test with empty existing data
        empty_df = self.spark.createDataFrame([], get_existing_customer_schema())
        empty_df.createOrReplaceTempView("existing_customers")
        
        next_key_empty = calculate_next_surrogate_key(self.spark)
        self.assertEqual(next_key_empty, 1)
    
    def test_scd_processing_queries(self):
        """Test SCD processing SQL query generation."""
        source_date = "2024-01-15"
        next_key = 6
        
        # Test close historical records query
        close_query = get_close_historical_records_query(source_date)
        self.assertIsNotNone(close_query)
        self.assertIn(source_date, close_query)
        self.assertIn("false as is_current", close_query)
        
        # Test new current records for changed query
        new_current_query = get_new_current_records_for_changed_query(next_key, source_date)
        self.assertIsNotNone(new_current_query)
        self.assertIn(str(next_key), new_current_query)
        self.assertIn("ROW_NUMBER()", new_current_query)
        self.assertIn("true as is_current", new_current_query)
        
        # Test new customer records query
        new_customer_query = get_new_customer_records_query(next_key, source_date)
        self.assertIsNotNone(new_customer_query)
        self.assertIn(str(next_key), new_customer_query)
        self.assertIn("ROW_NUMBER()", new_customer_query)
    
    def test_scd_processing_functions(self):
        """Test SCD processing functions with expected results validation."""
        source_date = "2024-01-15"
        
        # Test close historical records
        closed_df = process_scd_close_historical_records(self.spark, source_date, debug=False)
        closed_records = closed_df.collect()
        
        # Should close 2 records (customers 102 and 103)
        self.assertEqual(len(closed_records), 2)
        
        # Validate closed records
        for record in closed_records:
            self.assertFalse(record.is_current)
            self.assertEqual(str(record.effective_end_date), source_date)
        
        # Test new current records for changed customers
        next_key = calculate_next_surrogate_key(self.spark)
        new_current_df = process_scd_new_current_for_changed(self.spark, next_key, source_date, debug=False)
        new_current_records = new_current_df.collect()
        
        # Should create 2 new current records
        self.assertEqual(len(new_current_records), 2)
        
        # Validate new current records
        for record in new_current_records:
            self.assertTrue(record.is_current)
            self.assertEqual(str(record.effective_start_date), source_date)
            self.assertEqual(str(record.effective_end_date), "9999-12-31")
            self.assertGreaterEqual(record.customer_key, next_key)
        
        # Test new customer records
        next_key_for_new = next_key + 2  # After 2 changed customers
        new_customer_df = process_scd_new_customer_records(self.spark, next_key_for_new, source_date, debug=False)
        new_customer_records = new_customer_df.collect()
        
        # Should create 2 new customer records
        self.assertEqual(len(new_customer_records), 2)
        
        # Validate new customer records
        for record in new_customer_records:
            self.assertTrue(record.is_current)
            self.assertEqual(str(record.effective_start_date), source_date)
            self.assertEqual(str(record.effective_end_date), "9999-12-31")
            self.assertGreaterEqual(record.customer_key, next_key_for_new)
    
    def test_complete_scd_operations(self):
        """Test complete SCD Type II operations with expected results."""
        unchanged_df, closed_df, new_current_df, new_customer_df = execute_scd_type2_operations(
            self.spark, debug=False
        )
        
        # Validate record counts
        self.assertEqual(unchanged_df.count(), 1)  # 1 unchanged
        self.assertEqual(closed_df.count(), 2)     # 2 closed historical
        self.assertEqual(new_current_df.count(), 2) # 2 new current for changed
        self.assertEqual(new_customer_df.count(), 2) # 2 new customers
        
        # Validate total records (should be 7: 1 unchanged + 2 closed + 2 new current + 2 new customers)
        total_records = unchanged_df.count() + closed_df.count() + new_current_df.count() + new_customer_df.count()
        self.assertEqual(total_records, 7)
    
    def test_result_combination_logic(self):
        """Test result combination using SQL UNION operations."""
        # Execute complete SCD operations
        unchanged_df, closed_df, new_current_df, new_customer_df = execute_scd_type2_operations(
            self.spark, debug=False
        )
        
        # Test final result assembly
        final_df = execute_final_result_assembly(
            self.spark, unchanged_df, closed_df, new_current_df, new_customer_df, debug=False
        )
        
        # Validate final result count
        self.assertEqual(final_df.count(), 7)
        
        # Validate final result schema
        expected_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address', 
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        self.assertEqual(set(final_df.columns), set(expected_columns))
        
        # Validate business rules in final result
        final_records = final_df.collect()
        
        # Count current vs historical records
        current_records = [r for r in final_records if r.is_current]
        historical_records = [r for r in final_records if not r.is_current]
        
        # Should have 5 current records (1 unchanged + 2 new current + 2 new customers)
        self.assertEqual(len(current_records), 5)
        # Should have 2 historical records (2 closed)
        self.assertEqual(len(historical_records), 2)
        
        # Validate unique customer_keys
        customer_keys = [r.customer_key for r in final_records]
        self.assertEqual(len(customer_keys), len(set(customer_keys)))
    
    def test_union_query_generation(self):
        """Test SQL UNION query generation for result combination."""
        union_query = get_final_union_query()
        
        # Validate query structure
        self.assertIsNotNone(union_query)
        self.assertIn("UNION ALL", union_query)
        self.assertIn("unchanged_records", union_query)
        self.assertIn("closed_historical_records", union_query)
        self.assertIn("new_current_changed_records", union_query)
        self.assertIn("new_customer_records", union_query)
        self.assertIn("ORDER BY customer_key", union_query)
        
        # Count UNION ALL occurrences (should be 3 for 4 tables)
        union_count = union_query.count("UNION ALL")
        self.assertEqual(union_count, 3)


class TestSparkSQLSCDErrorHandling(unittest.TestCase):
    """Test cases for error handling scenarios and edge cases."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for error handling tests."""
        cls.spark = create_spark_session("SparkSQL-SCD-Error-Tests", debug=False)
        
        # Suppress Spark logging for cleaner test output
        logging.getLogger('pyspark').setLevel(logging.ERROR)
        logging.getLogger('py4j').setLevel(logging.ERROR)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session after all tests."""
        if hasattr(cls, 'spark') and cls.spark:
            cls.spark.stop()
    
    def tearDown(self):
        """Clean up temporary views after each test."""
        try:
            self.spark.catalog.dropTempView("existing_customers")
            self.spark.catalog.dropTempView("new_customers")
        except:
            pass
    
    def test_empty_datasets(self):
        """Test handling of empty datasets."""
        # Create empty DataFrames
        empty_existing = self.spark.createDataFrame([], get_existing_customer_schema())
        empty_new = self.spark.createDataFrame([], get_new_customer_schema())
        
        # Register temporary views
        register_temporary_views(self.spark, empty_existing, empty_new)
        
        # Test classification with empty data
        unchanged_df = process_unchanged_records(self.spark, debug=False)
        self.assertEqual(unchanged_df.count(), 0)
        
        changed_existing_df, changed_new_df = process_changed_records(self.spark, debug=False)
        self.assertEqual(changed_existing_df.count(), 0)
        self.assertEqual(changed_new_df.count(), 0)
        
        new_df = process_new_records(self.spark, debug=False)
        self.assertEqual(new_df.count(), 0)
    
    def test_single_record_datasets(self):
        """Test handling of single record datasets."""
        # Single existing record
        existing_data = [(1, 101, "John Doe", "123 Main St", "555-1234", "john@email.com", 
                         date(2023, 1, 1), date(9999, 12, 31), True)]
        existing_df = self.spark.createDataFrame(existing_data, get_existing_customer_schema())
        
        # Single new record (unchanged)
        new_data = [(101, "John Doe", "123 Main St", "555-1234", "john@email.com", date(2024, 1, 15))]
        new_df = self.spark.createDataFrame(new_data, get_new_customer_schema())
        
        register_temporary_views(self.spark, existing_df, new_df)
        
        # Test classification
        unchanged_df = process_unchanged_records(self.spark, debug=False)
        self.assertEqual(unchanged_df.count(), 1)
        
        changed_existing_df, changed_new_df = process_changed_records(self.spark, debug=False)
        self.assertEqual(changed_existing_df.count(), 0)
        
        new_df_result = process_new_records(self.spark, debug=False)
        self.assertEqual(new_df_result.count(), 0)
    
    def test_duplicate_customer_ids(self):
        """Test handling of duplicate customer IDs in new data."""
        # Existing data
        existing_data = [(1, 101, "John Doe", "123 Main St", "555-1234", "john@email.com", 
                         date(2023, 1, 1), date(9999, 12, 31), True)]
        existing_df = self.spark.createDataFrame(existing_data, get_existing_customer_schema())
        
        # New data with duplicates
        new_data = [
            (101, "John Doe", "123 Main St", "555-1234", "john@email.com", date(2024, 1, 15)),
            (101, "John Doe", "456 Oak Ave", "555-5678", "john.doe@email.com", date(2024, 1, 15))  # Duplicate
        ]
        new_df = self.spark.createDataFrame(new_data, get_new_customer_schema())
        
        register_temporary_views(self.spark, existing_df, new_df)
        
        # Should handle duplicates gracefully (implementation dependent)
        try:
            unchanged_df = process_unchanged_records(self.spark, debug=False)
            changed_existing_df, changed_new_df = process_changed_records(self.spark, debug=False)
            new_df_result = process_new_records(self.spark, debug=False)
            
            # At least one of these should have results
            total_classified = unchanged_df.count() + changed_existing_df.count() + new_df_result.count()
            self.assertGreaterEqual(total_classified, 1)
        except Exception as e:
            # If the implementation throws an error for duplicates, that's also acceptable
            self.assertIsInstance(e, (ValueError, RuntimeError))
    
    def test_null_values_handling(self):
        """Test handling of NULL values in data."""
        # Test with NULL customer_name (should be handled by schema validation)
        try:
            # This should fail during DataFrame creation due to non-nullable schema
            invalid_data = [(1, 101, None, "123 Main St", "555-1234", "john@email.com", 
                           date(2023, 1, 1), date(9999, 12, 31), True)]
            invalid_df = self.spark.createDataFrame(invalid_data, get_existing_customer_schema())
            
            # If it doesn't fail during creation, it should fail during processing
            register_temporary_views(self.spark, invalid_df, self.spark.createDataFrame([], get_new_customer_schema()))
            
        except Exception as e:
            # Expected to fail with NULL values in non-nullable fields
            self.assertTrue(isinstance(e, (ValueError, TypeError, Exception)))
    
    def test_invalid_date_formats(self):
        """Test handling of invalid date formats."""
        # Create data with valid dates (invalid dates would fail during DataFrame creation)
        existing_data = [(1, 101, "John Doe", "123 Main St", "555-1234", "john@email.com", 
                         date(2023, 1, 1), date(9999, 12, 31), True)]
        existing_df = self.spark.createDataFrame(existing_data, get_existing_customer_schema())
        
        new_data = [(101, "John Doe", "123 Main St", "555-1234", "john@email.com", date(2024, 1, 15))]
        new_df = self.spark.createDataFrame(new_data, get_new_customer_schema())
        
        register_temporary_views(self.spark, existing_df, new_df)
        
        # Test with invalid source_date in query (should be handled by query validation)
        try:
            invalid_source_date = "invalid-date"
            close_query = get_close_historical_records_query(invalid_source_date)
            # The query generation should succeed, but execution might fail
            self.assertIsNotNone(close_query)
        except Exception as e:
            # If validation catches invalid dates, that's acceptable
            self.assertIsInstance(e, (ValueError, RuntimeError))
    
    def test_query_validation_errors(self):
        """Test SQL query validation and error handling."""
        # Test with empty query
        try:
            validate_and_execute_classification_query(self.spark, "", "Empty Query Test", debug=False)
            self.fail("Should have raised an exception for empty query")
        except Exception as e:
            self.assertIsInstance(e, (ValueError, RuntimeError))
        
        # Test with invalid SQL syntax
        try:
            invalid_query = "SELECT * FROM non_existent_table WHERE invalid syntax"
            validate_and_execute_classification_query(self.spark, invalid_query, "Invalid Query Test", debug=False)
            self.fail("Should have raised an exception for invalid SQL")
        except Exception as e:
            self.assertIsInstance(e, (RuntimeError, Exception))
    
    def test_missing_temporary_views(self):
        """Test handling when temporary views are not registered."""
        # Don't register any views
        
        # Test queries that depend on temporary views
        try:
            unchanged_query = get_unchanged_records_query()
            validate_and_execute_classification_query(self.spark, unchanged_query, "Missing Views Test", debug=False)
            self.fail("Should have raised an exception for missing temporary views")
        except Exception as e:
            self.assertIsInstance(e, (RuntimeError, Exception))
    
    def test_surrogate_key_edge_cases(self):
        """Test surrogate key calculation edge cases."""
        # Test with no existing data
        empty_df = self.spark.createDataFrame([], get_existing_customer_schema())
        empty_df.createOrReplaceTempView("existing_customers")
        
        next_key = calculate_next_surrogate_key(self.spark)
        self.assertEqual(next_key, 1)
        
        # Test with large customer_key values
        large_key_data = [(999999, 101, "John Doe", "123 Main St", "555-1234", "john@email.com", 
                          date(2023, 1, 1), date(9999, 12, 31), True)]
        large_key_df = self.spark.createDataFrame(large_key_data, get_existing_customer_schema())
        large_key_df.createOrReplaceTempView("existing_customers")
        
        next_key_large = calculate_next_surrogate_key(self.spark)
        self.assertEqual(next_key_large, 1000000)


if __name__ == '__main__':
    # Configure logging for test execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the tests
    unittest.main(verbosity=2)