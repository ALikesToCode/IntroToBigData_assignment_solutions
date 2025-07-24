#!/usr/bin/env python3
"""
Test script to validate SQL queries implemented in Task 5.
This script tests the SQL query generation functions without requiring a full Spark session.
"""

import sys
import os

# Add the current directory to Python path to import the main module
sys.path.append('.')

try:
    from sparksql_scd_type2_implementation import (
        get_close_historical_records_query,
        get_new_current_records_for_changed_query,
        get_new_customer_records_query
    )
    
    def test_sql_query_generation():
        """Test that all SQL query generation functions work correctly."""
        print("Testing SQL query generation functions for Task 5...")
        
        # Test parameters
        source_date = "2024-01-15"
        next_key = 100
        
        try:
            # Test 1: Close historical records query
            print("\n1. Testing close historical records query...")
            close_query = get_close_historical_records_query(source_date)
            print("✓ Close historical records query generated successfully")
            print(f"Query length: {len(close_query)} characters")
            
            # Validate query contains expected elements
            assert "effective_end_date" in close_query
            assert "is_current" in close_query
            assert source_date in close_query
            assert "false" in close_query.lower()
            print("✓ Query validation passed")
            
            # Test 2: New current records for changed customers query
            print("\n2. Testing new current records for changed customers query...")
            new_current_query = get_new_current_records_for_changed_query(next_key, source_date)
            print("✓ New current records query generated successfully")
            print(f"Query length: {len(new_current_query)} characters")
            
            # Validate query contains expected elements
            assert "ROW_NUMBER()" in new_current_query
            assert str(next_key) in new_current_query
            assert "effective_start_date" in new_current_query
            assert "9999-12-31" in new_current_query
            assert "true" in new_current_query.lower()
            print("✓ Query validation passed")
            
            # Test 3: New customer records query
            print("\n3. Testing new customer records query...")
            new_customer_query = get_new_customer_records_query(next_key, source_date)
            print("✓ New customer records query generated successfully")
            print(f"Query length: {len(new_customer_query)} characters")
            
            # Validate query contains expected elements
            assert "ROW_NUMBER()" in new_customer_query
            assert str(next_key) in new_customer_query
            assert "LEFT JOIN" in new_customer_query
            assert "IS NULL" in new_customer_query
            assert "9999-12-31" in new_customer_query
            print("✓ Query validation passed")
            
            print("\n✅ All SQL query generation tests passed!")
            return True
            
        except Exception as e:
            print(f"\n❌ SQL query generation test failed: {str(e)}")
            return False
    
    def display_sample_queries():
        """Display sample generated queries for manual inspection."""
        print("\n" + "="*80)
        print("SAMPLE GENERATED SQL QUERIES")
        print("="*80)
        
        source_date = "2024-01-15"
        next_key = 100
        
        print("\n1. CLOSE HISTORICAL RECORDS QUERY:")
        print("-" * 50)
        close_query = get_close_historical_records_query(source_date)
        print(close_query)
        
        print("\n2. NEW CURRENT RECORDS FOR CHANGED CUSTOMERS QUERY:")
        print("-" * 50)
        new_current_query = get_new_current_records_for_changed_query(next_key, source_date)
        print(new_current_query)
        
        print("\n3. NEW CUSTOMER RECORDS QUERY:")
        print("-" * 50)
        new_customer_query = get_new_customer_records_query(next_key, source_date)
        print(new_customer_query)
        
        print("\n" + "="*80)
    
    if __name__ == "__main__":
        print("Task 5 SQL Query Validation Test")
        print("=" * 40)
        
        # Run tests
        success = test_sql_query_generation()
        
        if success:
            # Display sample queries for manual inspection
            display_sample_queries()
            print("\n🎉 Task 5 implementation validation completed successfully!")
            sys.exit(0)
        else:
            print("\n💥 Task 5 implementation validation failed!")
            sys.exit(1)

except ImportError as e:
    print(f"❌ Failed to import functions from main module: {str(e)}")
    print("Make sure sparksql_scd_type2_implementation.py is in the current directory")
    sys.exit(1)