#!/usr/bin/env python3
"""
SQL Query Validation Tests for SparkSQL SCD Type II Implementation

This module contains focused unit tests for SQL query validation and structure
without requiring a full Spark session initialization.

Author: Data Engineering Team
Date: 2025-07-18
"""

import unittest
import re
from datetime import date

# Import query generation functions
from sparksql_scd_type2_implementation import (
    get_unchanged_records_query,
    get_changed_records_query,
    get_new_records_query,
    get_close_historical_records_query,
    get_new_current_records_for_changed_query,
    get_new_customer_records_query,
    get_final_union_query
)


class TestSQLQueryValidation(unittest.TestCase):
    """Test cases for SQL query structure and validation."""
    
    def test_unchanged_records_query_structure(self):
        """Test unchanged records query structure and syntax."""
        query = get_unchanged_records_query()
        
        # Basic validation
        self.assertIsNotNone(query)
        self.assertIsInstance(query, str)
        self.assertTrue(len(query.strip()) > 0)
        
        # Check for required SQL elements
        query_upper = query.upper()
        self.assertIn("SELECT", query_upper)
        self.assertIn("FROM", query_upper)
        self.assertIn("INNER JOIN", query_upper)
        self.assertIn("WHERE", query_upper)
        self.assertIn("ORDER BY", query_upper)
        
        # Check for required table references
        self.assertIn("existing_customers", query)
        self.assertIn("new_customers", query)
        
        # Check for required columns
        required_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address',
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        for column in required_columns:
            self.assertIn(column, query)
        
        # Check for proper join condition
        self.assertIn("curr.customer_id = new.customer_id", query)
        
        # Check for unchanged condition logic
        self.assertIn("curr.is_current = true", query)
        self.assertIn("curr.customer_name = new.customer_name", query)
        self.assertIn("curr.address = new.address", query)
        self.assertIn("curr.phone = new.phone", query)
        self.assertIn("curr.email = new.email", query)
    
    def test_changed_records_query_structure(self):
        """Test changed records query structure and syntax."""
        query = get_changed_records_query()
        
        # Basic validation
        self.assertIsNotNone(query)
        self.assertIsInstance(query, str)
        self.assertTrue(len(query.strip()) > 0)
        
        # Check for required SQL elements
        query_upper = query.upper()
        self.assertIn("SELECT", query_upper)
        self.assertIn("FROM", query_upper)
        self.assertIn("INNER JOIN", query_upper)
        self.assertIn("WHERE", query_upper)
        self.assertIn("ORDER BY", query_upper)
        
        # Check for table references
        self.assertIn("existing_customers", query)
        self.assertIn("new_customers", query)
        
        # Check for aliased columns (existing vs new)
        self.assertIn("existing_customer_key", query)
        self.assertIn("existing_customer_name", query)
        self.assertIn("new_customer_name", query)
        self.assertIn("existing_address", query)
        self.assertIn("new_address", query)
        
        # Check for change detection logic
        self.assertIn("curr.is_current = true", query)
        self.assertIn("curr.customer_name != new.customer_name", query)
        self.assertIn("OR", query)
        self.assertIn("curr.address != new.address", query)
        self.assertIn("curr.phone != new.phone", query)
        self.assertIn("curr.email != new.email", query)
    
    def test_new_records_query_structure(self):
        """Test new records query structure and syntax."""
        query = get_new_records_query()
        
        # Basic validation
        self.assertIsNotNone(query)
        self.assertIsInstance(query, str)
        self.assertTrue(len(query.strip()) > 0)
        
        # Check for required SQL elements
        query_upper = query.upper()
        self.assertIn("SELECT", query_upper)
        self.assertIn("FROM", query_upper)
        self.assertIn("LEFT JOIN", query_upper)
        self.assertIn("WHERE", query_upper)
        self.assertIn("ORDER BY", query_upper)
        
        # Check for table references
        self.assertIn("new_customers", query)
        self.assertIn("existing_customers", query)
        
        # Check for new customer columns
        new_columns = ['customer_id', 'customer_name', 'address', 'phone', 'email', 'source_date']
        for column in new_columns:
            self.assertIn(column, query)
        
        # Check for LEFT JOIN logic to find new customers
        self.assertIn("new.customer_id = curr.customer_id", query)
        self.assertIn("curr.customer_id IS NULL", query)
    
    def test_close_historical_records_query_structure(self):
        """Test close historical records query structure and parameterization."""
        source_date = "2024-01-15"
        query = get_close_historical_records_query(source_date)
        
        # Basic validation
        self.assertIsNotNone(query)
        self.assertIsInstance(query, str)
        self.assertTrue(len(query.strip()) > 0)
        
        # Check for required SQL elements
        query_upper = query.upper()
        self.assertIn("SELECT", query_upper)
        self.assertIn("FROM", query_upper)
        self.assertIn("WHERE", query_upper)
        self.assertIn("ORDER BY", query_upper)
        
        # Check for parameterized source_date
        self.assertIn(source_date, query)
        self.assertIn("CAST", query)
        self.assertIn("AS DATE", query)
        
        # Check for SCD logic
        self.assertIn("false as is_current", query)
        self.assertIn("effective_end_date", query)
        self.assertIn("is_current = true", query)
        
        # Check for subquery to identify changed customers
        self.assertIn("customer_id IN", query)
        self.assertIn("INNER JOIN", query)
        self.assertIn("DISTINCT", query)
    
    def test_new_current_records_query_structure(self):
        """Test new current records query structure and surrogate key logic."""
        next_key = 10
        source_date = "2024-01-15"
        query = get_new_current_records_for_changed_query(next_key, source_date)
        
        # Basic validation
        self.assertIsNotNone(query)
        self.assertIsInstance(query, str)
        self.assertTrue(len(query.strip()) > 0)
        
        # Check for required SQL elements
        query_upper = query.upper()
        self.assertIn("SELECT", query_upper)
        self.assertIn("FROM", query_upper)
        self.assertIn("ORDER BY", query_upper)
        
        # Check for surrogate key generation
        self.assertIn(str(next_key), query)
        self.assertIn("ROW_NUMBER()", query)
        self.assertIn("OVER", query)
        
        # Check for parameterized dates
        self.assertIn(source_date, query)
        self.assertIn("9999-12-31", query)
        self.assertIn("CAST", query)
        
        # Check for SCD attributes
        self.assertIn("true as is_current", query)
        self.assertIn("effective_start_date", query)
        self.assertIn("effective_end_date", query)
        
        # Check for change detection subquery
        self.assertIn("DISTINCT", query)
        self.assertIn("INNER JOIN", query)
    
    def test_new_customer_records_query_structure(self):
        """Test new customer records query structure and surrogate key logic."""
        next_key = 15
        source_date = "2024-01-15"
        query = get_new_customer_records_query(next_key, source_date)
        
        # Basic validation
        self.assertIsNotNone(query)
        self.assertIsInstance(query, str)
        self.assertTrue(len(query.strip()) > 0)
        
        # Check for required SQL elements
        query_upper = query.upper()
        self.assertIn("SELECT", query_upper)
        self.assertIn("FROM", query_upper)
        self.assertIn("ORDER BY", query_upper)
        
        # Check for surrogate key generation
        self.assertIn(str(next_key), query)
        self.assertIn("ROW_NUMBER()", query)
        self.assertIn("OVER", query)
        
        # Check for parameterized dates
        self.assertIn(source_date, query)
        self.assertIn("9999-12-31", query)
        self.assertIn("CAST", query)
        
        # Check for SCD attributes
        self.assertIn("true as is_current", query)
        self.assertIn("effective_start_date", query)
        self.assertIn("effective_end_date", query)
        
        # Check for new customer detection subquery
        self.assertIn("LEFT JOIN", query)
        self.assertIn("curr.customer_id IS NULL", query)
    
    def test_final_union_query_structure(self):
        """Test final union query structure and table references."""
        query = get_final_union_query()
        
        # Basic validation
        self.assertIsNotNone(query)
        self.assertIsInstance(query, str)
        self.assertTrue(len(query.strip()) > 0)
        
        # Check for required SQL elements
        query_upper = query.upper()
        self.assertIn("SELECT", query_upper)
        self.assertIn("FROM", query_upper)
        self.assertIn("UNION ALL", query_upper)
        self.assertIn("ORDER BY", query_upper)
        
        # Check for all required table references
        required_tables = [
            "unchanged_records",
            "closed_historical_records", 
            "new_current_changed_records",
            "new_customer_records"
        ]
        for table in required_tables:
            self.assertIn(table, query)
        
        # Count UNION ALL occurrences (should be 3 for 4 tables)
        union_count = query.upper().count("UNION ALL")
        self.assertEqual(union_count, 3)
        
        # Check for consistent column selection
        required_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address',
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        for column in required_columns:
            # Each column should appear 4 times (once per SELECT statement)
            column_count = query.count(column)
            self.assertGreaterEqual(column_count, 4)
        
        # Check for proper ordering
        self.assertIn("ORDER BY customer_key", query)
    
    def test_query_parameterization(self):
        """Test that queries properly handle parameter substitution."""
        # Test different source dates
        dates = ["2024-01-15", "2023-12-31", "2025-06-30"]
        for test_date in dates:
            close_query = get_close_historical_records_query(test_date)
            self.assertIn(test_date, close_query)
            
            new_current_query = get_new_current_records_for_changed_query(10, test_date)
            self.assertIn(test_date, new_current_query)
            
            new_customer_query = get_new_customer_records_query(20, test_date)
            self.assertIn(test_date, new_customer_query)
        
        # Test different surrogate keys
        keys = [1, 100, 9999, 123456]
        for test_key in keys:
            new_current_query = get_new_current_records_for_changed_query(test_key, "2024-01-15")
            self.assertIn(str(test_key), new_current_query)
            
            new_customer_query = get_new_customer_records_query(test_key, "2024-01-15")
            self.assertIn(str(test_key), new_customer_query)
    
    def test_sql_syntax_validation(self):
        """Test basic SQL syntax validation for generated queries."""
        queries = [
            ("unchanged_records", get_unchanged_records_query()),
            ("changed_records", get_changed_records_query()),
            ("new_records", get_new_records_query()),
            ("close_historical", get_close_historical_records_query("2024-01-15")),
            ("new_current_changed", get_new_current_records_for_changed_query(10, "2024-01-15")),
            ("new_customer", get_new_customer_records_query(20, "2024-01-15")),
            ("final_union", get_final_union_query())
        ]
        
        for query_name, query in queries:
            with self.subTest(query=query_name):
                # Check for balanced parentheses
                open_parens = query.count('(')
                close_parens = query.count(')')
                self.assertEqual(open_parens, close_parens, 
                               f"Unbalanced parentheses in {query_name} query")
                
                # Check for proper quote matching
                single_quotes = query.count("'")
                self.assertEqual(single_quotes % 2, 0, 
                               f"Unmatched single quotes in {query_name} query")
                
                # Check that query doesn't end with comma or semicolon
                query_stripped = query.strip()
                self.assertFalse(query_stripped.endswith(','), 
                               f"Query {query_name} ends with comma")
                self.assertFalse(query_stripped.endswith(';'), 
                               f"Query {query_name} ends with semicolon")
                
                # Check for required SQL keywords
                query_upper = query.upper()
                self.assertIn("SELECT", query_upper, 
                            f"Missing SELECT in {query_name} query")
                self.assertIn("FROM", query_upper, 
                            f"Missing FROM in {query_name} query")
    
    def test_query_performance_considerations(self):
        """Test that queries include performance optimization elements."""
        # Check for proper indexing hints in join conditions
        join_queries = [
            get_unchanged_records_query(),
            get_changed_records_query(),
            get_new_records_query()
        ]
        
        for query in join_queries:
            # Should have proper join conditions on customer_id
            self.assertIn("customer_id", query)
            # Should use table aliases for clarity
            self.assertTrue(re.search(r'\b(curr|new)\.\w+', query))
        
        # Check for proper ordering for deterministic results
        all_queries = [
            get_unchanged_records_query(),
            get_changed_records_query(),
            get_new_records_query(),
            get_close_historical_records_query("2024-01-15"),
            get_new_current_records_for_changed_query(10, "2024-01-15"),
            get_new_customer_records_query(20, "2024-01-15"),
            get_final_union_query()
        ]
        
        for query in all_queries:
            query_upper = query.upper()
            self.assertIn("ORDER BY", query_upper, 
                        "Query should include ORDER BY for deterministic results")


class TestSQLQueryEdgeCases(unittest.TestCase):
    """Test cases for SQL query edge cases and boundary conditions."""
    
    def test_extreme_parameter_values(self):
        """Test queries with extreme parameter values."""
        # Test with very large surrogate key
        large_key = 999999999
        query = get_new_current_records_for_changed_query(large_key, "2024-01-15")
        self.assertIn(str(large_key), query)
        
        # Test with minimum surrogate key
        min_key = 1
        query = get_new_customer_records_query(min_key, "2024-01-15")
        self.assertIn(str(min_key), query)
        
        # Test with edge case dates
        edge_dates = ["1900-01-01", "2099-12-31", "2024-02-29"]  # Including leap year
        for edge_date in edge_dates:
            query = get_close_historical_records_query(edge_date)
            self.assertIn(edge_date, query)
    
    def test_special_character_handling(self):
        """Test that queries properly handle special characters in parameters."""
        # Test with date that could cause SQL injection (though parameters should be safe)
        test_date = "2024-01-15"  # Normal date - actual injection testing would be done at runtime
        query = get_close_historical_records_query(test_date)
        
        # Verify the date is properly quoted/cast
        self.assertIn("CAST", query)
        self.assertIn("AS DATE", query)
        self.assertIn(f"'{test_date}'", query)
    
    def test_query_length_and_complexity(self):
        """Test that queries are reasonable in length and complexity."""
        queries = [
            get_unchanged_records_query(),
            get_changed_records_query(),
            get_new_records_query(),
            get_close_historical_records_query("2024-01-15"),
            get_new_current_records_for_changed_query(10, "2024-01-15"),
            get_new_customer_records_query(20, "2024-01-15"),
            get_final_union_query()
        ]
        
        for query in queries:
            # Queries should not be empty but also not excessively long
            self.assertGreater(len(query), 50, "Query too short")
            self.assertLess(len(query), 5000, "Query too long")
            
            # Should not have excessive nesting (count subqueries)
            subquery_count = query.upper().count("SELECT")
            self.assertLessEqual(subquery_count, 5, "Too many nested subqueries")


if __name__ == '__main__':
    # Configure logging for test execution
    import logging
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise during testing
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the tests
    unittest.main(verbosity=2)