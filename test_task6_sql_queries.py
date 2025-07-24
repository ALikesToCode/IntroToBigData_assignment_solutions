#!/usr/bin/env python3
"""
Simple test for Task 6 SQL query generation

This script tests just the SQL query generation functions without requiring Spark.
"""

import sys
sys.path.append('.')

from sparksql_scd_type2_implementation import get_final_union_query


def test_final_union_query():
    """Test the SQL UNION query generation."""
    print("Testing final UNION query generation...")
    
    union_query = get_final_union_query()
    
    print("Generated UNION query:")
    print("-" * 40)
    print(union_query)
    print("-" * 40)
    
    # Verify query contains expected components
    checks = [
        ("UNION ALL", "UNION ALL operations"),
        ("unchanged_records", "unchanged_records table"),
        ("closed_historical_records", "closed_historical_records table"),
        ("new_current_changed_records", "new_current_changed_records table"),
        ("new_customer_records", "new_customer_records table"),
        ("ORDER BY customer_key", "proper ordering"),
        ("customer_key", "customer_key column"),
        ("customer_id", "customer_id column"),
        ("customer_name", "customer_name column"),
        ("address", "address column"),
        ("phone", "phone column"),
        ("email", "email column"),
        ("effective_start_date", "effective_start_date column"),
        ("effective_end_date", "effective_end_date column"),
        ("is_current", "is_current column")
    ]
    
    for check_text, description in checks:
        if check_text in union_query:
            print(f"✓ {description} found")
        else:
            print(f"❌ {description} NOT found")
            return False
    
    # Count UNION ALL occurrences (should be 3 for 4 tables)
    union_count = union_query.count("UNION ALL")
    if union_count == 3:
        print(f"✓ Correct number of UNION ALL operations: {union_count}")
    else:
        print(f"❌ Incorrect number of UNION ALL operations: expected 3, got {union_count}")
        return False
    
    print("✅ Final UNION query generation test passed!")
    return True


def main():
    """Run SQL query tests."""
    print("Starting Task 6 SQL query tests...")
    print("=" * 50)
    
    success = test_final_union_query()
    
    print("=" * 50)
    if success:
        print("✅ All Task 6 SQL query tests passed!")
        print("The SQL UNION query for result combination is correctly implemented.")
    else:
        print("❌ Task 6 SQL query tests failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())