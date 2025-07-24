# SparkSQL SCD Type II Implementation Summary

## Course Information
- **Course**: Introduction to Big Data - Week 5 Assignment
- **Author**: Abhyudaya B Tharakan 22f3001492
- **Date**: July 2025
- **Topic**: SCD Type II using SparkSQL on Dataproc

## Implementation Overview

This week's assignment implements Slowly Changing Dimensions Type II (SCD Type II) using **SparkSQL** instead of the DataFrame API used in Week 4. The key difference is that all data processing logic is implemented using SQL queries rather than PySpark DataFrame operations.

## Key SparkSQL Features Demonstrated

### 1. Temporary View Creation
```sql
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
```

### 2. Complex JOIN Operations
```sql
CREATE OR REPLACE TEMPORARY VIEW record_comparison AS
SELECT 
    curr.customer_key,
    curr.customer_id as curr_customer_id,
    -- ... other current fields
    new.customer_id as new_customer_id,
    new.customer_name as new_customer_name,
    -- ... other new fields
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
```

### 3. Window Functions for Surrogate Key Generation
```sql
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
```

### 4. UNION ALL Operations
```sql
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
```

### 5. Aggregate Functions and Statistics
```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_current = true THEN 1 END) as current_records,
    COUNT(CASE WHEN is_current = false THEN 1 END) as historical_records,
    COUNT(DISTINCT customer_id) as unique_customers
FROM final_dimension
```

## Processing Steps

### Step 1: Data Loading and Schema Conversion
- Load CSV files using structured schemas
- Convert string dates to DATE type using `TO_DATE()`
- Create temporary views for SQL processing

### Step 2: Record Type Identification
- Use FULL OUTER JOIN to compare current and new records
- Apply CASE statements to classify records as NEW, CHANGED, or UNCHANGED
- Create separate views for each record type

### Step 3: Process Unchanged Records
- Simple SELECT from unchanged records view
- No transformation needed

### Step 4: Process Changed Records
- Close existing records by setting end date and is_current=false
- Create new versions with new surrogate keys using ROW_NUMBER()
- Set effective_start_date to source date

### Step 5: Process New Records  
- Generate surrogate keys using ROW_NUMBER() with offset
- Set default effective dates and current flag

### Step 6: Combine Results
- Use UNION ALL to combine all processed record types
- Maintain historical records not affected by changes
- Sort by customer_id and effective_start_date

## Comparison: Week 4 (DataFrame API) vs Week 5 (SparkSQL)

| Operation | Week 4 DataFrame API | Week 5 SparkSQL |
|-----------|---------------------|-----------------
| **Data Loading** | `spark.read.schema().csv()` | `spark.read.schema().csv()` + `createOrReplaceTempView()` |
| **Date Conversion** | `.withColumn("date", to_date(col("date")))` | `TO_DATE(TRIM(date_col), 'yyyy-MM-dd')` |
| **Joins** | `df1.join(df2, condition, "full_outer")` | `FULL OUTER JOIN table2 ON condition` |
| **Filtering** | `df.filter(col("is_current") == True)` | `WHERE is_current = true` |
| **Conditional Logic** | `when(condition).otherwise(value)` | `CASE WHEN condition THEN value ELSE other END` |
| **Window Functions** | `Window.partitionBy().orderBy()` | `ROW_NUMBER() OVER (ORDER BY col)` |
| **Aggregations** | `df.agg(max("key")).collect()[0][0]` | `SELECT MAX(key) FROM table` |
| **Union** | `df1.union(df2)` | `SELECT * FROM table1 UNION ALL SELECT * FROM table2` |
| **Column Operations** | `.withColumn("new_col", lit(value))` | `value as new_col` |
| **Ordering** | `.orderBy("col1", "col2")` | `ORDER BY col1, col2` |

## Advantages of SparkSQL Implementation

### 1. **Readability**
- SQL is more familiar to data analysts and business users
- Complex logic is easier to understand in declarative SQL
- Self-documenting queries

### 2. **Maintainability**  
- SQL queries can be version controlled separately
- Easier to modify business logic without Python code changes
- Standard SQL patterns are reusable

### 3. **Performance**
- Catalyst optimizer can better optimize SQL queries
- Automatic query plan optimization
- Better predicate pushdown and column pruning

### 4. **Portability**
- SQL queries can be adapted to other SQL engines
- Less vendor lock-in compared to DataFrame API
- Easier migration between Spark versions

## Data Processing Results

Using the sample data, the SparkSQL implementation processes:

- **Input Records**: 8 existing + 8 new = 16 total input records
- **Unchanged**: 4 customers (John Smith, Sarah Davis, Michael Wilson, Lisa Anderson)
- **Changed**: 2 customers (Mary Johnson - address change, Robert Brown - phone change)  
- **New**: 2 customers (David Miller, Jennifer Taylor)
- **Output Records**: ~10 total (6 current + 4 historical)

### Sample Output:
```
customer_key | customer_id | customer_name   | address          | phone    | effective_start_date | effective_end_date | is_current
-------------|-------------|-----------------|------------------|----------|---------------------|--------------------|-----------
1            | 1001        | John Smith      | 123 Main St      | 555-1234 | 2023-01-01          | 9999-12-31         | true
2            | 1002        | Mary Johnson    | 456 Oak Ave      | 555-5678 | 2023-01-01          | 2024-01-15         | false
9            | 1002        | Mary Johnson    | 789 Sunset Blvd  | 555-5678 | 2024-01-15          | 9999-12-31         | true
4            | 1003        | Robert Brown    | 321 Elm St       | 555-9012 | 2023-06-16          | 2024-01-15         | false
10           | 1003        | Robert Brown    | 321 Elm St       | 555-9999 | 2024-01-15          | 9999-12-31         | true
```

## Cloud Deployment Architecture

### Dataproc Cluster Configuration
```yaml
Cluster Name: sparksql-scd-type2-cluster
Master Node: e2-standard-2 (2 vCPUs, 8GB RAM)
Worker Nodes: 2 x e2-standard-2 (2 vCPUs, 8GB RAM each)
Image Version: 2.0-debian10
Max Idle Time: 10 minutes
```

### GCS Storage Structure
```
gs://{project-id}-sparksql-scd-bucket/
├── data/
│   ├── customer_existing.csv
│   └── customer_new.csv
├── scripts/
│   └── sparksql_scd_type2_implementation.py
└── output/
    └── customer_dimension_{timestamp}/
        ├── part-00000-{uuid}.csv
        ├── _SUCCESS
        └── ... (additional part files if data is large)
```

## Performance Characteristics

### Query Optimization Features Used
- **Adaptive Query Execution (AQE)**: Enabled for runtime optimization
- **Coalesce Partitions**: Reduces small file issues
- **Broadcast Joins**: Automatic for small lookup tables
- **Column Pruning**: Only required columns processed
- **Predicate Pushdown**: Filters applied early

### Execution Plan Highlights
1. **Scan Phase**: Parallel CSV reading with schema enforcement
2. **Join Phase**: Hash joins with broadcast optimization
3. **Window Phase**: Distributed window function execution
4. **Union Phase**: Efficient concatenation of result sets
5. **Write Phase**: Parallel CSV output with single header

## Learning Outcomes

This SparkSQL implementation demonstrates:

### Technical Skills
- Advanced SQL query development for big data processing
- Complex join operations and window functions
- Temporary view management and query optimization
- Cloud deployment and monitoring

### Big Data Concepts
- SCD Type II implementation patterns
- Surrogate key management strategies
- Historical data preservation techniques
- Scalable data pipeline design

### Cloud Engineering
- Dataproc cluster management
- GCS integration and data organization
- Job monitoring and troubleshooting
- Resource cleanup and cost optimization

## Conclusion

The SparkSQL implementation of SCD Type II provides several advantages over the DataFrame API approach:

1. **Better Performance**: SQL optimization by Catalyst
2. **Higher Readability**: Familiar SQL syntax for complex logic
3. **Easier Maintenance**: Declarative approach to data transformations
4. **Greater Portability**: Standard SQL patterns across platforms

The implementation successfully processes customer dimension changes while maintaining full history and generating proper surrogate keys, demonstrating effective use of SparkSQL for enterprise data warehousing patterns.
