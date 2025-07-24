# Requirements Document

## Introduction

This feature implements Slowly Changing Dimensions Type II (SCD Type II) for customer master data using SparkSQL instead of PySpark DataFrame operations. The solution must execute on a Google Cloud Dataproc cluster and handle customer data changes by maintaining historical records while keeping current records active. This builds upon the existing PySpark implementation but converts the logic to use pure SQL statements for better performance and readability.

## Requirements

### Requirement 1

**User Story:** As a data engineer, I want to implement SCD Type II using SparkSQL instead of PySpark DataFrame operations, so that I can leverage SQL's declarative syntax and potentially better query optimization.

#### Acceptance Criteria

1. WHEN the system processes customer data THEN it SHALL use SparkSQL statements instead of PySpark DataFrame transformations
2. WHEN executing SQL queries THEN the system SHALL create temporary views for data manipulation
3. WHEN generating surrogate keys THEN the system SHALL use SQL window functions with ROW_NUMBER()
4. WHEN identifying record changes THEN the system SHALL use SQL JOIN operations with CASE statements

### Requirement 2

**User Story:** As a data engineer, I want to maintain the same SCD Type II logic as the existing PySpark implementation, so that the business rules and data integrity are preserved.

#### Acceptance Criteria

1. WHEN processing unchanged records THEN the system SHALL preserve them exactly as-is using SQL SELECT statements
2. WHEN processing changed records THEN the system SHALL close existing records by setting effective_end_date and is_current=false using SQL UPDATE logic
3. WHEN processing changed records THEN the system SHALL create new current records with new surrogate keys using SQL INSERT logic
4. WHEN processing new customers THEN the system SHALL create new records with current effective dates using SQL INSERT statements
5. WHEN combining results THEN the system SHALL use SQL UNION operations to merge all record types

### Requirement 3

**User Story:** As a data engineer, I want the SparkSQL implementation to execute on Google Cloud Dataproc, so that it can handle large-scale data processing in a distributed environment.

#### Acceptance Criteria

1. WHEN deploying to Dataproc THEN the system SHALL create a Spark session configured for cluster execution
2. WHEN executing on Dataproc THEN the system SHALL read input files from Google Cloud Storage or local cluster storage
3. WHEN saving results THEN the system SHALL write output to a distributed storage location accessible by the cluster
4. WHEN running the job THEN the system SHALL complete successfully on the Dataproc cluster environment

### Requirement 4

**User Story:** As a data engineer, I want to use the same input data structure as the existing implementation, so that I can validate the SparkSQL results against the PySpark results.

#### Acceptance Criteria

1. WHEN reading existing customer data THEN the system SHALL use the same CSV schema with customer_key, customer_id, customer_name, address, phone, email, effective_start_date, effective_end_date, is_current
2. WHEN reading new customer data THEN the system SHALL use the same CSV schema with customer_id, customer_name, address, phone, email, source_date
3. WHEN processing data THEN the system SHALL handle the same test scenarios: unchanged records, address changes, phone changes, and new customers
4. WHEN generating output THEN the system SHALL produce the same final record count and structure as the PySpark implementation

### Requirement 5

**User Story:** As a data engineer, I want comprehensive logging and error handling in the SparkSQL implementation, so that I can troubleshoot issues and monitor execution progress.

#### Acceptance Criteria

1. WHEN executing SQL statements THEN the system SHALL log each major SQL operation with record counts
2. WHEN errors occur THEN the system SHALL provide detailed error messages with context about which SQL operation failed
3. WHEN processing completes THEN the system SHALL display a summary showing total records, current records, historical records, and unique customers
4. WHEN running in debug mode THEN the system SHALL show sample data from intermediate SQL results

### Requirement 6

**User Story:** As a data engineer, I want the SparkSQL implementation to be deployable to Dataproc with the same automation as the existing PySpark version, so that I can easily test and run the solution in the cloud.

#### Acceptance Criteria

1. WHEN deploying to Dataproc THEN the system SHALL use the same deployment script pattern as the existing implementation
2. WHEN submitting the Spark job THEN the system SHALL pass the correct arguments for input and output paths
3. WHEN the job completes THEN the system SHALL download results and display summary statistics
4. WHEN cleaning up THEN the system SHALL properly terminate Spark sessions and clean up temporary resources