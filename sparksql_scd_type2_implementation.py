#!/usr/bin/env python3
"""
SparkSQL SCD Type II Implementation

This module implements Slowly Changing Dimensions Type II (SCD Type II) for customer master data
using SparkSQL instead of PySpark DataFrame operations. The solution executes on Google Cloud
Dataproc and handles customer data changes by maintaining historical records while keeping
current records active.

Author: Data Engineering Team
Date: 2025-07-18
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType


def setup_logging(debug: bool = False) -> None:
    """
    Configure logging for the application.
    
    Args:
        debug: Enable debug level logging if True
    """
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce Spark logging noise
    logging.getLogger('pyspark').setLevel(logging.WARN)
    logging.getLogger('py4j').setLevel(logging.WARN)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description='SparkSQL SCD Type II Implementation for Customer Data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local execution
  python sparksql_scd_type2_implementation.py \\
    --existing-data data/customer_existing.csv \\
    --new-data data/customer_new.csv \\
    --output-path output/customer_dimension.csv

  # Debug mode with detailed logging
  python sparksql_scd_type2_implementation.py \\
    --existing-data data/customer_existing.csv \\
    --new-data data/customer_new.csv \\
    --output-path output/customer_dimension.csv \\
    --debug
        """
    )
    
    parser.add_argument(
        '--existing-data',
        required=True,
        help='Path to existing customer dimension CSV file'
    )
    
    parser.add_argument(
        '--new-data',
        required=True,
        help='Path to new customer source CSV file'
    )
    
    parser.add_argument(
        '--output-path',
        required=True,
        help='Path for output customer dimension CSV file'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode with detailed logging and intermediate results'
    )
    
    parser.add_argument(
        '--app-name',
        default='SparkSQL-SCD-Type2',
        help='Spark application name (default: SparkSQL-SCD-Type2)'
    )
    
    return parser.parse_args()


def create_spark_session(app_name: str, debug: bool = False) -> SparkSession:
    """
    Create and configure Spark session optimized for SQL operations with comprehensive error handling.
    
    Args:
        app_name: Name for the Spark application
        debug: Enable debug configurations if True
        
    Returns:
        Configured SparkSession instance
        
    Raises:
        RuntimeError: If Spark session creation fails
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting Spark session creation: {app_name}")
    
    start_time = datetime.now()
    
    try:
        # Validate input parameters
        if not app_name or not app_name.strip():
            raise ValueError("Invalid or empty application name provided")
        
        logger.info(f"Building Spark session with app name: {app_name}")
        
        # Build Spark session with SQL-optimized configurations
        builder = SparkSession.builder.appName(app_name)
        
        # SQL optimization configurations with error handling
        try:
            logger.debug("Applying SQL optimization configurations...")
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            builder = builder.config("spark.sql.adaptive.skewJoin.enabled", "true")
            builder = builder.config("spark.sql.cbo.enabled", "true")
            builder = builder.config("spark.sql.cbo.joinReorder.enabled", "true")
            logger.debug("SQL optimization configurations applied successfully")
        except Exception as config_error:
            logger.warning(f"Failed to apply some SQL optimization configurations: {str(config_error)}")
            logger.warning("Continuing with basic configuration")
        
        # Memory and performance configurations with error handling
        try:
            logger.debug("Applying memory and performance configurations...")
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            logger.debug("Memory and performance configurations applied successfully")
        except Exception as perf_error:
            logger.warning(f"Failed to apply some performance configurations: {str(perf_error)}")
            logger.warning("Continuing with basic configuration")
        
        # Debug configurations with error handling
        if debug:
            try:
                logger.debug("Applying debug configurations...")
                builder = builder.config("spark.sql.execution.debug.maxToStringFields", "100")
                builder = builder.config("spark.sql.repl.eagerEval.enabled", "true")
                builder = builder.config("spark.sql.repl.eagerEval.maxNumRows", "20")
                logger.debug("Debug configurations applied successfully")
            except Exception as debug_error:
                logger.warning(f"Failed to apply debug configurations: {str(debug_error)}")
                logger.warning("Continuing without debug configurations")
        
        # Create session with comprehensive error handling
        logger.info("Creating Spark session...")
        try:
            spark = builder.getOrCreate()
            logger.info("Spark session created successfully")
        except Exception as creation_error:
            logger.error(f"Critical error during Spark session creation: {str(creation_error)}")
            logger.error(f"Error type: {type(creation_error).__name__}")
            
            # Try to provide helpful error context
            if "java.net.ConnectException" in str(creation_error):
                logger.error("Connection error detected - check if Spark master is accessible")
            elif "OutOfMemoryError" in str(creation_error):
                logger.error("Memory error detected - consider reducing memory requirements")
            elif "ClassNotFoundException" in str(creation_error):
                logger.error("Class not found error - check Spark installation and classpath")
            
            raise RuntimeError(f"Failed to create Spark session: {str(creation_error)}") from creation_error
        
        # Configure Spark session with error handling
        try:
            # Set log level for Spark SQL
            log_level = "WARN" if not debug else "INFO"
            spark.sparkContext.setLogLevel(log_level)
            logger.debug(f"Set Spark log level to: {log_level}")
        except Exception as log_error:
            logger.warning(f"Failed to set Spark log level: {str(log_error)}")
            logger.warning("Continuing with default log level")
        
        # Validate session and gather information
        try:
            session_info = {
                "version": spark.version,
                "master": spark.sparkContext.master,
                "app_id": spark.sparkContext.applicationId,
                "app_name": spark.sparkContext.appName,
                "default_parallelism": spark.sparkContext.defaultParallelism
            }
            
            creation_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Spark session validation successful in {creation_time:.2f} seconds")
            logger.info(f"Session details:")
            for key, value in session_info.items():
                logger.info(f"  {key}: {value}")
            
            # Additional debug information
            if debug:
                try:
                    logger.debug("Additional Spark session debug information:")
                    logger.debug(f"  Spark UI URL: {spark.sparkContext.uiWebUrl}")
                    logger.debug(f"  Total executor cores: {spark.sparkContext.statusTracker().getExecutorInfos()}")
                except Exception as debug_info_error:
                    logger.debug(f"Could not gather additional debug info: {str(debug_info_error)}")
            
        except Exception as validation_error:
            logger.error(f"Spark session validation failed: {str(validation_error)}")
            # Try to stop the session if validation fails
            try:
                spark.stop()
            except:
                pass
            raise RuntimeError(f"Spark session validation failed: {str(validation_error)}") from validation_error
        
        logger.info("Spark session creation and validation completed successfully")
        return spark
        
    except Exception as e:
        creation_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"CRITICAL ERROR: Failed to create Spark session after {creation_time:.2f} seconds")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Log system information for debugging
        try:
            import os
            logger.error("System environment information:")
            logger.error(f"  SPARK_HOME: {os.environ.get('SPARK_HOME', 'Not set')}")
            logger.error(f"  JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
            logger.error(f"  PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")
        except Exception as env_error:
            logger.error(f"Could not gather environment information: {str(env_error)}")
        
        raise RuntimeError(f"Critical failure in Spark session creation: {str(e)}") from e


def get_existing_customer_schema() -> StructType:
    """
    Define schema for existing customer dimension data.
    
    Returns:
        StructType schema for existing customer data
    """
    return StructType([
        StructField("customer_key", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("email", StringType(), False),
        StructField("effective_start_date", DateType(), False),
        StructField("effective_end_date", DateType(), False),
        StructField("is_current", BooleanType(), False)
    ])


def get_new_customer_schema() -> StructType:
    """
    Define schema for new customer source data.
    
    Returns:
        StructType schema for new customer data
    """
    return StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("email", StringType(), False),
        StructField("source_date", DateType(), False)
    ])


def load_existing_customer_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load existing customer dimension CSV data with explicit schema definition and comprehensive error handling.
    
    Args:
        spark: SparkSession instance
        file_path: Path to existing customer CSV file
        
    Returns:
        DataFrame containing existing customer data
        
    Raises:
        RuntimeError: If file loading fails or data validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting to load existing customer data from: {file_path}")
    
    start_time = datetime.now()
    
    try:
        # Input validation
        if not spark:
            raise ValueError("Invalid SparkSession provided")
        
        if not file_path or not file_path.strip():
            raise ValueError("Invalid or empty file path provided")
        
        logger.info(f"Validating file path and preparing to load: {file_path}")
        
        # Get schema definition with error handling
        try:
            schema = get_existing_customer_schema()
            logger.debug(f"Schema definition retrieved successfully with {len(schema.fields)} fields")
        except Exception as schema_error:
            logger.error(f"Failed to get existing customer schema: {str(schema_error)}")
            raise RuntimeError(f"Schema definition failed: {str(schema_error)}") from schema_error
        
        # Configure CSV reading options
        csv_options = {
            "header": "true",
            "inferSchema": "false",
            "dateFormat": "yyyy-MM-dd",
            "timestampFormat": "yyyy-MM-dd HH:mm:ss",
            "nullValue": "",
            "emptyValue": ""
        }
        
        logger.debug("CSV reading options configured:")
        for key, value in csv_options.items():
            logger.debug(f"  {key}: {value}")
        
        # Load CSV with explicit schema and comprehensive error handling
        try:
            logger.info("Reading CSV file with explicit schema...")
            df = spark.read \
                .options(**csv_options) \
                .schema(schema) \
                .csv(file_path)
            logger.debug("CSV file read operation completed successfully")
        except Exception as read_error:
            logger.error(f"Failed to read CSV file: {str(read_error)}")
            logger.error(f"Error type: {type(read_error).__name__}")
            
            # Provide specific error context
            if "FileNotFoundException" in str(read_error) or "Path does not exist" in str(read_error):
                logger.error(f"File not found: {file_path}")
                logger.error("Please verify the file path exists and is accessible")
            elif "PermissionDenied" in str(read_error):
                logger.error(f"Permission denied accessing file: {file_path}")
            elif "MalformedCSVException" in str(read_error):
                logger.error(f"Malformed CSV file detected: {file_path}")
                logger.error("Please verify the CSV file format and structure")
            
            raise RuntimeError(f"CSV file reading failed: {str(read_error)}") from read_error
        
        # Validate data loading with comprehensive error handling
        try:
            logger.info("Validating loaded data...")
            record_count = df.count()
            load_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Data loading completed in {load_time:.2f} seconds")
        except Exception as count_error:
            logger.error(f"Failed to count records in loaded data: {str(count_error)}")
            logger.error("This may indicate data corruption or access issues")
            raise RuntimeError(f"Record count validation failed: {str(count_error)}") from count_error
        
        if record_count == 0:
            logger.error(f"No records found in existing customer file: {file_path}")
            logger.error("Please verify the file contains data and has proper headers")
            raise ValueError(f"Empty dataset: No records found in {file_path}")
        
        logger.info(f"Successfully loaded {record_count} existing customer records")
        
        # Comprehensive data validation checks with detailed error handling
        logger.info("Performing data quality validation checks...")
        
        try:
            # Check for null customer_ids
            null_customer_ids = df.filter(df.customer_id.isNull()).count()
            if null_customer_ids > 0:
                logger.error(f"Data quality issue: Found {null_customer_ids} records with null customer_id")
                raise ValueError(f"Data integrity violation: {null_customer_ids} records with null customer_id in existing data")
            logger.debug("Customer ID validation passed - no null values found")
        except Exception as validation_error:
            if "Data integrity violation" in str(validation_error):
                raise
            logger.error(f"Failed to validate customer_id field: {str(validation_error)}")
            raise RuntimeError(f"Customer ID validation failed: {str(validation_error)}") from validation_error
        
        try:
            # Check for null customer_keys
            null_customer_keys = df.filter(df.customer_key.isNull()).count()
            if null_customer_keys > 0:
                logger.error(f"Data quality issue: Found {null_customer_keys} records with null customer_key")
                raise ValueError(f"Data integrity violation: {null_customer_keys} records with null customer_key in existing data")
            logger.debug("Customer key validation passed - no null values found")
        except Exception as validation_error:
            if "Data integrity violation" in str(validation_error):
                raise
            logger.error(f"Failed to validate customer_key field: {str(validation_error)}")
            raise RuntimeError(f"Customer key validation failed: {str(validation_error)}") from validation_error
        
        # Additional data quality checks with error handling
        try:
            # Check for duplicate customer_keys
            distinct_keys = df.select("customer_key").distinct().count()
            if distinct_keys != record_count:
                duplicate_keys = record_count - distinct_keys
                logger.warning(f"Data quality warning: Found {duplicate_keys} duplicate customer_key values")
            
            # Validate current/historical record distribution
            current_records = df.filter(df.is_current == True).count()
            historical_records = df.filter(df.is_current == False).count()
            
            if current_records + historical_records != record_count:
                logger.warning("Data quality warning: is_current field validation issue detected")
            
            logger.info(f"Data quality summary:")
            logger.info(f"  Total records: {record_count}")
            logger.info(f"  Current records: {current_records}")
            logger.info(f"  Historical records: {historical_records}")
            logger.info(f"  Unique customer keys: {distinct_keys}")
            
        except Exception as quality_error:
            logger.warning(f"Failed to perform additional data quality checks: {str(quality_error)}")
            logger.warning("Continuing with basic validation only")
        
        # Final validation and caching
        try:
            # Cache the DataFrame for better performance
            df.cache()
            logger.debug("Successfully cached existing customer data")
        except Exception as cache_error:
            logger.warning(f"Failed to cache existing customer data: {str(cache_error)}")
            logger.warning("Continuing without caching - performance may be impacted")
        
        logger.info("Existing customer data loading and validation completed successfully")
        return df
        
    except Exception as e:
        load_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"CRITICAL ERROR: Failed to load existing customer data after {load_time:.2f} seconds")
        logger.error(f"File path: {file_path}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Log additional debugging information
        try:
            import os
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                logger.error(f"File exists with size: {file_size} bytes")
            else:
                logger.error("File does not exist at specified path")
        except Exception as debug_error:
            logger.error(f"Could not gather file information: {str(debug_error)}")
        
        raise RuntimeError(f"Critical failure loading existing customer data: {str(e)}") from e


def load_new_customer_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load new customer source CSV data with explicit schema definition and comprehensive error handling.
    
    Args:
        spark: SparkSession instance
        file_path: Path to new customer CSV file
        
    Returns:
        DataFrame containing new customer data
        
    Raises:
        RuntimeError: If file loading fails or data validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting to load new customer data from: {file_path}")
    
    start_time = datetime.now()
    
    try:
        # Input validation
        if not spark:
            raise ValueError("Invalid SparkSession provided")
        
        if not file_path or not file_path.strip():
            raise ValueError("Invalid or empty file path provided")
        
        logger.info(f"Validating file path and preparing to load: {file_path}")
        
        # Get schema definition with error handling
        try:
            schema = get_new_customer_schema()
            logger.debug(f"Schema definition retrieved successfully with {len(schema.fields)} fields")
        except Exception as schema_error:
            logger.error(f"Failed to get new customer schema: {str(schema_error)}")
            raise RuntimeError(f"Schema definition failed: {str(schema_error)}") from schema_error
        
        # Configure CSV reading options
        csv_options = {
            "header": "true",
            "inferSchema": "false",
            "dateFormat": "yyyy-MM-dd",
            "timestampFormat": "yyyy-MM-dd HH:mm:ss",
            "nullValue": "",
            "emptyValue": ""
        }
        
        logger.debug("CSV reading options configured:")
        for key, value in csv_options.items():
            logger.debug(f"  {key}: {value}")
        
        # Load CSV with explicit schema and comprehensive error handling
        try:
            logger.info("Reading CSV file with explicit schema...")
            df = spark.read \
                .options(**csv_options) \
                .schema(schema) \
                .csv(file_path)
            logger.debug("CSV file read operation completed successfully")
        except Exception as read_error:
            logger.error(f"Failed to read CSV file: {str(read_error)}")
            logger.error(f"Error type: {type(read_error).__name__}")
            
            # Provide specific error context
            if "FileNotFoundException" in str(read_error) or "Path does not exist" in str(read_error):
                logger.error(f"File not found: {file_path}")
                logger.error("Please verify the file path exists and is accessible")
            elif "PermissionDenied" in str(read_error):
                logger.error(f"Permission denied accessing file: {file_path}")
            elif "MalformedCSVException" in str(read_error):
                logger.error(f"Malformed CSV file detected: {file_path}")
                logger.error("Please verify the CSV file format and structure")
            
            raise RuntimeError(f"CSV file reading failed: {str(read_error)}") from read_error
        
        # Validate data loading with comprehensive error handling
        try:
            logger.info("Validating loaded data...")
            record_count = df.count()
            load_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Data loading completed in {load_time:.2f} seconds")
        except Exception as count_error:
            logger.error(f"Failed to count records in loaded data: {str(count_error)}")
            logger.error("This may indicate data corruption or access issues")
            raise RuntimeError(f"Record count validation failed: {str(count_error)}") from count_error
        
        if record_count == 0:
            logger.error(f"No records found in new customer file: {file_path}")
            logger.error("Please verify the file contains data and has proper headers")
            raise ValueError(f"Empty dataset: No records found in {file_path}")
        
        logger.info(f"Successfully loaded {record_count} new customer records")
        
        # Comprehensive data validation checks with detailed error handling
        logger.info("Performing data quality validation checks...")
        
        try:
            # Check for null customer_ids
            null_customer_ids = df.filter(df.customer_id.isNull()).count()
            if null_customer_ids > 0:
                logger.error(f"Data quality issue: Found {null_customer_ids} records with null customer_id")
                raise ValueError(f"Data integrity violation: {null_customer_ids} records with null customer_id in new data")
            logger.debug("Customer ID validation passed - no null values found")
        except Exception as validation_error:
            if "Data integrity violation" in str(validation_error):
                raise
            logger.error(f"Failed to validate customer_id field: {str(validation_error)}")
            raise RuntimeError(f"Customer ID validation failed: {str(validation_error)}") from validation_error
        
        # Additional data quality checks with error handling
        try:
            # Check for duplicate customer_ids in new data
            distinct_customers = df.select("customer_id").distinct().count()
            if distinct_customers != record_count:
                duplicate_count = record_count - distinct_customers
                logger.warning(f"Data quality warning: Found {duplicate_count} duplicate customer_id entries in new data")
                logger.warning("This may indicate data quality issues in the source system")
            
            # Validate source date information
            source_dates = df.select("source_date").distinct().collect()
            source_date_count = len(source_dates)
            
            logger.info(f"Source date validation:")
            logger.info(f"  Total records: {record_count}")
            logger.info(f"  Unique customer IDs: {distinct_customers}")
            logger.info(f"  Distinct source dates: {source_date_count}")
            
            for i, row in enumerate(source_dates, 1):
                logger.info(f"  Source date {i}: {row.source_date}")
            
            # Validate that source dates are not null
            null_source_dates = df.filter(df.source_date.isNull()).count()
            if null_source_dates > 0:
                logger.warning(f"Data quality warning: Found {null_source_dates} records with null source_date")
            
        except Exception as quality_error:
            logger.warning(f"Failed to perform additional data quality checks: {str(quality_error)}")
            logger.warning("Continuing with basic validation only")
        
        # Final validation and caching
        try:
            # Cache the DataFrame for better performance
            df.cache()
            logger.debug("Successfully cached new customer data")
        except Exception as cache_error:
            logger.warning(f"Failed to cache new customer data: {str(cache_error)}")
            logger.warning("Continuing without caching - performance may be impacted")
        
        logger.info("New customer data loading and validation completed successfully")
        return df
        
    except Exception as e:
        load_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"CRITICAL ERROR: Failed to load new customer data after {load_time:.2f} seconds")
        logger.error(f"File path: {file_path}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Log additional debugging information
        try:
            import os
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                logger.error(f"File exists with size: {file_size} bytes")
            else:
                logger.error("File does not exist at specified path")
        except Exception as debug_error:
            logger.error(f"Could not gather file information: {str(debug_error)}")
        
        raise RuntimeError(f"Critical failure loading new customer data: {str(e)}") from e


def register_temporary_views(spark: SparkSession, existing_df: DataFrame, new_df: DataFrame) -> None:
    """
    Register DataFrames as temporary SQL views for SparkSQL operations with comprehensive error handling.
    
    Args:
        spark: SparkSession instance
        existing_df: DataFrame containing existing customer data
        new_df: DataFrame containing new customer data
        
    Raises:
        RuntimeError: If view registration fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting temporary SQL views registration")
    
    start_time = datetime.now()
    
    try:
        # Input validation
        if not spark:
            raise ValueError("Invalid SparkSession provided")
        
        if existing_df is None:
            raise ValueError("Invalid existing_df DataFrame provided")
        
        if new_df is None:
            raise ValueError("Invalid new_df DataFrame provided")
        
        logger.info("Input validation completed - proceeding with view registration")
        
        # Register existing customer data as temporary view with error handling
        try:
            logger.info("Registering 'existing_customers' temporary view...")
            existing_df.createOrReplaceTempView("existing_customers")
            logger.info("Successfully registered 'existing_customers' temporary view")
        except Exception as existing_error:
            logger.error(f"Failed to register 'existing_customers' view: {str(existing_error)}")
            logger.error(f"Error type: {type(existing_error).__name__}")
            raise RuntimeError(f"Failed to register existing_customers view: {str(existing_error)}") from existing_error
        
        # Register new customer data as temporary view with error handling
        try:
            logger.info("Registering 'new_customers' temporary view...")
            new_df.createOrReplaceTempView("new_customers")
            logger.info("Successfully registered 'new_customers' temporary view")
        except Exception as new_error:
            logger.error(f"Failed to register 'new_customers' view: {str(new_error)}")
            logger.error(f"Error type: {type(new_error).__name__}")
            raise RuntimeError(f"Failed to register new_customers view: {str(new_error)}") from new_error
        
        # Validate views were created successfully with comprehensive error handling
        logger.info("Validating registered temporary views...")
        
        try:
            # Validate existing_customers view
            logger.debug("Validating 'existing_customers' view...")
            existing_view_count = spark.sql("SELECT COUNT(*) as count FROM existing_customers").collect()[0].count
            logger.debug(f"'existing_customers' view validation successful: {existing_view_count} records")
        except Exception as existing_validation_error:
            logger.error(f"Failed to validate 'existing_customers' view: {str(existing_validation_error)}")
            logger.error("This indicates the view was not registered properly")
            raise RuntimeError(f"existing_customers view validation failed: {str(existing_validation_error)}") from existing_validation_error
        
        try:
            # Validate new_customers view
            logger.debug("Validating 'new_customers' view...")
            new_view_count = spark.sql("SELECT COUNT(*) as count FROM new_customers").collect()[0].count
            logger.debug(f"'new_customers' view validation successful: {new_view_count} records")
        except Exception as new_validation_error:
            logger.error(f"Failed to validate 'new_customers' view: {str(new_validation_error)}")
            logger.error("This indicates the view was not registered properly")
            raise RuntimeError(f"new_customers view validation failed: {str(new_validation_error)}") from new_validation_error
        
        # Cross-validation between DataFrames and views
        try:
            existing_df_count = existing_df.count()
            new_df_count = new_df.count()
            
            if existing_view_count != existing_df_count:
                logger.warning(f"Record count mismatch for existing_customers: DataFrame={existing_df_count}, View={existing_view_count}")
            
            if new_view_count != new_df_count:
                logger.warning(f"Record count mismatch for new_customers: DataFrame={new_df_count}, View={new_view_count}")
            
        except Exception as cross_validation_error:
            logger.warning(f"Failed to perform cross-validation: {str(cross_validation_error)}")
            logger.warning("Continuing without cross-validation")
        
        # Log view registration summary
        registration_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Temporary views registered successfully in {registration_time:.2f} seconds")
        logger.info(f"View registration summary:")
        logger.info(f"  existing_customers view: {existing_view_count} records")
        logger.info(f"  new_customers view: {new_view_count} records")
        
        # Log available views for debugging with error handling
        try:
            logger.debug("Available temporary views after registration:")
            temp_views = [view.name for view in spark.catalog.listTables() if view.isTemporary]
            for view_name in temp_views:
                logger.debug(f"  - {view_name}")
            
            if len(temp_views) < 2:
                logger.warning(f"Expected at least 2 temporary views, found {len(temp_views)}")
            
        except Exception as debug_error:
            logger.warning(f"Failed to list temporary views for debugging: {str(debug_error)}")
            logger.warning("Continuing without debug view listing")
        
        # Additional validation - test basic SQL operations on views
        try:
            logger.debug("Testing basic SQL operations on registered views...")
            
            # Test simple SELECT on existing_customers
            test_existing = spark.sql("SELECT customer_id FROM existing_customers LIMIT 1").collect()
            logger.debug(f"existing_customers view test successful: {len(test_existing)} record(s) retrieved")
            
            # Test simple SELECT on new_customers
            test_new = spark.sql("SELECT customer_id FROM new_customers LIMIT 1").collect()
            logger.debug(f"new_customers view test successful: {len(test_new)} record(s) retrieved")
            
        except Exception as sql_test_error:
            logger.error(f"SQL operation test failed on registered views: {str(sql_test_error)}")
            logger.error("This indicates serious issues with view registration")
            raise RuntimeError(f"View SQL operation test failed: {str(sql_test_error)}") from sql_test_error
        
        logger.info("Temporary view registration and validation completed successfully")
        
    except Exception as e:
        registration_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"CRITICAL ERROR: Failed to register temporary views after {registration_time:.2f} seconds")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Try to clean up any partially registered views
        try:
            logger.error("Attempting to clean up any partially registered views...")
            spark.catalog.dropTempView("existing_customers")
            spark.catalog.dropTempView("new_customers")
            logger.error("Cleanup completed")
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup views: {str(cleanup_error)}")
        
        raise RuntimeError(f"Critical failure in temporary view registration: {str(e)}") from e


def get_unchanged_records_query() -> str:
    """
    Define SQL query to identify unchanged records using INNER JOIN with attribute comparison.
    
    This query finds records where all customer attributes remain the same between
    existing and new data by performing an INNER JOIN and comparing all relevant fields.
    
    Returns:
        SQL query string for identifying unchanged records
    """
    return """
        SELECT 
            curr.customer_key,
            curr.customer_id,
            curr.customer_name,
            curr.address,
            curr.phone,
            curr.email,
            curr.effective_start_date,
            curr.effective_end_date,
            curr.is_current
        FROM existing_customers curr
        INNER JOIN new_customers new ON curr.customer_id = new.customer_id
        WHERE curr.is_current = true
          AND curr.customer_name = new.customer_name
          AND curr.address = new.address
          AND curr.phone = new.phone
          AND curr.email = new.email
        ORDER BY curr.customer_id
    """


def get_changed_records_query() -> str:
    """
    Define SQL query to identify changed records using INNER JOIN with attribute differences.
    
    This query finds records where at least one customer attribute has changed between
    existing and new data by performing an INNER JOIN and checking for differences
    in any of the tracked attributes.
    
    Returns:
        SQL query string for identifying changed records
    """
    return """
        SELECT 
            curr.customer_key as existing_customer_key,
            curr.customer_id,
            curr.customer_name as existing_customer_name,
            curr.address as existing_address,
            curr.phone as existing_phone,
            curr.email as existing_email,
            curr.effective_start_date as existing_effective_start_date,
            curr.effective_end_date as existing_effective_end_date,
            curr.is_current as existing_is_current,
            new.customer_name as new_customer_name,
            new.address as new_address,
            new.phone as new_phone,
            new.email as new_email,
            new.source_date
        FROM existing_customers curr
        INNER JOIN new_customers new ON curr.customer_id = new.customer_id
        WHERE curr.is_current = true
          AND (curr.customer_name != new.customer_name
               OR curr.address != new.address
               OR curr.phone != new.phone
               OR curr.email != new.email)
        ORDER BY curr.customer_id
    """


def get_new_records_query() -> str:
    """
    Define SQL query to identify new records using LEFT JOIN with NULL check.
    
    This query finds completely new customer records that don't exist in the
    existing customer dimension by performing a LEFT JOIN and checking for
    NULL values in the existing customer data.
    
    Returns:
        SQL query string for identifying new customer records
    """
    return """
        SELECT 
            new.customer_id,
            new.customer_name,
            new.address,
            new.phone,
            new.email,
            new.source_date
        FROM new_customers new
        LEFT JOIN existing_customers curr ON new.customer_id = curr.customer_id
        WHERE curr.customer_id IS NULL
        ORDER BY new.customer_id
    """


def validate_and_execute_classification_query(spark: SparkSession, query: str, query_name: str, debug: bool = False) -> DataFrame:
    """
    Validate and execute a record classification SQL query with comprehensive logging and error handling.
    
    Args:
        spark: SparkSession instance
        query: SQL query string to execute
        query_name: Descriptive name for the query (for logging)
        debug: Enable debug mode to show sample results
        
    Returns:
        DataFrame containing query results
        
    Raises:
        Exception: If query validation or execution fails
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting execution of {query_name} classification query")
    
    # Initialize timing for performance monitoring
    start_time = datetime.now()
    
    try:
        # Pre-execution validation
        if not query or not query.strip():
            raise ValueError(f"Empty or invalid SQL query provided for {query_name}")
        
        if not spark:
            raise ValueError(f"Invalid SparkSession provided for {query_name}")
        
        # Log the query in debug mode with line numbers for easier debugging
        if debug:
            logger.debug(f"=== {query_name} SQL Query ===")
            for i, line in enumerate(query.strip().split('\n'), 1):
                logger.debug(f"  {i:2d}: {line}")
            logger.debug(f"=== End of {query_name} Query ===")
        
        # Log query execution start
        logger.info(f"Executing SQL query for {query_name}...")
        
        # Execute the query with comprehensive error handling
        try:
            result_df = spark.sql(query)
            logger.debug(f"SQL query compilation successful for {query_name}")
        except Exception as sql_error:
            logger.error(f"SQL compilation/execution failed for {query_name}")
            logger.error(f"SQL Error Type: {type(sql_error).__name__}")
            logger.error(f"SQL Error Message: {str(sql_error)}")
            logger.error(f"Failed Query Context:")
            for i, line in enumerate(query.strip().split('\n'), 1):
                logger.error(f"  {i:2d}: {line}")
            raise RuntimeError(f"SQL execution failed for {query_name}: {str(sql_error)}") from sql_error
        
        # Get record count with error handling
        try:
            record_count = result_df.count()
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"{query_name} query executed successfully - Found {record_count} records in {execution_time:.2f} seconds")
        except Exception as count_error:
            logger.error(f"Failed to count records for {query_name}: {str(count_error)}")
            logger.error(f"This may indicate a data access or computation error")
            raise RuntimeError(f"Record count failed for {query_name}: {str(count_error)}") from count_error
        
        # Show sample results in debug mode with error handling
        if debug and record_count > 0:
            try:
                logger.debug(f"=== Sample {query_name} Results (first 5 records) ===")
                sample_results = result_df.limit(5).collect()
                for i, row in enumerate(sample_results, 1):
                    row_dict = row.asDict()
                    logger.debug(f"  Record {i}: {row_dict}")
                logger.debug(f"=== End of Sample {query_name} Results ===")
            except Exception as sample_error:
                logger.warning(f"Failed to collect sample results for {query_name}: {str(sample_error)}")
                logger.warning("Continuing execution despite sample collection failure")
        elif debug and record_count == 0:
            logger.debug(f"No records found for {query_name} - empty result set")
        
        # Validate query execution results
        if record_count < 0:
            raise ValueError(f"Invalid negative record count returned for {query_name}: {record_count}")
        
        # Performance logging
        if execution_time > 30:  # Log slow queries (>30 seconds)
            logger.warning(f"Slow query detected for {query_name}: {execution_time:.2f} seconds")
        
        # Cache the result for potential reuse with error handling
        try:
            result_df.cache()
            logger.debug(f"Successfully cached results for {query_name}")
        except Exception as cache_error:
            logger.warning(f"Failed to cache results for {query_name}: {str(cache_error)}")
            logger.warning("Continuing execution without caching")
        
        logger.info(f"Successfully completed {query_name} query execution")
        return result_df
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"CRITICAL ERROR: Failed to execute {query_name} query after {execution_time:.2f} seconds")
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.error(f"Query Context for {query_name}:")
        for i, line in enumerate(query.strip().split('\n'), 1):
            logger.error(f"  {i:2d}: {line}")
        
        # Log additional context for debugging
        try:
            logger.error(f"Spark Session Status: {spark.sparkContext.applicationId if spark and spark.sparkContext else 'Unknown'}")
            logger.error(f"Available Temporary Views: {[view.name for view in spark.catalog.listTables() if view.isTemporary] if spark else 'Unknown'}")
        except Exception as context_error:
            logger.error(f"Failed to gather additional context: {str(context_error)}")
        
        raise RuntimeError(f"Critical failure in {query_name} query execution: {str(e)}") from e


def process_unchanged_records(spark: SparkSession, debug: bool = False) -> DataFrame:
    """
    Execute unchanged records SQL query and return results.
    
    This function processes records that have no changes between existing and new data.
    These records are preserved exactly as-is in the final output.
    
    Args:
        spark: SparkSession instance
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        DataFrame containing unchanged customer records
        
    Raises:
        Exception: If query execution fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info("Processing unchanged customer records")
    
    try:
        # Get and execute unchanged records query
        unchanged_query = get_unchanged_records_query()
        unchanged_df = validate_and_execute_classification_query(
            spark, unchanged_query, "Unchanged Records Processing", debug
        )
        
        # Validate record structure
        expected_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address', 
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        actual_columns = unchanged_df.columns
        
        if set(expected_columns) != set(actual_columns):
            missing_cols = set(expected_columns) - set(actual_columns)
            extra_cols = set(actual_columns) - set(expected_columns)
            error_msg = f"Schema validation failed for unchanged records. Missing: {missing_cols}, Extra: {extra_cols}"
            raise ValueError(error_msg)
        
        # Record count validation and logging
        record_count = unchanged_df.count()
        logger.info(f"Unchanged records processing completed - {record_count} records preserved")
        
        # Validate that all records are current
        if record_count > 0:
            non_current_count = unchanged_df.filter(unchanged_df.is_current == False).count()
            if non_current_count > 0:
                logger.warning(f"Found {non_current_count} non-current records in unchanged data")
        
        return unchanged_df
        
    except Exception as e:
        logger.error(f"Failed to process unchanged records: {str(e)}")
        raise


def process_changed_records(spark: SparkSession, debug: bool = False) -> Tuple[DataFrame, DataFrame]:
    """
    Execute changed records SQL query and return both current and new data.
    
    This function processes records that have attribute changes between existing and new data.
    It returns both the existing records (to be closed) and the new attribute data (for new records).
    
    Args:
        spark: SparkSession instance
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        Tuple containing (existing_records_df, new_data_df) for changed customers
        
    Raises:
        Exception: If query execution fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info("Processing changed customer records")
    
    try:
        # Get and execute changed records query
        changed_query = get_changed_records_query()
        changed_df = validate_and_execute_classification_query(
            spark, changed_query, "Changed Records Processing", debug
        )
        
        record_count = changed_df.count()
        logger.info(f"Changed records query executed - {record_count} changed customers identified")
        
        if record_count == 0:
            logger.info("No changed records found - returning empty DataFrames")
            # Return empty DataFrames with correct schemas
            empty_existing = spark.createDataFrame([], get_existing_customer_schema())
            empty_new = spark.createDataFrame([], get_new_customer_schema())
            return empty_existing, empty_new
        
        # Create temporary view for changed records to enable further SQL processing
        changed_df.createOrReplaceTempView("changed_records_raw")
        logger.debug("Created temporary view 'changed_records_raw' for changed records")
        
        # Extract existing records data (to be closed)
        existing_records_query = """
            SELECT 
                existing_customer_key as customer_key,
                customer_id,
                existing_customer_name as customer_name,
                existing_address as address,
                existing_phone as phone,
                existing_email as email,
                existing_effective_start_date as effective_start_date,
                existing_effective_end_date as effective_end_date,
                existing_is_current as is_current
            FROM changed_records_raw
            ORDER BY customer_id
        """
        
        existing_records_df = validate_and_execute_classification_query(
            spark, existing_records_query, "Changed Records - Existing Data", debug
        )
        
        # Extract new data for changed records
        new_data_query = """
            SELECT 
                customer_id,
                new_customer_name as customer_name,
                new_address as address,
                new_phone as phone,
                new_email as email,
                source_date
            FROM changed_records_raw
            ORDER BY customer_id
        """
        
        new_data_df = validate_and_execute_classification_query(
            spark, new_data_query, "Changed Records - New Data", debug
        )
        
        # Validation checks
        existing_count = existing_records_df.count()
        new_data_count = new_data_df.count()
        
        if existing_count != new_data_count:
            raise ValueError(f"Mismatch in changed records: {existing_count} existing vs {new_data_count} new data records")
        
        if existing_count != record_count:
            raise ValueError(f"Record count mismatch: expected {record_count}, got {existing_count}")
        
        # Validate that all existing records are current
        non_current_existing = existing_records_df.filter(existing_records_df.is_current == False).count()
        if non_current_existing > 0:
            logger.warning(f"Found {non_current_existing} non-current records in changed existing data")
        
        logger.info(f"Changed records processing completed - {existing_count} records to be updated")
        
        return existing_records_df, new_data_df
        
    except Exception as e:
        logger.error(f"Failed to process changed records: {str(e)}")
        raise


def process_new_records(spark: SparkSession, debug: bool = False) -> DataFrame:
    """
    Execute new records SQL query and return new customer data.
    
    This function processes completely new customer records that don't exist in the
    existing customer dimension.
    
    Args:
        spark: SparkSession instance
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        DataFrame containing new customer records
        
    Raises:
        Exception: If query execution fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info("Processing new customer records")
    
    try:
        # Get and execute new records query
        new_query = get_new_records_query()
        new_df = validate_and_execute_classification_query(
            spark, new_query, "New Records Processing", debug
        )
        
        # Validate record structure
        expected_columns = ['customer_id', 'customer_name', 'address', 'phone', 'email', 'source_date']
        actual_columns = new_df.columns
        
        if set(expected_columns) != set(actual_columns):
            missing_cols = set(expected_columns) - set(actual_columns)
            extra_cols = set(actual_columns) - set(expected_columns)
            error_msg = f"Schema validation failed for new records. Missing: {missing_cols}, Extra: {extra_cols}"
            raise ValueError(error_msg)
        
        # Record count validation and logging
        record_count = new_df.count()
        logger.info(f"New records processing completed - {record_count} new customers identified")
        
        # Validate no duplicate customer_ids in new records
        if record_count > 0:
            distinct_customers = new_df.select("customer_id").distinct().count()
            if distinct_customers != record_count:
                duplicate_count = record_count - distinct_customers
                logger.warning(f"Found {duplicate_count} duplicate customer_ids in new records")
        
        # Log customer_id range for new records
        if record_count > 0 and debug:
            customer_id_stats = new_df.agg(
                {"customer_id": "min", "customer_id": "max"}
            ).collect()[0]
            logger.debug(f"New customer ID range: {customer_id_stats}")
        
        return new_df
        
    except Exception as e:
        logger.error(f"Failed to process new records: {str(e)}")
        raise


def classify_customer_records(spark: SparkSession, debug: bool = False) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Classify customer records into unchanged, changed, and new categories using SQL queries.
    
    This function executes the three classification queries to categorize all customer
    records based on their status between existing and new data.
    
    Args:
        spark: SparkSession instance
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        Tuple containing (unchanged_df, changed_df, new_df) DataFrames
        
    Raises:
        Exception: If any classification query fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting customer record classification using SQL queries")
    
    try:
        # Process each record type using dedicated functions
        unchanged_df = process_unchanged_records(spark, debug)
        changed_existing_df, changed_new_df = process_changed_records(spark, debug)
        new_df = process_new_records(spark, debug)
        
        # Log classification summary
        unchanged_count = unchanged_df.count()
        changed_count = changed_existing_df.count()
        new_count = new_df.count()
        total_classified = unchanged_count + changed_count + new_count
        
        logger.info("Record classification completed successfully")
        logger.info(f"Classification Summary:")
        logger.info(f"  - Unchanged records: {unchanged_count}")
        logger.info(f"  - Changed records: {changed_count}")
        logger.info(f"  - New records: {new_count}")
        logger.info(f"  - Total classified: {total_classified}")
        
        # Validate classification completeness
        total_new_records = spark.sql("SELECT COUNT(*) as count FROM new_customers").collect()[0].count
        if total_classified != total_new_records:
            logger.warning(f"Classification mismatch: {total_classified} classified vs {total_new_records} total new records")
        
        # For backward compatibility, return changed_existing_df as the changed_df
        # The caller can use process_changed_records directly if they need both DataFrames
        return unchanged_df, changed_existing_df, new_df
        
    except Exception as e:
        logger.error(f"Failed to classify customer records: {str(e)}")
        raise


def calculate_next_surrogate_key(spark: SparkSession) -> int:
    """
    Calculate the next available surrogate key using MAX() aggregate function.
    
    This function determines the highest existing customer_key value and returns
    the next sequential key to be used for new records.
    
    Args:
        spark: SparkSession instance
        
    Returns:
        Next available surrogate key as integer
        
    Raises:
        Exception: If surrogate key calculation fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Calculating next available surrogate key")
    
    try:
        # SQL query to find maximum customer_key using MAX() aggregate function
        max_key_query = """
            SELECT COALESCE(MAX(customer_key), 0) as max_key
            FROM existing_customers
        """
        
        logger.debug("Executing MAX() aggregate query for surrogate key calculation")
        result = spark.sql(max_key_query).collect()[0]
        max_key = result.max_key
        
        # Calculate next available key
        next_key = max_key + 1
        
        logger.info(f"Surrogate key calculation completed - Current max: {max_key}, Next key: {next_key}")
        
        # Validate the calculated key
        if next_key <= 0:
            raise ValueError(f"Invalid next surrogate key calculated: {next_key}")
        
        return next_key
        
    except Exception as e:
        logger.error(f"Failed to calculate next surrogate key: {str(e)}")
        raise


def get_close_historical_records_query(source_date: str) -> str:
    """
    Create SQL query to close historical records by updating effective_end_date and is_current flag.
    
    This query generates records for changed customers where the existing current records
    need to be closed by setting the effective_end_date to the source_date and is_current to false.
    
    Args:
        source_date: Date string in YYYY-MM-DD format to use as the effective_end_date
        
    Returns:
        SQL query string for closing historical records
    """
    return f"""
        SELECT 
            customer_key,
            customer_id,
            customer_name,
            address,
            phone,
            email,
            effective_start_date,
            CAST('{source_date}' AS DATE) as effective_end_date,
            false as is_current
        FROM existing_customers
        WHERE customer_id IN (
            SELECT DISTINCT curr.customer_id
            FROM existing_customers curr
            INNER JOIN new_customers new ON curr.customer_id = new.customer_id
            WHERE curr.is_current = true
              AND (curr.customer_name != new.customer_name
                   OR curr.address != new.address
                   OR curr.phone != new.phone
                   OR curr.email != new.email)
        )
        AND is_current = true
        ORDER BY customer_id
    """


def get_new_current_records_for_changed_query(next_key: int, source_date: str) -> str:
    """
    Create SQL query to generate new current records for changed customers using ROW_NUMBER() window function.
    
    This query creates new current records for customers whose attributes have changed,
    using ROW_NUMBER() window function to assign sequential surrogate keys starting from next_key.
    
    Args:
        next_key: Starting surrogate key value for new records
        source_date: Date string in YYYY-MM-DD format to use as the effective_start_date
        
    Returns:
        SQL query string for creating new current records for changed customers
    """
    return f"""
        SELECT 
            {next_key} + ROW_NUMBER() OVER (ORDER BY customer_id) - 1 as customer_key,
            customer_id,
            customer_name,
            address,
            phone,
            email,
            CAST('{source_date}' AS DATE) as effective_start_date,
            CAST('9999-12-31' AS DATE) as effective_end_date,
            true as is_current
        FROM (
            SELECT DISTINCT
                new.customer_id,
                new.customer_name,
                new.address,
                new.phone,
                new.email
            FROM existing_customers curr
            INNER JOIN new_customers new ON curr.customer_id = new.customer_id
            WHERE curr.is_current = true
              AND (curr.customer_name != new.customer_name
                   OR curr.address != new.address
                   OR curr.phone != new.phone
                   OR curr.email != new.email)
        ) changed_customer_data
        ORDER BY customer_id
    """


def get_new_customer_records_query(next_key: int, source_date: str) -> str:
    """
    Create SQL query to process completely new customers with surrogate key generation.
    
    This query creates records for completely new customers that don't exist in the
    existing customer dimension, using ROW_NUMBER() window function for surrogate key assignment.
    
    Args:
        next_key: Starting surrogate key value for new customer records
        source_date: Date string in YYYY-MM-DD format to use as the effective_start_date
        
    Returns:
        SQL query string for processing new customer records
    """
    return f"""
        SELECT 
            {next_key} + ROW_NUMBER() OVER (ORDER BY customer_id) - 1 as customer_key,
            customer_id,
            customer_name,
            address,
            phone,
            email,
            CAST('{source_date}' AS DATE) as effective_start_date,
            CAST('9999-12-31' AS DATE) as effective_end_date,
            true as is_current
        FROM (
            SELECT 
                new.customer_id,
                new.customer_name,
                new.address,
                new.phone,
                new.email
            FROM new_customers new
            LEFT JOIN existing_customers curr ON new.customer_id = curr.customer_id
            WHERE curr.customer_id IS NULL
        ) new_customer_data
        ORDER BY customer_id
    """


def process_scd_close_historical_records(spark: SparkSession, source_date: str, debug: bool = False) -> DataFrame:
    """
    Execute SQL query to close historical records for changed customers.
    
    This function processes existing current records for customers whose attributes have changed,
    creating closed historical records with updated effective_end_date and is_current flag.
    
    Args:
        spark: SparkSession instance
        source_date: Date string in YYYY-MM-DD format for effective_end_date
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        DataFrame containing closed historical records
        
    Raises:
        Exception: If query execution fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing SCD close historical records with source_date: {source_date}")
    
    try:
        # Get and execute close historical records query
        close_query = get_close_historical_records_query(source_date)
        closed_df = validate_and_execute_classification_query(
            spark, close_query, "SCD Close Historical Records", debug
        )
        
        # Validate record structure
        expected_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address', 
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        actual_columns = closed_df.columns
        
        if set(expected_columns) != set(actual_columns):
            missing_cols = set(expected_columns) - set(actual_columns)
            extra_cols = set(actual_columns) - set(expected_columns)
            error_msg = f"Schema validation failed for closed historical records. Missing: {missing_cols}, Extra: {extra_cols}"
            raise ValueError(error_msg)
        
        # Record count validation and logging
        record_count = closed_df.count()
        logger.info(f"SCD close historical records completed - {record_count} records closed")
        
        # Validate that all records are now historical (is_current = false)
        if record_count > 0:
            current_count = closed_df.filter(closed_df.is_current == True).count()
            if current_count > 0:
                raise ValueError(f"Found {current_count} records still marked as current in closed historical records")
            
            # Validate effective_end_date is set correctly
            incorrect_end_date = closed_df.filter(closed_df.effective_end_date != source_date).count()
            if incorrect_end_date > 0:
                logger.warning(f"Found {incorrect_end_date} records with incorrect effective_end_date")
        
        return closed_df
        
    except Exception as e:
        logger.error(f"Failed to process SCD close historical records: {str(e)}")
        raise


def process_scd_new_current_for_changed(spark: SparkSession, next_key: int, source_date: str, debug: bool = False) -> DataFrame:
    """
    Execute SQL query to create new current records for changed customers.
    
    This function processes changed customers by creating new current records with updated
    attributes and sequential surrogate keys using ROW_NUMBER() window function.
    
    Args:
        spark: SparkSession instance
        next_key: Starting surrogate key value for new records
        source_date: Date string in YYYY-MM-DD format for effective_start_date
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        DataFrame containing new current records for changed customers
        
    Raises:
        Exception: If query execution fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing SCD new current records for changed customers - Starting key: {next_key}, Source date: {source_date}")
    
    try:
        # Get and execute new current records query for changed customers
        new_current_query = get_new_current_records_for_changed_query(next_key, source_date)
        new_current_df = validate_and_execute_classification_query(
            spark, new_current_query, "SCD New Current Records for Changed", debug
        )
        
        # Validate record structure
        expected_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address', 
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        actual_columns = new_current_df.columns
        
        if set(expected_columns) != set(actual_columns):
            missing_cols = set(expected_columns) - set(actual_columns)
            extra_cols = set(actual_columns) - set(expected_columns)
            error_msg = f"Schema validation failed for new current records. Missing: {missing_cols}, Extra: {extra_cols}"
            raise ValueError(error_msg)
        
        # Record count validation and logging
        record_count = new_current_df.count()
        logger.info(f"SCD new current records for changed customers completed - {record_count} records created")
        
        # Validate record attributes
        if record_count > 0:
            # Validate that all records are current (is_current = true)
            non_current_count = new_current_df.filter(new_current_df.is_current == False).count()
            if non_current_count > 0:
                raise ValueError(f"Found {non_current_count} records not marked as current in new current records")
            
            # Validate effective_start_date is set correctly
            incorrect_start_date = new_current_df.filter(new_current_df.effective_start_date != source_date).count()
            if incorrect_start_date > 0:
                logger.warning(f"Found {incorrect_start_date} records with incorrect effective_start_date")
            
            # Validate effective_end_date is set to future date
            incorrect_end_date = new_current_df.filter(new_current_df.effective_end_date != '9999-12-31').count()
            if incorrect_end_date > 0:
                logger.warning(f"Found {incorrect_end_date} records with incorrect effective_end_date")
            
            # Validate surrogate key range
            key_stats = new_current_df.agg({"customer_key": "min", "customer_key": "max"}).collect()[0]
            min_key = key_stats["min(customer_key)"]
            max_key = key_stats["max(customer_key)"]
            expected_max_key = next_key + record_count - 1
            
            if min_key != next_key:
                logger.warning(f"Unexpected minimum customer_key: expected {next_key}, got {min_key}")
            if max_key != expected_max_key:
                logger.warning(f"Unexpected maximum customer_key: expected {expected_max_key}, got {max_key}")
            
            logger.info(f"Surrogate key range for new current records: {min_key} to {max_key}")
        
        return new_current_df
        
    except Exception as e:
        logger.error(f"Failed to process SCD new current records for changed customers: {str(e)}")
        raise


def process_scd_new_customer_records(spark: SparkSession, next_key: int, source_date: str, debug: bool = False) -> DataFrame:
    """
    Execute SQL query to create records for completely new customers.
    
    This function processes completely new customers by creating records with sequential
    surrogate keys using ROW_NUMBER() window function.
    
    Args:
        spark: SparkSession instance
        next_key: Starting surrogate key value for new customer records
        source_date: Date string in YYYY-MM-DD format for effective_start_date
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        DataFrame containing records for new customers
        
    Raises:
        Exception: If query execution fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing SCD new customer records - Starting key: {next_key}, Source date: {source_date}")
    
    try:
        # Get and execute new customer records query
        new_customer_query = get_new_customer_records_query(next_key, source_date)
        new_customer_df = validate_and_execute_classification_query(
            spark, new_customer_query, "SCD New Customer Records", debug
        )
        
        # Validate record structure
        expected_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address', 
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        actual_columns = new_customer_df.columns
        
        if set(expected_columns) != set(actual_columns):
            missing_cols = set(expected_columns) - set(actual_columns)
            extra_cols = set(actual_columns) - set(expected_columns)
            error_msg = f"Schema validation failed for new customer records. Missing: {missing_cols}, Extra: {extra_cols}"
            raise ValueError(error_msg)
        
        # Record count validation and logging
        record_count = new_customer_df.count()
        logger.info(f"SCD new customer records completed - {record_count} records created")
        
        # Validate record attributes
        if record_count > 0:
            # Validate that all records are current (is_current = true)
            non_current_count = new_customer_df.filter(new_customer_df.is_current == False).count()
            if non_current_count > 0:
                raise ValueError(f"Found {non_current_count} records not marked as current in new customer records")
            
            # Validate effective_start_date is set correctly
            incorrect_start_date = new_customer_df.filter(new_customer_df.effective_start_date != source_date).count()
            if incorrect_start_date > 0:
                logger.warning(f"Found {incorrect_start_date} records with incorrect effective_start_date")
            
            # Validate effective_end_date is set to future date
            incorrect_end_date = new_customer_df.filter(new_customer_df.effective_end_date != '9999-12-31').count()
            if incorrect_end_date > 0:
                logger.warning(f"Found {incorrect_end_date} records with incorrect effective_end_date")
            
            # Validate surrogate key range
            key_stats = new_customer_df.agg({"customer_key": "min", "customer_key": "max"}).collect()[0]
            min_key = key_stats["min(customer_key)"]
            max_key = key_stats["max(customer_key)"]
            expected_max_key = next_key + record_count - 1
            
            if min_key != next_key:
                logger.warning(f"Unexpected minimum customer_key: expected {next_key}, got {min_key}")
            if max_key != expected_max_key:
                logger.warning(f"Unexpected maximum customer_key: expected {expected_max_key}, got {max_key}")
            
            logger.info(f"Surrogate key range for new customer records: {min_key} to {max_key}")
        
        return new_customer_df
        
    except Exception as e:
        logger.error(f"Failed to process SCD new customer records: {str(e)}")
        raise


def execute_scd_type2_operations(spark: SparkSession, debug: bool = False) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Execute all SCD Type II operations using SQL queries.
    
    This function orchestrates the complete SCD Type II processing by executing SQL queries
    to close historical records, create new current records for changed customers, and
    process completely new customers with proper surrogate key generation.
    
    Args:
        spark: SparkSession instance
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        Tuple containing (unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df)
        
    Raises:
        Exception: If any SCD operation fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting SCD Type II operations using SQL queries")
    
    try:
        # Step 1: Classify records to understand what needs processing
        unchanged_df, changed_existing_df, new_records_df = classify_customer_records(spark, debug)
        
        # Get source date from new customer data
        source_date_result = spark.sql("SELECT DISTINCT source_date FROM new_customers ORDER BY source_date").collect()
        if not source_date_result:
            raise ValueError("No source_date found in new customer data")
        
        source_date = str(source_date_result[0].source_date)
        logger.info(f"Using source_date for SCD operations: {source_date}")
        
        # Step 2: Calculate next available surrogate key
        next_key = calculate_next_surrogate_key(spark)
        
        # Step 3: Process unchanged records (no SCD operations needed)
        logger.info("Processing unchanged records (no SCD operations required)")
        unchanged_count = unchanged_df.count()
        logger.info(f"Unchanged records: {unchanged_count} (preserved as-is)")
        
        # Step 4: Process changed customers - close historical records
        closed_historical_df = process_scd_close_historical_records(spark, source_date, debug)
        closed_count = closed_historical_df.count()
        
        # Step 5: Process changed customers - create new current records
        if closed_count > 0:
            new_current_changed_df = process_scd_new_current_for_changed(spark, next_key, source_date, debug)
            new_current_changed_count = new_current_changed_df.count()
            
            # Update next_key for new customer processing
            next_key += new_current_changed_count
        else:
            # Create empty DataFrame with correct schema if no changed records
            new_current_changed_df = spark.createDataFrame([], get_existing_customer_schema())
            new_current_changed_count = 0
        
        # Step 6: Process completely new customers
        new_customer_df = process_scd_new_customer_records(spark, next_key, source_date, debug)
        new_customer_count = new_customer_df.count()
        
        # Log SCD operations summary
        logger.info("SCD Type II operations completed successfully")
        logger.info(f"SCD Operations Summary:")
        logger.info(f"  - Unchanged records: {unchanged_count}")
        logger.info(f"  - Closed historical records: {closed_count}")
        logger.info(f"  - New current records (changed): {new_current_changed_count}")
        logger.info(f"  - New customer records: {new_customer_count}")
        logger.info(f"  - Total processed records: {unchanged_count + closed_count + new_current_changed_count + new_customer_count}")
        
        # Validate surrogate key usage
        total_new_keys_used = new_current_changed_count + new_customer_count
        if total_new_keys_used > 0:
            final_max_key = next_key + new_customer_count - 1
            logger.info(f"Surrogate key usage: {total_new_keys_used} new keys assigned (up to key {final_max_key})")
        
        return unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df
        
    except Exception as e:
        logger.error(f"Failed to execute SCD Type II operations: {str(e)}")
        raise


def get_final_union_query() -> str:
    """
    Create SQL UNION query to combine all processed record types with proper column ordering.
    
    This query combines unchanged records, closed historical records, new current records
    for changed customers, and new customer records into a single result set using UNION ALL
    operations for optimal performance.
    
    Returns:
        SQL query string for combining all processed record types
    """
    return """
        SELECT 
            customer_key,
            customer_id,
            customer_name,
            address,
            phone,
            email,
            effective_start_date,
            effective_end_date,
            is_current
        FROM unchanged_records
        
        UNION ALL
        
        SELECT 
            customer_key,
            customer_id,
            customer_name,
            address,
            phone,
            email,
            effective_start_date,
            effective_end_date,
            is_current
        FROM closed_historical_records
        
        UNION ALL
        
        SELECT 
            customer_key,
            customer_id,
            customer_name,
            address,
            phone,
            email,
            effective_start_date,
            effective_end_date,
            is_current
        FROM new_current_changed_records
        
        UNION ALL
        
        SELECT 
            customer_key,
            customer_id,
            customer_name,
            address,
            phone,
            email,
            effective_start_date,
            effective_end_date,
            is_current
        FROM new_customer_records
        
        ORDER BY customer_key
    """


def execute_final_result_assembly(spark: SparkSession, unchanged_df: DataFrame, closed_historical_df: DataFrame, 
                                new_current_changed_df: DataFrame, new_customer_df: DataFrame, debug: bool = False) -> DataFrame:
    """
    Execute final result assembly query with proper column ordering using SQL UNION operations.
    
    This function creates temporary views for all processed record types and executes a SQL UNION
    query to combine them into the final customer dimension result with consistent column ordering.
    
    Args:
        spark: SparkSession instance
        unchanged_df: DataFrame containing unchanged customer records
        closed_historical_df: DataFrame containing closed historical records
        new_current_changed_df: DataFrame containing new current records for changed customers
        new_customer_df: DataFrame containing new customer records
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        DataFrame containing the final combined customer dimension result
        
    Raises:
        Exception: If result assembly fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info("Executing final result assembly using SQL UNION operations")
    
    try:
        # Create temporary views for all result components
        unchanged_df.createOrReplaceTempView("unchanged_records")
        closed_historical_df.createOrReplaceTempView("closed_historical_records")
        new_current_changed_df.createOrReplaceTempView("new_current_changed_records")
        new_customer_df.createOrReplaceTempView("new_customer_records")
        
        logger.info("Created temporary views for result assembly:")
        logger.info(f"  - unchanged_records: {unchanged_df.count()} records")
        logger.info(f"  - closed_historical_records: {closed_historical_df.count()} records")
        logger.info(f"  - new_current_changed_records: {new_current_changed_df.count()} records")
        logger.info(f"  - new_customer_records: {new_customer_df.count()} records")
        
        # Get and execute final union query
        union_query = get_final_union_query()
        final_result_df = validate_and_execute_classification_query(
            spark, union_query, "Final Result Assembly", debug
        )
        
        # Validate final result structure
        expected_columns = [
            'customer_key', 'customer_id', 'customer_name', 'address', 
            'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
        ]
        actual_columns = final_result_df.columns
        
        if set(expected_columns) != set(actual_columns):
            missing_cols = set(expected_columns) - set(actual_columns)
            extra_cols = set(actual_columns) - set(expected_columns)
            error_msg = f"Schema validation failed for final result. Missing: {missing_cols}, Extra: {extra_cols}"
            raise ValueError(error_msg)
        
        # Record count validation
        final_count = final_result_df.count()
        expected_count = (unchanged_df.count() + closed_historical_df.count() + 
                         new_current_changed_df.count() + new_customer_df.count())
        
        if final_count != expected_count:
            raise ValueError(f"Final result count mismatch: expected {expected_count}, got {final_count}")
        
        logger.info(f"Final result assembly completed successfully - {final_count} total records")
        
        # Cache the final result for output operations
        final_result_df.cache()
        
        return final_result_df
        
    except Exception as e:
        logger.error(f"Failed to execute final result assembly: {str(e)}")
        raise


def save_results_to_csv(spark: SparkSession, result_df: DataFrame, output_path: str, debug: bool = False) -> None:
    """
    Save final results to CSV with header and proper formatting with comprehensive error handling.
    
    This function writes the final customer dimension results to a CSV file with proper
    formatting, headers, and data type handling for dates and booleans.
    
    Args:
        spark: SparkSession instance
        result_df: DataFrame containing final customer dimension results
        output_path: Path where CSV output should be saved
        debug: Enable debug mode for detailed logging
        
    Raises:
        RuntimeError: If CSV output fails or validation errors occur
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting CSV output process to: {output_path}")
    
    start_time = datetime.now()
    
    try:
        # Input validation with comprehensive error handling
        if not spark:
            raise ValueError("Invalid SparkSession provided for CSV output")
        
        if result_df is None:
            raise ValueError("Invalid result DataFrame provided for CSV output")
        
        if not output_path or not output_path.strip():
            raise ValueError("Invalid or empty output path provided for CSV output")
        
        logger.info(f"Input validation completed - proceeding with CSV output to: {output_path}")
        
        # Validate input DataFrame with comprehensive error handling
        try:
            logger.info("Validating input DataFrame for CSV output...")
            record_count = result_df.count()
            column_count = len(result_df.columns)
            logger.info(f"DataFrame validation successful: {record_count} records, {column_count} columns")
        except Exception as df_error:
            logger.error(f"Failed to validate input DataFrame: {str(df_error)}")
            logger.error(f"Error type: {type(df_error).__name__}")
            raise RuntimeError(f"DataFrame validation failed for CSV output: {str(df_error)}") from df_error
        
        if record_count == 0:
            logger.warning("No records to save - creating empty CSV file with headers only")
        else:
            logger.info(f"Preparing to save {record_count} records to CSV")
        
        # Validate DataFrame schema for CSV compatibility
        try:
            logger.debug("Validating DataFrame schema for CSV compatibility...")
            expected_columns = [
                'customer_key', 'customer_id', 'customer_name', 'address', 
                'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current'
            ]
            actual_columns = result_df.columns
            
            if set(expected_columns) != set(actual_columns):
                missing_cols = set(expected_columns) - set(actual_columns)
                extra_cols = set(actual_columns) - set(expected_columns)
                logger.error(f"Schema validation failed for CSV output. Missing: {missing_cols}, Extra: {extra_cols}")
                raise ValueError(f"Invalid DataFrame schema for CSV output. Missing: {missing_cols}, Extra: {extra_cols}")
            
            logger.debug("DataFrame schema validation successful for CSV output")
        except Exception as schema_error:
            if "Invalid DataFrame schema" in str(schema_error):
                raise
            logger.error(f"Failed to validate DataFrame schema: {str(schema_error)}")
            raise RuntimeError(f"Schema validation failed for CSV output: {str(schema_error)}") from schema_error
        
        # Configure CSV output options with error handling
        try:
            csv_options = {
                "header": "true",
                "dateFormat": "yyyy-MM-dd",
                "timestampFormat": "yyyy-MM-dd HH:mm:ss",
                "nullValue": "",
                "emptyValue": "",
                "quote": '"',
                "escape": '"',
                "sep": ",",
                "encoding": "UTF-8"
            }
            
            logger.debug("CSV output options configured successfully")
            
            # Log CSV configuration in debug mode
            if debug:
                logger.debug("CSV output configuration details:")
                for key, value in csv_options.items():
                    logger.debug(f"  {key}: {value}")
        except Exception as config_error:
            logger.error(f"Failed to configure CSV output options: {str(config_error)}")
            raise RuntimeError(f"CSV configuration failed: {str(config_error)}") from config_error
        
        # Prepare DataFrame for CSV output with error handling
        try:
            logger.info("Preparing DataFrame for CSV output (coalescing to single partition)...")
            # Coalesce to single partition for single CSV file output
            coalesced_df = result_df.coalesce(1)
            logger.debug("DataFrame coalescing completed successfully")
        except Exception as coalesce_error:
            logger.error(f"Failed to coalesce DataFrame for CSV output: {str(coalesce_error)}")
            logger.error("This may indicate memory or performance issues")
            raise RuntimeError(f"DataFrame coalescing failed: {str(coalesce_error)}") from coalesce_error
        
        # Write DataFrame to CSV with comprehensive error handling
        try:
            logger.info("Writing DataFrame to CSV file...")
            write_start = datetime.now()
            
            coalesced_df.write \
                .mode("overwrite") \
                .options(**csv_options) \
                .csv(output_path)
            
            write_time = (datetime.now() - write_start).total_seconds()
            logger.info(f"CSV write operation completed successfully in {write_time:.2f} seconds")
            
        except Exception as write_error:
            logger.error(f"Failed to write DataFrame to CSV: {str(write_error)}")
            logger.error(f"Error type: {type(write_error).__name__}")
            logger.error(f"Output path: {output_path}")
            
            # Provide specific error context
            if "PermissionDenied" in str(write_error) or "Access is denied" in str(write_error):
                logger.error(f"Permission denied writing to path: {output_path}")
                logger.error("Please verify write permissions for the output directory")
            elif "No space left on device" in str(write_error):
                logger.error("Insufficient disk space for CSV output")
            elif "FileNotFoundException" in str(write_error):
                logger.error(f"Output directory does not exist: {output_path}")
                logger.error("Please ensure the output directory exists and is accessible")
            
            raise RuntimeError(f"CSV write operation failed: {str(write_error)}") from write_error
        
        logger.info(f"Successfully saved {record_count} records to CSV: {output_path}")
        
        # Validate output file was created with comprehensive error handling
        logger.info("Performing output file validation...")
        try:
            validation_start = datetime.now()
            
            # Try to read back a sample to validate the output
            logger.debug("Reading back CSV file for validation...")
            validation_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("dateFormat", "yyyy-MM-dd") \
                .csv(output_path)
            
            validation_count = validation_df.count()
            validation_columns = len(validation_df.columns)
            validation_time = (datetime.now() - validation_start).total_seconds()
            
            logger.debug(f"Output validation read completed in {validation_time:.2f} seconds")
            
            # Validate record count
            if validation_count != record_count:
                logger.error(f"Output validation FAILED: expected {record_count} records, found {validation_count}")
                raise ValueError(f"Output record count mismatch: expected {record_count}, found {validation_count}")
            
            # Validate column count
            if validation_columns != column_count:
                logger.error(f"Output validation FAILED: expected {column_count} columns, found {validation_columns}")
                raise ValueError(f"Output column count mismatch: expected {column_count}, found {validation_columns}")
            
            logger.info("Output validation successful:")
            logger.info(f"  ✓ Record count matches: {validation_count}")
            logger.info(f"  ✓ Column count matches: {validation_columns}")
            
            # Additional validation in debug mode
            if debug and validation_count > 0:
                try:
                    logger.debug("Performing additional output validation checks...")
                    
                    # Check for header presence
                    first_row = validation_df.limit(1).collect()[0]
                    logger.debug(f"First record validation: {first_row.asDict()}")
                    
                    # Validate column names
                    validation_column_names = validation_df.columns
                    if set(validation_column_names) != set(expected_columns):
                        logger.warning(f"Column name validation issue: expected {expected_columns}, got {validation_column_names}")
                    else:
                        logger.debug("Column names validation successful")
                        
                except Exception as debug_validation_error:
                    logger.warning(f"Additional validation checks failed: {str(debug_validation_error)}")
                    logger.warning("Continuing with basic validation only")
                
        except Exception as validation_error:
            if "Output record count mismatch" in str(validation_error) or "Output column count mismatch" in str(validation_error):
                raise
            logger.warning(f"Could not validate output file: {str(validation_error)}")
            logger.warning(f"Error type: {type(validation_error).__name__}")
            logger.warning("CSV file may have been created but validation failed")
            logger.warning("Please manually verify the output file integrity")
        
        # Log final success summary
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"CSV output process completed successfully in {total_time:.2f} seconds")
        logger.info(f"Output summary:")
        logger.info(f"  File path: {output_path}")
        logger.info(f"  Records saved: {record_count}")
        logger.info(f"  Columns saved: {column_count}")
        
        # Performance analysis
        if total_time > 60:  # More than 1 minute
            logger.warning(f"Slow CSV output detected: {total_time:.2f} seconds")
            logger.warning("Consider optimizing data size or output configuration")
        
    except Exception as e:
        total_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"CRITICAL ERROR: Failed to save results to CSV after {total_time:.2f} seconds")
        logger.error(f"Output path: {output_path}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Log additional debugging information
        try:
            import os
            logger.error("Output path analysis:")
            output_dir = os.path.dirname(output_path) if output_path else "Unknown"
            if output_dir and os.path.exists(output_dir):
                logger.error(f"  Output directory exists: {output_dir}")
                logger.error(f"  Directory writable: {os.access(output_dir, os.W_OK)}")
            else:
                logger.error(f"  Output directory does not exist: {output_dir}")
        except Exception as debug_error:
            logger.error(f"Could not analyze output path: {str(debug_error)}")
        
        raise RuntimeError(f"Critical failure in CSV output: {str(e)}") from e


def generate_final_summary_statistics(spark: SparkSession, result_df: DataFrame, debug: bool = False) -> dict:
    """
    Generate final result validation and summary statistics with comprehensive error handling.
    
    This function analyzes the final customer dimension results to provide comprehensive
    summary statistics including record counts, data quality metrics, and business validation.
    
    Args:
        spark: SparkSession instance
        result_df: DataFrame containing final customer dimension results
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        Dictionary containing summary statistics and validation results
        
    Raises:
        RuntimeError: If statistics generation fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting final summary statistics generation and validation")
    
    start_time = datetime.now()
    
    try:
        # Input validation with comprehensive error handling
        if not spark:
            raise ValueError("Invalid SparkSession provided for statistics generation")
        
        if result_df is None:
            raise ValueError("Invalid result DataFrame provided for statistics generation")
        
        logger.info("Input validation completed - proceeding with statistics generation")
        
        # Initialize statistics dictionary with error handling
        try:
            summary_stats = {
                "total_records": 0,
                "current_records": 0,
                "historical_records": 0,
                "unique_customers": 0,
                "unique_customer_keys": 0,
                "data_quality": {
                    "null_customer_ids": 0,
                    "null_customer_keys": 0,
                    "duplicate_customer_keys": 0
                },
                "date_range": {
                    "min_start_date": None,
                    "max_start_date": None,
                    "min_end_date": None,
                    "max_end_date": None
                },
                "customer_key_range": {
                    "min_customer_key": None,
                    "max_customer_key": None
                }
            }
            logger.debug("Statistics dictionary initialized successfully")
        except Exception as init_error:
            logger.error(f"Failed to initialize statistics dictionary: {str(init_error)}")
            raise RuntimeError(f"Statistics initialization failed: {str(init_error)}") from init_error
        
        # Basic record counts with comprehensive error handling
        logger.info("Calculating basic record counts...")
        try:
            logger.debug("Calculating total record count...")
            total_records = result_df.count()
            summary_stats["total_records"] = total_records
            logger.debug(f"Total records calculated: {total_records}")
        except Exception as count_error:
            logger.error(f"Failed to calculate total record count: {str(count_error)}")
            raise RuntimeError(f"Total record count calculation failed: {str(count_error)}") from count_error
        
        try:
            logger.debug("Calculating current records count...")
            current_records = result_df.filter(result_df.is_current == True).count()
            summary_stats["current_records"] = current_records
            logger.debug(f"Current records calculated: {current_records}")
        except Exception as current_error:
            logger.error(f"Failed to calculate current records count: {str(current_error)}")
            raise RuntimeError(f"Current records count calculation failed: {str(current_error)}") from current_error
        
        try:
            logger.debug("Calculating historical records count...")
            historical_records = result_df.filter(result_df.is_current == False).count()
            summary_stats["historical_records"] = historical_records
            logger.debug(f"Historical records calculated: {historical_records}")
        except Exception as historical_error:
            logger.error(f"Failed to calculate historical records count: {str(historical_error)}")
            raise RuntimeError(f"Historical records count calculation failed: {str(historical_error)}") from historical_error
        
        # Customer analysis with comprehensive error handling
        logger.info("Performing customer analysis...")
        try:
            logger.debug("Calculating unique customers count...")
            unique_customers = result_df.select("customer_id").distinct().count()
            summary_stats["unique_customers"] = unique_customers
            logger.debug(f"Unique customers calculated: {unique_customers}")
        except Exception as customers_error:
            logger.error(f"Failed to calculate unique customers count: {str(customers_error)}")
            raise RuntimeError(f"Unique customers count calculation failed: {str(customers_error)}") from customers_error
        
        try:
            logger.debug("Calculating unique customer keys count...")
            unique_customer_keys = result_df.select("customer_key").distinct().count()
            summary_stats["unique_customer_keys"] = unique_customer_keys
            logger.debug(f"Unique customer keys calculated: {unique_customer_keys}")
        except Exception as keys_error:
            logger.error(f"Failed to calculate unique customer keys count: {str(keys_error)}")
            raise RuntimeError(f"Unique customer keys count calculation failed: {str(keys_error)}") from keys_error
        
        # Data quality checks with comprehensive error handling
        logger.info("Performing data quality validation checks...")
        try:
            logger.debug("Checking for null customer_id values...")
            null_customer_ids = result_df.filter(result_df.customer_id.isNull()).count()
            summary_stats["data_quality"]["null_customer_ids"] = null_customer_ids
            logger.debug(f"Null customer_id count: {null_customer_ids}")
        except Exception as null_id_error:
            logger.error(f"Failed to check null customer_id values: {str(null_id_error)}")
            raise RuntimeError(f"Null customer_id check failed: {str(null_id_error)}") from null_id_error
        
        try:
            logger.debug("Checking for null customer_key values...")
            null_customer_keys = result_df.filter(result_df.customer_key.isNull()).count()
            summary_stats["data_quality"]["null_customer_keys"] = null_customer_keys
            logger.debug(f"Null customer_key count: {null_customer_keys}")
        except Exception as null_key_error:
            logger.error(f"Failed to check null customer_key values: {str(null_key_error)}")
            raise RuntimeError(f"Null customer_key check failed: {str(null_key_error)}") from null_key_error
        
        try:
            logger.debug("Calculating duplicate customer_key count...")
            duplicate_customer_keys = total_records - unique_customer_keys
            summary_stats["data_quality"]["duplicate_customer_keys"] = duplicate_customer_keys
            logger.debug(f"Duplicate customer_key count: {duplicate_customer_keys}")
        except Exception as duplicate_error:
            logger.error(f"Failed to calculate duplicate customer_key count: {str(duplicate_error)}")
            raise RuntimeError(f"Duplicate customer_key calculation failed: {str(duplicate_error)}") from duplicate_error
        
        # Date analysis with comprehensive error handling
        logger.info("Performing date range analysis...")
        try:
            logger.debug("Calculating date statistics...")
            date_stats = result_df.agg(
                {"effective_start_date": "min", "effective_start_date": "max",
                 "effective_end_date": "min", "effective_end_date": "max"}
            ).collect()[0]
            
            min_start_date = date_stats["min(effective_start_date)"]
            max_start_date = date_stats["max(effective_start_date)"]
            min_end_date = date_stats["min(effective_end_date)"]
            max_end_date = date_stats["max(effective_end_date)"]
            
            summary_stats["date_range"]["min_start_date"] = str(min_start_date) if min_start_date else None
            summary_stats["date_range"]["max_start_date"] = str(max_start_date) if max_start_date else None
            summary_stats["date_range"]["min_end_date"] = str(min_end_date) if min_end_date else None
            summary_stats["date_range"]["max_end_date"] = str(max_end_date) if max_end_date else None
            
            logger.debug(f"Date range analysis completed: start {min_start_date} to {max_start_date}, end {min_end_date} to {max_end_date}")
        except Exception as date_error:
            logger.error(f"Failed to calculate date statistics: {str(date_error)}")
            raise RuntimeError(f"Date statistics calculation failed: {str(date_error)}") from date_error
        
        # Customer key range analysis with comprehensive error handling
        logger.info("Performing customer key range analysis...")
        try:
            logger.debug("Calculating customer key statistics...")
            key_stats = result_df.agg(
                {"customer_key": "min", "customer_key": "max"}
            ).collect()[0]
            
            min_customer_key = key_stats["min(customer_key)"]
            max_customer_key = key_stats["max(customer_key)"]
            
            summary_stats["customer_key_range"]["min_customer_key"] = min_customer_key
            summary_stats["customer_key_range"]["max_customer_key"] = max_customer_key
            
            logger.debug(f"Customer key range: {min_customer_key} to {max_customer_key}")
        except Exception as key_stats_error:
            logger.error(f"Failed to calculate customer key statistics: {str(key_stats_error)}")
            raise RuntimeError(f"Customer key statistics calculation failed: {str(key_stats_error)}") from key_stats_error
        
        # Log comprehensive summary statistics
        logger.info("Final Summary Statistics:")
        logger.info(f"  Total Records: {summary_stats['total_records']}")
        logger.info(f"  Current Records: {summary_stats['current_records']}")
        logger.info(f"  Historical Records: {summary_stats['historical_records']}")
        logger.info(f"  Unique Customers: {summary_stats['unique_customers']}")
        logger.info(f"  Unique Customer Keys: {summary_stats['unique_customer_keys']}")
        logger.info(f"  Customer Key Range: {summary_stats['customer_key_range']['min_customer_key']} to {summary_stats['customer_key_range']['max_customer_key']}")
        
        # Data quality validation with comprehensive error handling
        logger.info("Data Quality Validation:")
        try:
            data_quality_issues = 0
            
            if summary_stats["data_quality"]["null_customer_ids"] > 0:
                logger.error(f"  ERROR: Found {summary_stats['data_quality']['null_customer_ids']} records with null customer_id")
                data_quality_issues += 1
            
            if summary_stats["data_quality"]["null_customer_keys"] > 0:
                logger.error(f"  ERROR: Found {summary_stats['data_quality']['null_customer_keys']} records with null customer_key")
                data_quality_issues += 1
            
            if summary_stats["data_quality"]["duplicate_customer_keys"] > 0:
                logger.error(f"  ERROR: Found {summary_stats['data_quality']['duplicate_customer_keys']} duplicate customer_key values")
                data_quality_issues += 1
            
            if data_quality_issues == 0:
                logger.info("  ✓ All data quality checks passed")
            else:
                logger.error(f"  Found {data_quality_issues} data quality issues")
                
        except Exception as quality_validation_error:
            logger.warning(f"Failed to perform data quality validation logging: {str(quality_validation_error)}")
            logger.warning("Continuing with statistics generation")
        
        # Business validation with comprehensive error handling
        logger.info("Business Validation:")
        try:
            if summary_stats["current_records"] > summary_stats["unique_customers"]:
                logger.error(f"  ERROR: More current records ({summary_stats['current_records']}) than unique customers ({summary_stats['unique_customers']})")
            elif summary_stats["current_records"] == summary_stats["unique_customers"]:
                logger.info("  ✓ Current records count matches unique customers (expected for SCD Type II)")
            else:
                logger.warning(f"  WARNING: Fewer current records ({summary_stats['current_records']}) than unique customers ({summary_stats['unique_customers']})")
                
        except Exception as business_validation_error:
            logger.warning(f"Failed to perform business validation logging: {str(business_validation_error)}")
            logger.warning("Continuing with statistics generation")
        
        # Show sample records in debug mode with comprehensive error handling
        if debug and summary_stats["total_records"] > 0:
            try:
                logger.debug("Collecting sample final results (first 5 records)...")
                sample_results = result_df.limit(5).collect()
                logger.debug("Sample final results:")
                for i, row in enumerate(sample_results, 1):
                    logger.debug(f"  Record {i}: {row.asDict()}")
            except Exception as sample_error:
                logger.warning(f"Failed to collect sample results for debug mode: {str(sample_error)}")
                logger.warning("Continuing without sample results display")
        
        # Final validation and completion
        statistics_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Final summary statistics generation completed successfully in {statistics_time:.2f} seconds")
        
        return summary_stats
        
    except Exception as e:
        statistics_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"CRITICAL ERROR: Failed to generate final summary statistics after {statistics_time:.2f} seconds")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Log partial statistics if available
        try:
            logger.error("Partial statistics collected before failure:")
            for key, value in summary_stats.items():
                logger.error(f"  {key}: {value}")
        except Exception as partial_error:
            logger.error(f"Could not log partial statistics: {str(partial_error)}")
        
        raise RuntimeError(f"Critical failure in summary statistics generation: {str(e)}") from e
        
    except Exception as e:
        logger.error(f"Failed to generate final summary statistics: {str(e)}")
        raise


def combine_and_output_results(spark: SparkSession, unchanged_df: DataFrame, closed_historical_df: DataFrame,
                             new_current_changed_df: DataFrame, new_customer_df: DataFrame, 
                             output_path: str, debug: bool = False) -> dict:
    """
    Combine all processed record types and output final results with validation.
    
    This function orchestrates the final result combination using SQL UNION operations,
    saves the results to CSV with proper formatting, and generates comprehensive
    summary statistics for validation.
    
    Args:
        spark: SparkSession instance
        unchanged_df: DataFrame containing unchanged customer records
        closed_historical_df: DataFrame containing closed historical records
        new_current_changed_df: DataFrame containing new current records for changed customers
        new_customer_df: DataFrame containing new customer records
        output_path: Path where CSV output should be saved
        debug: Enable debug mode for detailed logging and sample results
        
    Returns:
        Dictionary containing summary statistics and validation results
        
    Raises:
        Exception: If result combination or output fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting result combination and output process")
    
    try:
        # Step 1: Execute final result assembly using SQL UNION operations
        logger.info("Step 1: Combining all processed record types using SQL UNION")
        final_result_df = execute_final_result_assembly(
            spark, unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df, debug
        )
        
        # Step 2: Generate summary statistics and validation
        logger.info("Step 2: Generating final result validation and summary statistics")
        summary_stats = generate_final_summary_statistics(spark, final_result_df, debug)
        
        # Step 3: Save results to CSV with proper formatting
        logger.info("Step 3: Saving final results to CSV with header and proper formatting")
        save_results_to_csv(spark, final_result_df, output_path, debug)
        
        # Step 4: Final validation and reporting
        logger.info("Result combination and output completed successfully")
        logger.info(f"Final results saved to: {output_path}")
        logger.info(f"Processing summary: {summary_stats['total_records']} total records, "
                   f"{summary_stats['current_records']} current, {summary_stats['historical_records']} historical")
        
        return summary_stats
        
    except Exception as e:
        logger.error(f"Failed to combine and output results: {str(e)}")
        raise


def main():
    """
    Main execution function for SparkSQL SCD Type II implementation with comprehensive error handling and logging.
    """
    # Initialize variables for cleanup
    spark = None
    start_time = datetime.now()
    
    try:
        # Parse command line arguments with error handling
        try:
            args = parse_arguments()
        except Exception as arg_error:
            print(f"CRITICAL ERROR: Failed to parse command line arguments: {str(arg_error)}")
            sys.exit(1)
        
        # Setup logging with error handling
        try:
            setup_logging(debug=args.debug)
            logger = logging.getLogger(__name__)
        except Exception as log_error:
            print(f"CRITICAL ERROR: Failed to setup logging: {str(log_error)}")
            sys.exit(1)
        
        logger.info("=" * 80)
        logger.info("STARTING SPARKSQL SCD TYPE II IMPLEMENTATION")
        logger.info("=" * 80)
        logger.info(f"Execution started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Configuration:")
        logger.info(f"  Existing data path: {args.existing_data}")
        logger.info(f"  New data path: {args.new_data}")
        logger.info(f"  Output path: {args.output_path}")
        logger.info(f"  Application name: {args.app_name}")
        logger.info(f"  Debug mode: {args.debug}")
        logger.info("-" * 80)
        
        # Step 1: Initialize Spark session with comprehensive error handling
        logger.info("STEP 1: Initializing Spark session...")
        step_start = datetime.now()
        try:
            spark = create_spark_session(args.app_name, debug=args.debug)
            step_time = (datetime.now() - step_start).total_seconds()
            logger.info(f"STEP 1 COMPLETED: Spark session initialized successfully in {step_time:.2f} seconds")
        except Exception as spark_error:
            logger.error(f"STEP 1 FAILED: Spark session initialization failed: {str(spark_error)}")
            logger.error(f"Error type: {type(spark_error).__name__}")
            raise RuntimeError(f"Critical failure in Spark session initialization: {str(spark_error)}") from spark_error
        
        # Step 2: Load customer data files with comprehensive error handling
        logger.info("STEP 2: Loading customer data files...")
        step_start = datetime.now()
        try:
            logger.info("STEP 2a: Loading existing customer data...")
            existing_df = load_existing_customer_data(spark, args.existing_data)
            
            logger.info("STEP 2b: Loading new customer data...")
            new_df = load_new_customer_data(spark, args.new_data)
            
            step_time = (datetime.now() - step_start).total_seconds()
            logger.info(f"STEP 2 COMPLETED: Customer data files loaded successfully in {step_time:.2f} seconds")
        except Exception as data_error:
            logger.error(f"STEP 2 FAILED: Data loading failed: {str(data_error)}")
            logger.error(f"Error type: {type(data_error).__name__}")
            raise RuntimeError(f"Critical failure in data loading: {str(data_error)}") from data_error
        
        # Step 3: Register temporary SQL views with comprehensive error handling
        logger.info("STEP 3: Registering temporary SQL views...")
        step_start = datetime.now()
        try:
            register_temporary_views(spark, existing_df, new_df)
            step_time = (datetime.now() - step_start).total_seconds()
            logger.info(f"STEP 3 COMPLETED: Temporary views registered successfully in {step_time:.2f} seconds")
        except Exception as view_error:
            logger.error(f"STEP 3 FAILED: Temporary view registration failed: {str(view_error)}")
            logger.error(f"Error type: {type(view_error).__name__}")
            raise RuntimeError(f"Critical failure in view registration: {str(view_error)}") from view_error
        
        # Step 4: Execute SCD Type II operations with comprehensive error handling
        logger.info("STEP 4: Executing SCD Type II operations using SQL queries...")
        step_start = datetime.now()
        try:
            unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df = execute_scd_type2_operations(spark, debug=args.debug)
            step_time = (datetime.now() - step_start).total_seconds()
            logger.info(f"STEP 4 COMPLETED: SCD Type II operations executed successfully in {step_time:.2f} seconds")
        except Exception as scd_error:
            logger.error(f"STEP 4 FAILED: SCD Type II operations failed: {str(scd_error)}")
            logger.error(f"Error type: {type(scd_error).__name__}")
            raise RuntimeError(f"Critical failure in SCD operations: {str(scd_error)}") from scd_error
        
        # Step 5: Combine results and generate output with comprehensive error handling
        logger.info("STEP 5: Combining results and generating output...")
        step_start = datetime.now()
        try:
            summary_stats = combine_and_output_results(
                spark, unchanged_df, closed_historical_df, new_current_changed_df, new_customer_df,
                args.output_path, debug=args.debug
            )
            step_time = (datetime.now() - step_start).total_seconds()
            logger.info(f"STEP 5 COMPLETED: Results combined and output generated successfully in {step_time:.2f} seconds")
        except Exception as output_error:
            logger.error(f"STEP 5 FAILED: Result combination and output failed: {str(output_error)}")
            logger.error(f"Error type: {type(output_error).__name__}")
            raise RuntimeError(f"Critical failure in result output: {str(output_error)}") from output_error
        
        # Final success logging
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 80)
        logger.info("SPARKSQL SCD TYPE II IMPLEMENTATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Execution completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Final processing summary:")
        for key, value in summary_stats.items():
            logger.info(f"  {key}: {value}")
        logger.info("=" * 80)
        
        # Performance analysis
        if total_time > 300:  # 5 minutes
            logger.warning(f"Long execution time detected: {total_time:.2f} seconds")
            logger.warning("Consider optimizing data size or Spark configuration for better performance")
        elif total_time < 10:
            logger.info(f"Excellent performance: completed in {total_time:.2f} seconds")
        
    except KeyboardInterrupt:
        logger.error("EXECUTION INTERRUPTED: User requested termination (Ctrl+C)")
        sys.exit(130)  # Standard exit code for Ctrl+C
        
    except SystemExit as sys_exit:
        # Re-raise SystemExit to preserve exit codes
        raise
        
    except Exception as e:
        total_time = (datetime.now() - start_time).total_seconds()
        
        # Create logger if it doesn't exist (in case logging setup failed)
        try:
            logger = logging.getLogger(__name__)
        except:
            # Fallback to print if logging completely failed
            print(f"CRITICAL ERROR: SparkSQL SCD Type II implementation failed after {total_time:.2f} seconds")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            sys.exit(1)
        
        logger.error("=" * 80)
        logger.error("CRITICAL ERROR: SPARKSQL SCD TYPE II IMPLEMENTATION FAILED")
        logger.error("=" * 80)
        logger.error(f"Total execution time before failure: {total_time:.2f} seconds")
        logger.error(f"Error occurred at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Log stack trace in debug mode
        if hasattr(args, 'debug') and args.debug:
            import traceback
            logger.error("Full stack trace:")
            logger.error(traceback.format_exc())
        
        logger.error("=" * 80)
        sys.exit(1)
        
    finally:
        # Clean up Spark session with comprehensive error handling
        cleanup_start = datetime.now()
        if spark:
            try:
                # Get logger safely
                try:
                    logger = logging.getLogger(__name__)
                    logger.info("Cleaning up Spark session...")
                except:
                    print("Cleaning up Spark session...")
                
                # Try to uncache any cached DataFrames
                try:
                    spark.catalog.clearCache()
                    try:
                        logger.debug("Cleared Spark catalog cache")
                    except:
                        pass
                except Exception as cache_error:
                    try:
                        logger.warning(f"Failed to clear Spark cache: {str(cache_error)}")
                    except:
                        print(f"Failed to clear Spark cache: {str(cache_error)}")
                
                # Stop the Spark session
                spark.stop()
                cleanup_time = (datetime.now() - cleanup_start).total_seconds()
                try:
                    logger.info(f"Spark session cleanup completed in {cleanup_time:.2f} seconds")
                except:
                    print(f"Spark session cleanup completed in {cleanup_time:.2f} seconds")
                
            except Exception as cleanup_error:
                cleanup_time = (datetime.now() - cleanup_start).total_seconds()
                try:
                    logger.error(f"Failed to cleanup Spark session after {cleanup_time:.2f} seconds: {str(cleanup_error)}")
                    logger.error("This may cause resource leaks - manual cleanup may be required")
                except:
                    print(f"Failed to cleanup Spark session after {cleanup_time:.2f} seconds: {str(cleanup_error)}")
                    print("This may cause resource leaks - manual cleanup may be required")
        
        # Final cleanup logging
        try:
            total_time = (datetime.now() - start_time).total_seconds()
            try:
                logger = logging.getLogger(__name__)
                logger.info(f"Application shutdown completed - Total runtime: {total_time:.2f} seconds")
            except:
                # Ignore any logging errors during final cleanup
                pass
        except:
            # Ignore any logging errors during final cleanup
            pass


if __name__ == "__main__":
    main()