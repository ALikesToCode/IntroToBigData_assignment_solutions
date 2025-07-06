#!/usr/bin/env python3
"""
Local Test Script for SCD Type II Implementation
Course: Introduction to Big Data - Week 4 Assignment

This script tests the SCD Type II implementation locally before deploying to Dataproc.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import shutil
import subprocess
import logging
from pathlib import Path

def setup_logging():
    """Configure logging for testing"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def check_dependencies():
    """Check if required dependencies are installed"""
    logger = logging.getLogger(__name__)
    
    try:
        import pyspark
        logger.info(f"PySpark version: {pyspark.__version__}")
        
        # Check Java
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("Java is installed and accessible")
        else:
            logger.error("Java is not installed or not in PATH")
            return False
            
        return True
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        return False
    except Exception as e:
        logger.error(f"Error checking dependencies: {e}")
        return False

def prepare_test_environment():
    """Prepare local test environment"""
    logger = logging.getLogger(__name__)
    
    # Create output directory if it doesn't exist
    output_dir = Path("output")
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir()
    
    logger.info("Test environment prepared")
    return True

def run_scd_test():
    """Run the SCD Type II implementation locally"""
    logger = logging.getLogger(__name__)
    
    # Define file paths
    script_path = "scd_type2_implementation.py"
    existing_data = "data/customer_existing.csv"
    new_data = "data/customer_new.csv"
    output_path = "output/customer_dimension_updated"
    
    # Check if input files exist
    if not os.path.exists(existing_data):
        logger.error(f"Existing data file not found: {existing_data}")
        return False
    
    if not os.path.exists(new_data):
        logger.error(f"New data file not found: {new_data}")
        return False
    
    if not os.path.exists(script_path):
        logger.error(f"Script file not found: {script_path}")
        return False
    
    # Run the script
    logger.info("Running SCD Type II implementation...")
    try:
        result = subprocess.run([
            sys.executable, script_path, 
            '--existing_data_path', existing_data,
            '--new_data_path', new_data,
            '--output_path', output_path
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logger.info("SCD Type II implementation completed successfully")
            print("STDOUT:")
            print(result.stdout)
            return True
        else:
            logger.error("SCD Type II implementation failed")
            print("STDERR:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("SCD Type II implementation timed out")
        return False
    except Exception as e:
        logger.error(f"Error running SCD implementation: {e}")
        return False

def validate_results():
    """Validate the output results"""
    logger = logging.getLogger(__name__)
    
    output_dir = Path("output/customer_dimension_updated")
    
    if not output_dir.exists():
        logger.error("Output directory not found")
        return False
    
    # Check if CSV files were created
    csv_files = list(output_dir.glob("*.csv"))
    if not csv_files:
        logger.error("No CSV output files found")
        return False
    
    logger.info(f"Found {len(csv_files)} output file(s)")
    
    # Read and validate the first CSV file
    try:
        import pandas as pd
        
        # Read the output file
        output_file = csv_files[0]
        df = pd.read_csv(output_file)
        
        logger.info(f"Output contains {len(df)} records")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Validate schema
        expected_columns = ['customer_key', 'customer_id', 'customer_name', 'address', 
                          'phone', 'email', 'effective_start_date', 'effective_end_date', 'is_current']
        
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing expected columns: {missing_columns}")
            return False
        
        # Check for current records
        current_records = df[df['is_current'] == True]
        logger.info(f"Current records: {len(current_records)}")
        
        # Check for historical records
        historical_records = df[df['is_current'] == False]
        logger.info(f"Historical records: {len(historical_records)}")
        
        # Check unique customers
        unique_customers = df['customer_id'].nunique()
        logger.info(f"Unique customers: {unique_customers}")
        
        # Display sample results
        print("\n=== Sample Output Results ===")
        print(df.head(10).to_string(index=False))
        
        return True
        
    except ImportError:
        logger.warning("Pandas not available for detailed validation")
        logger.info("Basic file validation passed")
        return True
    except Exception as e:
        logger.error(f"Error validating results: {e}")
        return False

def display_test_summary():
    """Display test summary and next steps"""
    print("\n" + "="*60)
    print("SCD TYPE II LOCAL TEST SUMMARY")
    print("="*60)
    print("✓ Dependencies checked")
    print("✓ Local environment prepared")
    print("✓ SCD Type II implementation executed")
    print("✓ Results validated")
    print("\nNext Steps:")
    print("1. Review the output files in the 'output' directory")
    print("2. Deploy to Google Cloud Dataproc using the cloud deployment script")
    print("3. Compare local and cloud results for consistency")
    print("\nCloud Deployment Command:")
    print("python deploy_to_dataproc.py")
    print("="*60)

def main():
    """Main test execution"""
    logger = setup_logging()
    logger.info("Starting SCD Type II local testing")
    
    try:
        # Check dependencies
        if not check_dependencies():
            print("❌ Dependency check failed")
            sys.exit(1)
        
        # Prepare test environment
        if not prepare_test_environment():
            print("❌ Test environment preparation failed")
            sys.exit(1)
        
        # Run SCD implementation
        if not run_scd_test():
            print("❌ SCD Type II test failed")
            sys.exit(1)
        
        # Validate results
        if not validate_results():
            print("❌ Results validation failed")
            sys.exit(1)
        
        # Display summary
        display_test_summary()
        
        logger.info("All tests passed successfully!")
        print("✅ SCD Type II local testing completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Testing interrupted by user")
        print("\n⚠️ Testing interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during testing: {e}")
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 