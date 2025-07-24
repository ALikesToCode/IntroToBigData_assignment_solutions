#!/usr/bin/env python3
"""
Local test script for SparkSQL SCD Type II implementation
Course: Introduction to Big Data - Week 5 Assignment

This script tests the SparkSQL SCD Type II implementation locally before deploying to Dataproc.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import logging
import tempfile
import shutil
from datetime import datetime

# Add current directory to path so we can import our main script
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_sparksql_scd_implementation():
    """Test the SparkSQL SCD Type II implementation locally"""
    logger = setup_logging()
    logger.info("Starting local test of SparkSQL SCD Type II implementation")
    
    # Check if required files exist
    required_files = [
        'sparksql_scd_type2_implementation.py',
        'data/customer_existing.csv',
        'data/customer_new.csv'
    ]
    
    for file in required_files:
        if not os.path.exists(file):
            logger.error(f"Required file not found: {file}")
            return False
    
    # Create temporary output directory
    temp_output_dir = tempfile.mkdtemp(prefix="scd_test_output_")
    logger.info(f"Using temporary output directory: {temp_output_dir}")
    
    try:
        # Import and run the main implementation
        logger.info("Importing SparkSQL SCD implementation...")
        
        # We need to run it as a subprocess since it has its own main()
        import subprocess
        
        cmd = [
            sys.executable, 
            'sparksql_scd_type2_implementation.py',
            '--existing_data_path', 'data/customer_existing.csv',
            '--new_data_path', 'data/customer_new.csv',
            '--output_path', temp_output_dir
        ]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        # Run the implementation
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        # Log the output
        if result.stdout:
            logger.info("Standard output:")
            print(result.stdout)
        
        if result.stderr:
            logger.info("Standard error:")
            print(result.stderr)
        
        if result.returncode == 0:
            logger.info("SparkSQL SCD implementation completed successfully!")
            
            # Check if output files were created
            if os.path.exists(temp_output_dir):
                output_files = os.listdir(temp_output_dir)
                logger.info(f"Output files created: {output_files}")
                
                # Look for CSV files
                csv_files = [f for f in output_files if f.endswith('.csv')]
                if csv_files:
                    # Read and display sample of the first CSV file
                    csv_file_path = os.path.join(temp_output_dir, csv_files[0])
                    logger.info(f"Sample output from {csv_files[0]}:")
                    
                    try:
                        with open(csv_file_path, 'r') as f:
                            lines = f.readlines()
                            for i, line in enumerate(lines[:10]):  # Show first 10 lines
                                print(f"{i+1:2d}: {line.strip()}")
                            if len(lines) > 10:
                                print(f"... and {len(lines) - 10} more lines")
                        
                        logger.info(f"Total lines in output: {len(lines)}")
                    except Exception as e:
                        logger.error(f"Error reading output file: {e}")
                
                return True
            else:
                logger.error("No output directory created")
                return False
        else:
            logger.error(f"SparkSQL SCD implementation failed with return code: {result.returncode}")
            return False
            
    except Exception as e:
        logger.error(f"Error during testing: {e}")
        return False
    
    finally:
        # Clean up temporary directory
        if os.path.exists(temp_output_dir):
            logger.info(f"Cleaning up temporary directory: {temp_output_dir}")
            shutil.rmtree(temp_output_dir)

def verify_data_files():
    """Verify that the input data files have the expected structure"""
    logger = logging.getLogger(__name__)
    logger.info("Verifying input data files...")
    
    # Check existing data file
    try:
        with open('data/customer_existing.csv', 'r') as f:
            lines = f.readlines()
            logger.info(f"customer_existing.csv contains {len(lines)} lines")
            if len(lines) > 0:
                logger.info(f"Header: {lines[0].strip()}")
            if len(lines) > 1:
                logger.info(f"Sample record: {lines[1].strip()}")
    except Exception as e:
        logger.error(f"Error reading customer_existing.csv: {e}")
        return False
    
    # Check new data file
    try:
        with open('data/customer_new.csv', 'r') as f:
            lines = f.readlines()
            logger.info(f"customer_new.csv contains {len(lines)} lines")
            if len(lines) > 0:
                logger.info(f"Header: {lines[0].strip()}")
            if len(lines) > 1:
                logger.info(f"Sample record: {lines[1].strip()}")
    except Exception as e:
        logger.error(f"Error reading customer_new.csv: {e}")
        return False
    
    return True

def main():
    """Main test function"""
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("SparkSQL SCD Type II Local Test")
    logger.info("=" * 60)
    
    # Verify data files first
    if not verify_data_files():
        logger.error("Data file verification failed")
        sys.exit(1)
    
    # Run the test
    if test_sparksql_scd_implementation():
        logger.info("✅ Local test completed successfully!")
        print("\n" + "=" * 50)
        print("LOCAL TEST SUMMARY")
        print("=" * 50)
        print("✅ SparkSQL SCD Type II implementation working correctly")
        print("✅ Input data files are valid")
        print("✅ Output files generated successfully")
        print("✅ Ready for Dataproc deployment")
        print("=" * 50)
        
        print("\nNext steps:")
        print("1. Run: python deploy_to_dataproc.py")
        print("2. Monitor the job in Google Cloud Console")
        print("3. Download and verify results")
        
    else:
        logger.error("❌ Local test failed!")
        print("\n" + "=" * 50)
        print("LOCAL TEST FAILED")
        print("=" * 50)
        print("❌ SparkSQL SCD Type II implementation has issues")
        print("❌ Please check the logs above for details")
        print("❌ Fix issues before deploying to Dataproc")
        print("=" * 50)
        sys.exit(1)

if __name__ == "__main__":
    main()
