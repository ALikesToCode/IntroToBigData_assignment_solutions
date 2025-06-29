"""
Test script to run the Spark application locally and validate results.

This script helps test the Spark application in local mode before deploying
to a cluster or cloud environment.
"""

import subprocess
import sys
import os
import json
from datetime import datetime

def check_spark_installation():
    """Check if PySpark is properly installed."""
    try:
        import pyspark
        print(f"✓ PySpark version: {pyspark.__version__}")
        return True
    except ImportError:
        print("✗ PySpark not found. Install with: pip install pyspark")
        return False

def check_java_installation():
    """Check if Java is installed and accessible."""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True, check=True)
        print("✓ Java is installed")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("✗ Java not found. Spark requires Java 8 or later.")
        print("  Install Java: https://adoptopenjdk.net/")
        return False

def validate_data_file():
    """Validate the data.txt file exists and has expected format."""
    if not os.path.exists("data.txt"):
        print("✗ data.txt not found")
        return False
    
    with open("data.txt", "r") as f:
        lines = f.readlines()
    
    print(f"✓ Data file contains {len(lines)} lines")
    
    # Validate a few lines
    for i, line in enumerate(lines[:3]):
        parts = line.strip().split()
        if len(parts) != 3:
            print(f"✗ Line {i+1} has unexpected format: {line.strip()}")
            return False
        try:
            # Try to parse time
            time_parts = parts[1].split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                print(f"✗ Invalid time on line {i+1}: {parts[1]}")
                return False
        except (ValueError, IndexError):
            print(f"✗ Cannot parse time on line {i+1}: {parts[1]}")
            return False
    
    print("✓ Data file format looks correct")
    return True

def run_spark_application():
    """Run the Spark application and capture output."""
    print("\n" + "="*50)
    print("Running Spark Application")
    print("="*50)
    
    try:
        # Run the Spark application
        result = subprocess.run([sys.executable, "click_analysis_spark.py"], 
                              capture_output=True, text=True, check=True)
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        return result.stdout, result.stderr
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Application failed with return code {e.returncode}")
        print("STDOUT:")
        print(e.stdout)
        print("STDERR:")
        print(e.stderr)
        return None, None

def validate_results(output):
    """Validate the output results make sense."""
    if not output:
        return False
    
    # Look for the results section
    lines = output.split('\n')
    
    # Find the results table
    found_results = False
    interval_counts = {}
    
    for i, line in enumerate(lines):
        if "Time Interval" in line and "Click Count" in line:
            found_results = True
            # Parse the next few lines for results
            for j in range(i+2, min(i+7, len(lines))):
                if lines[j].strip() and not lines[j].startswith('-') and not lines[j].startswith('='):
                    parts = lines[j].split()
                    if len(parts) >= 2 and parts[0] in ["0-6", "6-12", "12-18", "18-24"]:
                        try:
                            interval_counts[parts[0]] = int(parts[1])
                        except ValueError:
                            pass
            break
    
    if not found_results:
        print("✗ Could not find results table in output")
        return False
    
    print("✓ Found results table")
    
    # Validate intervals
    expected_intervals = ["0-6", "6-12", "12-18", "18-24"]
    for interval in expected_intervals:
        if interval not in interval_counts:
            print(f"✗ Missing interval {interval} in results")
            return False
    
    # Calculate total
    total = sum(interval_counts.values())
    print(f"✓ Total clicks: {total}")
    
    # Show breakdown
    for interval in expected_intervals:
        count = interval_counts[interval]
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {interval}: {count} clicks ({percentage:.1f}%)")
    
    return True

def create_test_report(output, stderr):
    """Create a test report."""
    report = {
        "timestamp": datetime.now().isoformat(),
        "test_status": "completed",
        "output": output,
        "stderr": stderr,
        "data_file_lines": 0
    }
    
    # Count data file lines
    if os.path.exists("data.txt"):
        with open("data.txt", "r") as f:
            report["data_file_lines"] = len(f.readlines())
    
    with open("test_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"\n✓ Test report saved to test_report.json")

def main():
    """Main test function."""
    print("Apache Spark Local Testing")
    print("="*50)
    
    # Run pre-flight checks
    print("Running pre-flight checks...")
    
    all_checks_passed = True
    
    if not check_java_installation():
        all_checks_passed = False
    
    if not check_spark_installation():
        all_checks_passed = False
    
    if not validate_data_file():
        all_checks_passed = False
    
    if not all_checks_passed:
        print("\n✗ Pre-flight checks failed. Please fix issues before running.")
        sys.exit(1)
    
    print("\n✓ All pre-flight checks passed!")
    
    # Run the application
    output, stderr = run_spark_application()
    
    if output is None:
        print("\n✗ Application failed to run")
        sys.exit(1)
    
    # Validate results
    print("\n" + "="*50)
    print("Validating Results")
    print("="*50)
    
    if validate_results(output):
        print("\n✓ All validations passed!")
        print("✓ Spark application is working correctly")
    else:
        print("\n✗ Results validation failed")
        sys.exit(1)
    
    # Create test report
    create_test_report(output, stderr)
    
    print("\n" + "="*50)
    print("Test Summary")
    print("="*50)
    print("✓ Java installation: OK")
    print("✓ PySpark installation: OK")
    print("✓ Data file validation: OK")
    print("✓ Application execution: OK")
    print("✓ Results validation: OK")
    print("\nYour Spark application is ready for deployment!")

if __name__ == "__main__":
    main() 