"""
Apache Spark application to analyze user click data by time intervals.

This application reads user click data from a text file and counts the number of clicks
that fall within each of four 6-hour intervals in a day (0-6, 6-12, 12-18, 18-24).

Data format: Each line contains: DATE TIME USER_ID
Example: 10-Jan 11:10 1001
"""

from pyspark.sql import SparkSession
import sys
import os

def get_time_interval(time_str):
    """
    Assigns a time string to one of four 6-hour intervals.
    
    Args:
        time_str (str): Time in HH:MM format (e.g., "11:10")
        
    Returns:
        str: Time interval ("0-6", "6-12", "12-18", or "18-24")
    """
    try:
        # Extract hour from time string (format: HH:MM)
        hour = int(time_str.split(':')[0])
        
        if 0 <= hour < 6:
            return "0-6"
        elif 6 <= hour < 12:
            return "6-12"
        elif 12 <= hour < 18:
            return "12-18"
        else:  # 18 <= hour < 24
            return "18-24"
    except (ValueError, IndexError) as e:
        print(f"Error parsing time '{time_str}': {e}")
        return "UNKNOWN"

def validate_data_file(file_path):
    """
    Validates that the data file exists and is readable.
    
    Args:
        file_path (str): Path to the data file
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    if not os.path.exists(file_path):
        print(f"Error: Data file '{file_path}' does not exist.")
        return False
    
    if not os.path.isfile(file_path):
        print(f"Error: '{file_path}' is not a file.")
        return False
    
    try:
        with open(file_path, 'r') as f:
            first_line = f.readline().strip()
            if not first_line:
                print(f"Error: Data file '{file_path}' is empty.")
                return False
            
            # Validate format of first line
            parts = first_line.split()
            if len(parts) != 3:
                print(f"Warning: First line has unexpected format: '{first_line}'")
                print("Expected format: DATE TIME USER_ID")
                
    except IOError as e:
        print(f"Error reading file '{file_path}': {e}")
        return False
    
    return True

def analyze_click_data(spark_session, data_file="data.txt"):
    """
    Analyzes user click data to count clicks by time intervals.
    
    Args:
        spark_session: SparkSession object
        data_file (str): Path to the data file
        
    Returns:
        list: List of (interval, count) tuples
    """
    try:
        # Read the data file into an RDD
        lines = spark_session.sparkContext.textFile(data_file)
        
        # Check if file was read successfully
        if lines.isEmpty():
            print(f"Warning: No data found in '{data_file}'")
            return []
        
        print(f"Processing {lines.count()} lines from '{data_file}'")
        
        # Process each line to extract time intervals and count clicks
        # Line format: "10-Jan 11:10 1001" -> split()[1] gives "11:10"
        def process_line(line):
            try:
                parts = line.strip().split()
                if len(parts) >= 2:
                    time_str = parts[1]  # Second element is the time
                    interval = get_time_interval(time_str)
                    return (interval, 1)
                else:
                    print(f"Warning: Skipping malformed line: '{line}'")
                    return ("MALFORMED", 1)
            except Exception as e:
                print(f"Error processing line '{line}': {e}")
                return ("ERROR", 1)
        
        # Map each line to (interval, 1) and reduce by key to count
        interval_counts = lines.map(process_line) \
                              .filter(lambda x: x[0] not in ["MALFORMED", "ERROR", "UNKNOWN"]) \
                              .reduceByKey(lambda a, b: a + b)
        
        # Collect results
        results = interval_counts.collect()
        
        # Sort results by interval for consistent output
        interval_order = ["0-6", "6-12", "12-18", "18-24"]
        sorted_results = []
        
        # Create a dictionary for easy lookup
        result_dict = dict(results)
        
        # Add results in order, with 0 for missing intervals
        for interval in interval_order:
            count = result_dict.get(interval, 0)
            sorted_results.append((interval, count))
        
        return sorted_results
        
    except Exception as e:
        print(f"Error during Spark processing: {e}")
        return []

def main():
    """
    Main function that sets up Spark and runs the click analysis.
    """
    print("=" * 60)
    print("User Click Data Analysis with Apache Spark")
    print("=" * 60)
    
    # Configuration
    data_file = "data.txt"
    app_name = "UserClickAnalysis"
    
    # Check if custom data file is provided
    if len(sys.argv) > 1:
        data_file = sys.argv[1]
    
    print(f"Data file: {data_file}")
    print(f"Application: {app_name}")
    print("-" * 60)
    
    # Validate data file before starting Spark
    if not validate_data_file(data_file):
        print("Exiting due to data file validation errors.")
        sys.exit(1)
    
    # Initialize Spark session
    try:
        print("Initializing Spark session...")
        spark = SparkSession.builder \
                          .appName(app_name) \
                          .config("spark.sql.adaptive.enabled", "true") \
                          .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                          .getOrCreate()
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        print(f"Spark version: {spark.version}")
        print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
        print("-" * 60)
        
    except Exception as e:
        print(f"Error initializing Spark: {e}")
        sys.exit(1)
    
    try:
        # Analyze the click data
        print("Analyzing click data...")
        results = analyze_click_data(spark, data_file)
        
        if not results:
            print("No results to display.")
            return
        
        # Display results
        print("\nClick Analysis Results:")
        print("=" * 30)
        print(f"{'Time Interval':<15} {'Click Count':<10}")
        print("-" * 30)
        
        total_clicks = 0
        for interval, count in results:
            print(f"{interval:<15} {count:<10}")
            total_clicks += count
        
        print("-" * 30)
        print(f"{'Total':<15} {total_clicks:<10}")
        print("=" * 30)
        
        # Additional statistics
        if total_clicks > 0:
            print("\nDistribution Analysis:")
            print("-" * 30)
            for interval, count in results:
                percentage = (count / total_clicks) * 100
                print(f"{interval}: {count} clicks ({percentage:.1f}%)")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        sys.exit(1)
    
    finally:
        # Clean up Spark session
        try:
            print("\nStopping Spark session...")
            spark.stop()
            print("Spark session stopped successfully.")
        except Exception as e:
            print(f"Error stopping Spark: {e}")

if __name__ == "__main__":
    main() 