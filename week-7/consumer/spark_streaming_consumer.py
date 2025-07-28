#!/usr/bin/env python3
"""
Spark Streaming Consumer for Week 7 Real-time Streaming Assignment
Course: Introduction to Big Data - Week 7

This Spark Streaming application reads from Kafka topic every 5 seconds and 
calculates the count of rows seen in the last 10 seconds using sliding windows.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import sys
import time
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CustomerTransactionStreamProcessor:
    """
    Spark Streaming processor for real-time customer transaction analysis.
    """
    
    def __init__(self, kafka_servers='localhost:9092', topic_name='customer-transactions'):
        """
        Initialize Spark Streaming processor.
        
        Args:
            kafka_servers: Kafka bootstrap servers
            topic_name: Kafka topic to consume from
        """
        self.kafka_servers = kafka_servers
        self.topic_name = topic_name
        self.total_records_processed = 0
        self.batch_count = 0
        
        # Initialize Spark Session
        self.spark = self._create_spark_session()
        
        logger.info(f"✅ Spark Streaming processor initialized")
        logger.info(f"   Kafka servers: {self.kafka_servers}")
        logger.info(f"   Topic: {self.topic_name}")
        logger.info(f"   Spark version: {self.spark.version}")
    
    def _create_spark_session(self):
        """
        Create Spark session with required configurations.
        
        Returns:
            SparkSession object
        """
        try:
            spark = SparkSession.builder \
                .appName("CustomerTransactionStreamProcessor") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
                .getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            return spark
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            raise
    
    def _define_transaction_schema(self):
        """
        Define schema for customer transaction data.
        
        Returns:
            StructType schema
        """
        return StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", IntegerType(), True),
            StructField("product_category", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("final_amount", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("is_premium_customer", BooleanType(), True),
            StructField("customer_since", StringType(), True),
            StructField("loyalty_points", IntegerType(), True),
            StructField("record_id", IntegerType(), True),
            StructField("batch_number", IntegerType(), True),
            StructField("batch_position", IntegerType(), True),
            StructField("producer_timestamp", DoubleType(), True),
            StructField("producer_timestamp_str", StringType(), True)
        ])
    
    def process_stream(self, window_duration_seconds=10, slide_duration_seconds=5):
        """
        Process Kafka stream with sliding window aggregations.
        
        Args:
            window_duration_seconds: Window duration for aggregation
            slide_duration_seconds: Slide interval for windows
        """
        try:
            logger.info(f"🚀 Starting stream processing...")
            logger.info(f"   Window duration: {window_duration_seconds} seconds")
            logger.info(f"   Slide duration: {slide_duration_seconds} seconds")
            
            # Read from Kafka stream
            kafka_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.topic_name) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info("📡 Connected to Kafka stream")
            
            # Parse JSON messages
            transaction_schema = self._define_transaction_schema()
            
            parsed_stream = kafka_stream.select(
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset"),
                from_json(col("value").cast("string"), transaction_schema).alias("data")
            ).select(
                col("kafka_timestamp"),
                col("partition"),
                col("offset"),
                col("data.*")
            )
            
            # Add processing timestamp
            processed_stream = parsed_stream.withColumn(
                "processing_timestamp", 
                current_timestamp()
            ).withColumn(
                "processing_timestamp_str",
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")
            )
            
            logger.info("✅ Stream parsing configured")
            
            # Configure sliding window aggregation
            windowed_counts = processed_stream \
                .withWatermark("processing_timestamp", "30 seconds") \
                .groupBy(
                    window(
                        col("processing_timestamp"),
                        f"{window_duration_seconds} seconds",
                        f"{slide_duration_seconds} seconds"
                    )
                ) \
                .agg(
                    count("*").alias("record_count"),
                    sum("final_amount").alias("total_transaction_value"),
                    avg("final_amount").alias("avg_transaction_value"),
                    countDistinct("customer_id").alias("unique_customers"),
                    max("batch_number").alias("max_batch_number"),
                    min("batch_number").alias("min_batch_number")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("record_count"),
                    col("total_transaction_value"),
                    col("avg_transaction_value"),
                    col("unique_customers"),
                    col("min_batch_number"),
                    col("max_batch_number")
                )
            
            # Output stream query with custom processing
            query = windowed_counts.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime=f"{slide_duration_seconds} seconds") \
                .foreachBatch(self._process_batch) \
                .start()
            
            logger.info("📊 Windowed aggregation query started")
            
            # Also create a simple count query for debugging
            simple_query = processed_stream.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("numRows", 5) \
                .option("truncate", "false") \
                .trigger(processingTime=f"{slide_duration_seconds} seconds") \
                .queryName("simple_records") \
                .start()
            
            logger.info("🔍 Simple record display query started")
            
            # Wait for queries to terminate
            try:
                logger.info("⏳ Waiting for streaming queries to process data...")
                logger.info("   Press Ctrl+C to stop the consumer")
                
                # Monitor queries
                while query.isActive or simple_query.isActive:
                    time.sleep(2)
                    
                    # Print status every 30 seconds
                    if int(time.time()) % 30 == 0:
                        logger.info(f"📈 Status - Batches processed: {self.batch_count}, "
                                  f"Total records: {self.total_records_processed}")
                
            except KeyboardInterrupt:
                logger.info("\n⚠️ Stream processing interrupted by user")
            
            finally:
                logger.info("🛑 Stopping streaming queries...")
                query.stop()
                simple_query.stop()
                logger.info("✅ All queries stopped")
                
        except Exception as e:
            logger.error(f"❌ Error in stream processing: {e}")
            raise
    
    def _process_batch(self, batch_df, batch_id):
        """
        Process each batch of windowed results.
        
        Args:
            batch_df: DataFrame with batch data
            batch_id: Batch identifier
        """
        try:
            batch_count = batch_df.count()
            
            if batch_count > 0:
                self.batch_count += 1
                
                logger.info(f"\n{'='*80}")
                logger.info(f"PROCESSING BATCH {batch_id} (Consumer Batch #{self.batch_count})")
                logger.info(f"{'='*80}")
                logger.info(f"Batch size: {batch_count} windows")
                
                # Show the windowed results
                batch_df.show(truncate=False)
                
                # Calculate total records in this batch
                batch_records = batch_df.agg(sum("record_count")).collect()[0][0]
                if batch_records:
                    self.total_records_processed += batch_records
                    logger.info(f"📊 Records in this batch: {batch_records}")
                    logger.info(f"📊 Total records processed: {self.total_records_processed}")
                
                # Show detailed statistics
                logger.info(f"\n📈 BATCH STATISTICS:")
                for row in batch_df.collect():
                    window_start = row['window_start']
                    window_end = row['window_end']
                    record_count = row['record_count']
                    total_value = row['total_transaction_value']
                    avg_value = row['avg_transaction_value']
                    unique_customers = row['unique_customers']
                    min_batch = row['min_batch_number']
                    max_batch = row['max_batch_number']
                    
                    logger.info(f"   Window: {window_start} to {window_end}")
                    logger.info(f"   Records: {record_count}")
                    logger.info(f"   Unique customers: {unique_customers}")
                    logger.info(f"   Total transaction value: ${total_value:,.2f}")
                    logger.info(f"   Average transaction value: ${avg_value:.2f}")
                    logger.info(f"   Batch range: {min_batch} to {max_batch}")
                    logger.info(f"   " + "-" * 50)
                
                logger.info(f"✅ Batch {batch_id} processed successfully")
            else:
                logger.debug(f"📭 Empty batch {batch_id}")
                
        except Exception as e:
            logger.error(f"❌ Error processing batch {batch_id}: {e}")
    
    def stop(self):
        """
        Stop the Spark session.
        """
        try:
            logger.info("🔒 Stopping Spark session...")
            self.spark.stop()
            logger.info("✅ Spark session stopped")
        except Exception as e:
            logger.error(f"❌ Error stopping Spark session: {e}")

def main():
    """
    Main function to run the Spark Streaming consumer.
    """
    parser = argparse.ArgumentParser(description='Spark Streaming Consumer for Customer Transactions')
    parser.add_argument('--kafka-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='customer-transactions',
                       help='Kafka topic name (default: customer-transactions)')
    parser.add_argument('--window-duration', type=int, default=10,
                       help='Window duration in seconds (default: 10)')
    parser.add_argument('--slide-duration', type=int, default=5,
                       help='Slide duration in seconds (default: 5)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    processor = None
    
    try:
        # Create processor
        processor = CustomerTransactionStreamProcessor(
            kafka_servers=args.kafka_servers,
            topic_name=args.topic
        )
        
        # Start processing
        processor.process_stream(
            window_duration_seconds=args.window_duration,
            slide_duration_seconds=args.slide_duration
        )
        
    except KeyboardInterrupt:
        logger.info("👋 Consumer stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)
    finally:
        if processor:
            processor.stop()

if __name__ == '__main__':
    main()