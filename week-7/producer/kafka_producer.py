#!/usr/bin/env python3
"""
Kafka Producer for Week 7 Real-time Streaming Assignment
Course: Introduction to Big Data - Week 7

This producer reads customer transaction data from a CSV file and sends it to Kafka
in batches of 10 records with 10-second intervals between batches.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import csv
import json
import time
import logging
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('kafka_producer.log')
    ]
)
logger = logging.getLogger(__name__)

class CustomerTransactionProducer:
    """
    Kafka producer for streaming customer transaction data in batches.
    """
    
    def __init__(self, bootstrap_servers='localhost:9092', topic_name='customer-transactions'):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic_name: Kafka topic name
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.records_sent = 0
        self.batches_sent = 0
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,   # Retry failed sends
                batch_size=16384,  # Batch size in bytes
                linger_ms=10,      # Wait up to 10ms to batch messages
                buffer_memory=33554432,  # 32MB buffer
                compression_type='gzip'  # Compress messages
            )
            logger.info(f"✅ Kafka producer initialized")
            logger.info(f"   Bootstrap servers: {self.bootstrap_servers}")
            logger.info(f"   Topic: {self.topic_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise
    
    def read_csv_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Read customer transaction data from CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List of transaction records
        """
        try:
            logger.info(f"📖 Reading data from: {file_path}")
            
            data = []
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Convert numeric fields to appropriate types
                    try:
                        row['age'] = int(row['age'])
                        row['zip_code'] = int(row['zip_code'])
                        row['quantity'] = int(row['quantity'])
                        row['unit_price'] = float(row['unit_price'])
                        row['total_amount'] = float(row['total_amount'])
                        row['discount'] = float(row['discount'])
                        row['final_amount'] = float(row['final_amount'])
                        row['loyalty_points'] = int(row['loyalty_points'])
                        row['record_id'] = int(row['record_id'])
                        row['is_premium_customer'] = row['is_premium_customer'].lower() == 'true'
                    except ValueError as e:
                        logger.warning(f"Data conversion warning for record {row.get('record_id', 'unknown')}: {e}")
                        continue
                    
                    data.append(row)
            
            logger.info(f"✅ Successfully read {len(data)} records from CSV")
            return data
            
        except FileNotFoundError:
            logger.error(f"❌ File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"❌ Error reading CSV file: {e}")
            raise
    
    def send_batch(self, batch: List[Dict[str, Any]], batch_number: int) -> bool:
        """
        Send a batch of records to Kafka topic.
        
        Args:
            batch: List of records to send
            batch_number: Batch sequence number
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"📤 Sending batch {batch_number} with {len(batch)} records...")
            
            futures = []
            for i, record in enumerate(batch):
                # Use customer_id as the message key for partitioning
                key = record.get('customer_id')
                
                # Add batch metadata
                enriched_record = {
                    **record,
                    'batch_number': batch_number,
                    'batch_position': i + 1,
                    'producer_timestamp': time.time(),
                    'producer_timestamp_str': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                }
                
                # Send to Kafka
                future = self.producer.send(
                    topic=self.topic_name,
                    key=key,
                    value=enriched_record
                )
                futures.append(future)
                
                logger.debug(f"   Queued record {record['record_id']} (customer: {key})")
            
            # Wait for all messages in the batch to be sent
            for future in futures:
                try:
                    record_metadata = future.get(timeout=30)
                    logger.debug(f"   Message delivered to partition {record_metadata.partition} "
                               f"at offset {record_metadata.offset}")
                except KafkaError as e:
                    logger.error(f"   Failed to send message: {e}")
                    return False
            
            self.records_sent += len(batch)
            self.batches_sent += 1
            
            logger.info(f"✅ Batch {batch_number} sent successfully")
            logger.info(f"   Records in batch: {len(batch)}")
            logger.info(f"   Total records sent: {self.records_sent}")
            logger.info(f"   Total batches sent: {self.batches_sent}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending batch {batch_number}: {e}")
            return False
    
    def stream_data(self, data: List[Dict[str, Any]], batch_size: int = 10, 
                   sleep_seconds: int = 10, max_records: int = 1000) -> None:
        """
        Stream data to Kafka in batches with specified intervals.
        
        Args:
            data: List of records to stream
            batch_size: Number of records per batch
            sleep_seconds: Sleep time between batches
            max_records: Maximum number of records to send
        """
        try:
            logger.info(f"🚀 Starting data streaming...")
            logger.info(f"   Total available records: {len(data)}")
            logger.info(f"   Batch size: {batch_size}")
            logger.info(f"   Sleep between batches: {sleep_seconds} seconds")
            logger.info(f"   Max records to send: {max_records}")
            
            # Limit data to max_records
            data_to_send = data[:max_records]
            logger.info(f"   Records to send: {len(data_to_send)}")
            
            # Calculate expected batches
            expected_batches = (len(data_to_send) + batch_size - 1) // batch_size
            logger.info(f"   Expected batches: {expected_batches}")
            
            start_time = time.time()
            
            # Send data in batches
            for i in range(0, len(data_to_send), batch_size):
                batch_number = (i // batch_size) + 1
                batch = data_to_send[i:i + batch_size]
                
                logger.info(f"\n{'='*60}")
                logger.info(f"BATCH {batch_number}/{expected_batches}")
                logger.info(f"{'='*60}")
                
                # Send the batch
                success = self.send_batch(batch, batch_number)
                
                if not success:
                    logger.error(f"❌ Failed to send batch {batch_number}, stopping...")
                    break
                
                # Sleep between batches (except for the last batch)
                if i + batch_size < len(data_to_send):
                    logger.info(f"⏰ Sleeping for {sleep_seconds} seconds before next batch...")
                    
                    # Progress indicator during sleep
                    for remaining in range(sleep_seconds, 0, -1):
                        print(f"\r   Next batch in {remaining} seconds...", end='', flush=True)
                        time.sleep(1)
                    print()  # New line after countdown
                
                # Break if we've sent enough records
                if self.records_sent >= max_records:
                    logger.info(f"✅ Reached maximum records limit ({max_records})")
                    break
            
            # Final statistics
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"\n{'='*60}")
            logger.info(f"STREAMING COMPLETED")
            logger.info(f"{'='*60}")
            logger.info(f"   Total records sent: {self.records_sent}")
            logger.info(f"   Total batches sent: {self.batches_sent}")
            logger.info(f"   Total duration: {duration:.2f} seconds")
            logger.info(f"   Average records per second: {self.records_sent / duration:.2f}")
            
            if self.records_sent > 0:
                logger.info(f"   Average batch processing time: {duration / self.batches_sent:.2f} seconds")
            
        except KeyboardInterrupt:
            logger.info(f"\n⚠️ Streaming interrupted by user")
            logger.info(f"   Records sent before interruption: {self.records_sent}")
        except Exception as e:
            logger.error(f"❌ Error during streaming: {e}")
            raise
        finally:
            self.close()
    
    def close(self):
        """
        Close the Kafka producer and clean up resources.
        """
        try:
            logger.info("🔒 Closing Kafka producer...")
            self.producer.flush(timeout=30)  # Wait for pending messages
            self.producer.close(timeout=30)  # Close the producer
            logger.info("✅ Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"❌ Error closing Kafka producer: {e}")

def main():
    """
    Main function to run the Kafka producer.
    """
    parser = argparse.ArgumentParser(description='Kafka Producer for Customer Transaction Streaming')
    parser.add_argument('--data-file', required=True,
                       help='Path to the CSV data file')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='customer-transactions',
                       help='Kafka topic name (default: customer-transactions)')
    parser.add_argument('--batch-size', type=int, default=10,
                       help='Number of records per batch (default: 10)')
    parser.add_argument('--sleep-seconds', type=int, default=10,
                       help='Sleep time between batches in seconds (default: 10)')
    parser.add_argument('--max-records', type=int, default=1000,
                       help='Maximum number of records to send (default: 1000)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Validate data file exists
        data_file = Path(args.data_file)
        if not data_file.exists():
            logger.error(f"❌ Data file not found: {data_file}")
            sys.exit(1)
        
        # Create producer
        producer = CustomerTransactionProducer(
            bootstrap_servers=args.bootstrap_servers,
            topic_name=args.topic
        )
        
        # Read data
        data = producer.read_csv_data(args.data_file)
        
        if not data:
            logger.error("❌ No data found in the file")
            sys.exit(1)
        
        # Start streaming
        producer.stream_data(
            data=data,
            batch_size=args.batch_size,
            sleep_seconds=args.sleep_seconds,
            max_records=args.max_records
        )
        
    except KeyboardInterrupt:
        logger.info("👋 Producer stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()