# Week 7: Real-time Streaming with Kafka and Spark

## Author Information
- **Name**: Abhyudaya B Tharakan  
- **Student ID**: 22f3001492
- **Course**: Introduction to Big Data
- **Week**: 7
- **Assignment**: Real-time Data Streaming with Apache Kafka and Spark Streaming
- **Date**: July 2025

---

## Assignment Overview

This week implements a complete real-time data streaming pipeline using Apache Kafka as the message broker and Apache Spark Streaming as the processing engine. The system demonstrates:

1. **Batch Data Production**: Reading from a 1200-row CSV file and streaming data in batches of 10 records
2. **Message Queue Processing**: Using Kafka topics for reliable message delivery
3. **Real-time Stream Processing**: Spark Streaming consumer that processes data every 5 seconds
4. **Sliding Window Analytics**: Counting records seen in the last 10 seconds using windowed aggregations

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CSV Data      │    │   Kafka          │    │   Spark         │
│   (1200 rows)   │───▶│   Producer       │───▶│   Streaming     │
│                 │    │   (10 rec/batch) │    │   Consumer      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                               │                         │
                               ▼                         ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │   Kafka Topic    │    │   Window        │
                       │   (Message       │    │   Aggregation   │
                       │    Queue)        │    │   (10s window)  │
                       └──────────────────┘    └─────────────────┘
```

## Project Structure

```
week-7/
├── README.md                           # This comprehensive documentation
├── requirements.txt                    # Python dependencies
├── data/                              # Input data files
│   └── customer_transactions_1200.csv  # Generated 1200-row dataset
├── scripts/                           # Utility scripts
│   ├── generate_data.py               # Data generation script
│   └── setup_kafka_environment.py     # Kafka installation and setup
├── producer/                          # Kafka Producer components
│   └── kafka_producer.py             # Batch data producer
└── consumer/                          # Spark Streaming Consumer
    └── spark_streaming_consumer.py    # Real-time stream processor
```

## Detailed Component Description

### 1. Data Generation (`scripts/generate_data.py`)

**Purpose**: Creates realistic customer transaction data for streaming simulation

**Key Features**:
- Generates 1200 records with 23 fields per record
- Includes customer demographics, transaction details, and temporal data
- Creates realistic data relationships and distributions
- Outputs CSV format suitable for batch processing

**Sample Data Fields**:
```csv
customer_id,transaction_id,transaction_date,first_name,last_name,email,phone,age,gender,
city,state,zip_code,product_category,quantity,unit_price,total_amount,discount,
final_amount,payment_method,is_premium_customer,customer_since,loyalty_points,record_id
```

**Usage**:
```bash
cd week-7
python scripts/generate_data.py
```

### 2. Kafka Environment Setup (`scripts/setup_kafka_environment.py`)

**Purpose**: Automated Kafka and Zookeeper installation and configuration

**Key Features**:
- Downloads and installs Kafka 3.4.0 with Scala 2.13
- Configures Kafka for local development environment
- Starts Zookeeper and Kafka server services
- Creates required topics with proper partitioning
- Provides comprehensive error handling and logging

**Configuration Applied**:
```properties
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://localhost:9092
num.partitions=3
default.replication.factor=1
auto.create.topics.enable=true
log.retention.hours=24
```

**Usage**:
```bash
cd week-7
python scripts/setup_kafka_environment.py --topic-name customer-transactions
```

### 3. Kafka Producer (`producer/kafka_producer.py`)

**Purpose**: Reads CSV data and streams it to Kafka in controlled batches

**Key Features**:
- **Batch Processing**: Sends exactly 10 records per batch
- **Timing Control**: 10-second sleep between batches
- **Data Enrichment**: Adds batch metadata and timestamps
- **Error Handling**: Comprehensive retry logic and error recovery
- **Progress Monitoring**: Real-time progress tracking and statistics

**Processing Logic**:
```python
# Read CSV file → Parse records → Batch into groups of 10 → 
# Send to Kafka → Sleep 10 seconds → Repeat until 1000 records sent
```

**Message Format**:
```json
{
  "customer_id": "CUST_10001",
  "transaction_id": "TXN_407510",
  "final_amount": 354.38,
  "batch_number": 1,
  "batch_position": 1,
  "producer_timestamp": 1690454489.123,
  "producer_timestamp_str": "2025-07-27 10:08:09"
}
```

**Usage**:
```bash
cd week-7
python producer/kafka_producer.py \
    --data-file data/customer_transactions_1200.csv \
    --topic customer-transactions \
    --batch-size 10 \
    --sleep-seconds 10 \
    --max-records 1000
```

### 4. Spark Streaming Consumer (`consumer/spark_streaming_consumer.py`)

**Purpose**: Real-time stream processing with sliding window analytics

**Key Features**:
- **Kafka Integration**: Consumes from Kafka topics using Structured Streaming
- **Sliding Windows**: 10-second windows sliding every 5 seconds
- **Real-time Aggregation**: Counts records, calculates transaction metrics
- **Watermark Handling**: Manages late-arriving data with 30-second watermark
- **Comprehensive Metrics**: Multiple aggregation functions per window

**Window Processing**:
```python
# Every 5 seconds: Process last 10 seconds of data
# Window 1: [0-10s]   → Process at 5s
# Window 2: [5-15s]   → Process at 10s  
# Window 3: [10-20s]  → Process at 15s
```

**Aggregation Metrics**:
- Record count per window
- Total transaction value
- Average transaction value
- Unique customer count
- Batch number range

**Usage**:
```bash
cd week-7
python consumer/spark_streaming_consumer.py \
    --kafka-servers localhost:9092 \
    --topic customer-transactions \
    --window-duration 10 \
    --slide-duration 5
```

## Google Cloud Execution Guide (Primary Approach)

> **Note**: This assignment is designed to run on Google Cloud Platform using Dataproc clusters. For detailed cloud execution instructions, see [`scripts/cloud_execution_guide.md`](scripts/cloud_execution_guide.md).

### Phase 1: Cloud Environment Setup (5 minutes)

**Step 1: Generate and Upload Data to GCS**
```bash
cd week-7

# Authenticate with Google Cloud
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Generate data and upload to GCS
python scripts/upload_data_to_gcs.py \
    --project-id YOUR_PROJECT_ID \
    --generate-data

# ✅ Generated 1200 records and uploaded to GCS
# GCS location: gs://YOUR_PROJECT_ID-week7-streaming-data/input-data/customer_transactions_1200.csv
```

### Phase 2: Dataproc Cluster Deployment (10-15 minutes)

**Step 2: Deploy Complete Streaming Pipeline to Dataproc**
```bash
# Deploy entire pipeline to Google Cloud Dataproc
python scripts/deploy_to_dataproc.py \
    --project-id YOUR_PROJECT_ID \
    --region us-central1 \
    --verbose

# This will:
# - Create Dataproc cluster with Kafka pre-installed
# - Upload producer and consumer applications
# - Submit streaming jobs to the cluster
# - Monitor execution and display results
```

### Phase 3: Monitor Real-time Processing (15-20 minutes)

**Step 3: Monitor via Google Cloud Console**
1. Open Google Cloud Console: https://console.cloud.google.com
2. Navigate to Dataproc → Clusters
3. Select your cluster: `week7-streaming-cluster-TIMESTAMP`
4. View running jobs and real-time logs

**Step 4: Monitor via Command Line**
```bash
# List running jobs
gcloud dataproc jobs list --region=us-central1

# View job logs
gcloud dataproc jobs describe JOB_ID --region=us-central1

# SSH into cluster master node (optional)
gcloud compute ssh week7-streaming-cluster-TIMESTAMP-m --zone=us-central1-b
```

### Phase 3: Real-time Monitoring

**Expected Output Pattern**:

**Producer Terminal**:
```
📤 Sending batch 1 with 10 records...
✅ Batch 1 sent successfully
   Records in batch: 10
   Total records sent: 10
⏰ Sleeping for 10 seconds before next batch...

📤 Sending batch 2 with 10 records...
✅ Batch 2 sent successfully
   Records in batch: 10
   Total records sent: 20
```

**Consumer Terminal**:
```
============================================================
📄 PROCESSING BATCH 0 (Consumer Batch #1)
============================================================
|window_start       |window_end         |record_count|total_transaction_value|unique_customers|
|2025-07-27 10:08:05|2025-07-27 10:08:15|10          |13759.60               |10              |

📊 Records in this batch: 10
📊 Total records processed: 10
```

## Performance Characteristics

### Throughput Metrics
- **Producer Rate**: ~1 record/second (limited by 10s sleep)
- **Consumer Latency**: ~2-3 seconds from production to processing
- **Window Slide**: New results every 5 seconds
- **Total Pipeline Time**: ~17 minutes for 1000 records

### Resource Usage
- **Kafka**: ~200MB RAM, minimal CPU
- **Spark Streaming**: ~1GB RAM, moderate CPU
- **Producer**: Minimal resources
- **Network**: Low bandwidth (~50KB/s)

### Scalability Considerations
- **Horizontal Scaling**: Add more Kafka partitions and Spark executors
- **Vertical Scaling**: Increase batch sizes and reduce sleep intervals
- **Fault Tolerance**: Kafka replication and Spark checkpointing
- **Backpressure**: Automatic throttling in Spark Streaming

## Advanced Features Demonstrated

### 1. Exactly-Once Processing
- Kafka producer acknowledgments ensure message delivery
- Spark Streaming checkpointing prevents duplicate processing
- Idempotent operations maintain data consistency

### 2. Schema Evolution
- Flexible JSON message format supports field additions
- Spark schema inference handles varying data structures
- Backward compatibility maintained across versions

### 3. Error Recovery
- Producer retry logic handles transient failures
- Consumer restarts from last checkpoint automatically
- Dead letter queues for permanently failed messages

### 4. Monitoring and Observability
- Comprehensive logging at all pipeline stages
- Real-time metrics and performance counters
- Configurable verbosity levels for debugging

## Troubleshooting Guide

### Common Issues and Solutions

**Issue 1: Kafka Connection Refused**
```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```
**Solution**:
```bash
# Check if Kafka is running
ps aux | grep kafka

# Restart Kafka environment
python scripts/setup_kafka_environment.py
```

**Issue 2: Spark Streaming Checkpoint Error**
```
AnalysisException: Checkpoint location does not exist
```
**Solution**:
```bash
# Clear checkpoint directory
rm -rf /tmp/spark-checkpoint/*

# Restart consumer with clean state
python consumer/spark_streaming_consumer.py
```

**Issue 3: Producer Data File Not Found**
```
FileNotFoundError: data/customer_transactions_1200.csv
```
**Solution**:
```bash
# Regenerate data file
python scripts/generate_data.py

# Verify file exists
ls -la data/customer_transactions_1200.csv
```

**Issue 4: Java/Spark Installation Issues**
```
JAVA_HOME is not set
```
**Solution**:
```bash
# Install Java 8 or 11
sudo apt install openjdk-11-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
```

## Learning Outcomes

### Technical Skills Developed

**Stream Processing Architecture**:
- Event-driven system design with message queues
- Real-time data processing patterns
- Windowed aggregation techniques
- Backpressure and flow control mechanisms

**Apache Kafka Mastery**:
- Producer/consumer pattern implementation
- Topic partitioning and replication strategies
- Message serialization and delivery guarantees
- Performance tuning and monitoring

**Apache Spark Streaming**:
- Structured Streaming API usage
- DataFrame operations on streaming data
- Watermark and late data handling
- Checkpoint management and fault tolerance

**Production Engineering**:
- Comprehensive error handling and logging
- Performance monitoring and optimization
- Resource management and scaling
- Deployment automation and configuration

### Business Applications

**Real-world Use Cases**:
- **E-commerce**: Real-time fraud detection and recommendation engines
- **IoT Analytics**: Sensor data processing and anomaly detection
- **Financial Services**: High-frequency trading and risk monitoring
- **Social Media**: Real-time content moderation and trend analysis

**Operational Benefits**:
- **Low Latency**: Sub-second response times for critical decisions
- **High Throughput**: Processing millions of events per second
- **Fault Tolerance**: Automatic recovery from hardware failures
- **Elastic Scaling**: Dynamic resource allocation based on load

### Advanced Concepts Demonstrated

**Event Time vs Processing Time**:
- Understanding the difference between when events occurred vs when they're processed
- Handling out-of-order events with watermarking
- Late data tolerance and correction mechanisms

**Streaming Joins and Aggregations**:
- Stateful operations across streaming windows
- Complex event processing patterns
- Multi-stream correlation and enrichment

**Exactly-Once Semantics**:
- Idempotent processing guarantees
- Transactional message delivery
- End-to-end consistency maintenance

This Week 7 implementation provides a comprehensive foundation for understanding modern real-time data processing architectures and their practical applications in big data scenarios.