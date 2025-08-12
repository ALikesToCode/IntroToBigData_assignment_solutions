# Week 9: Real-Time Image Classification with Spark Streaming

## Course Information
- **Course**: Introduction to Big Data
- **Week**: 9
- **Topic**: Converting Batch Image Classification to Real-Time Streaming
- **Author**: Abhyudaya B Tharakan 22f3001492
- **Date**: August 2025

## Overview

This week's assignment converts the batch image classification use case from the class into a comprehensive real-time execution model using Spark Streaming. The implementation demonstrates modern streaming architectures with multiple ingestion sources, scalable processing, and real-time ML inference.

## Architecture Components

### 1. **Data Ingestion Sources**
- **Kafka Topics**: Real-time image streams with base64-encoded images
- **Cloud Storage + Pub/Sub**: GCS object notifications for uploaded images
- **Simulated Streams**: Image generator for testing and demonstration

### 2. **Processing Layer**
- **Apache Spark Structured Streaming**: Core streaming engine
- **TensorFlow Integration**: Real-time ML inference
- **Multiple Model Support**: MobileNetV2, ResNet50, VGG16, or custom models

### 3. **Output Sinks**
- **BigQuery**: Structured storage for predictions
- **Kafka**: Downstream processing pipeline
- **Cloud Storage**: JSON outputs for batch analysis
- **Console**: Real-time monitoring

## Key Features

### Enhanced Streaming Capabilities
- **Multi-source ingestion**: Simultaneously process from Kafka and Pub/Sub
- **Model caching**: Executor-level model caching for performance
- **Performance metrics**: Real-time tracking of processing times and success rates
- **Error handling**: Comprehensive error handling and logging
- **Scalable architecture**: Designed for horizontal scaling on Dataproc

### Machine Learning Features
- **Pre-trained models**: ImageNet-trained models (MobileNetV2, ResNet50, VGG16)
- **Custom models**: Support for custom TensorFlow SavedModel or H5 models
- **Preprocessing pipeline**: Model-specific image preprocessing
- **Confidence filtering**: Configurable confidence thresholds

## Project Structure

```
week-9/
├── streaming_image_classifier.py       # Core streaming implementation
├── enhanced_streaming_image_classifier.py  # Enhanced version with Kafka
├── image_stream_simulator.py          # Generate synthetic image streams
├── test_enhanced_streaming.py         # Comprehensive local testing
├── local_smoke_test.py                # Quick batch testing
├── deploy_streaming_pipeline.py       # Complete deployment automation
├── requirements.txt                   # Python dependencies
├── scripts/
│   ├── create_dataproc_cluster_tf.sh # Cluster creation script
│   └── submit_image_stream.sh        # Job submission script
├── README.md                          # This file
├── README_IMAGE_STREAMING.md          # Original streaming documentation
└── STREAMING_ARCHITECTURE_REPORT.md   # Detailed architecture report
```

## Setup Instructions

### Prerequisites
1. **Google Cloud Platform Account** with billing enabled
2. **Required APIs enabled**:
   - Compute Engine API
   - Dataproc API
   - Cloud Storage API
   - Pub/Sub API
   - BigQuery API
3. **gcloud CLI** installed and authenticated
4. **Python 3.7+** for local testing

### Local Testing

1. **Install dependencies**:
```bash
cd week-9
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Run comprehensive tests**:
```bash
python test_enhanced_streaming.py --test all
```

3. **Test specific components**:
```bash
# Test image generation
python test_enhanced_streaming.py --test image-gen

# Test UDF functionality
python test_enhanced_streaming.py --test udf

# Test streaming operations
python test_enhanced_streaming.py --test dataframe
```

## Cloud Deployment

### Option 1: Automated Deployment (Recommended)

Use the complete deployment script that handles all infrastructure setup:

```bash
# Deploy complete pipeline
python deploy_streaming_pipeline.py \
    --project-id YOUR_PROJECT_ID \
    --region us-central1

# The script will:
# 1. Check prerequisites
# 2. Create GCS bucket with directory structure
# 3. Setup Pub/Sub topics and subscriptions
# 4. Create BigQuery dataset and tables
# 5. Create optimized Dataproc cluster
# 6. Upload application code
# 7. Create Kafka topics
# 8. Deploy streaming job
# 9. Provide simulator instructions
```

### Option 2: Manual Deployment

1. **Create GCS bucket**:
```bash
PROJECT_ID=your-project-id
BUCKET_NAME=${PROJECT_ID}-week9-streaming-data

gcloud storage buckets create gs://${BUCKET_NAME} --location=us-central1
```

2. **Setup Pub/Sub**:
```bash
# Create topic
gcloud pubsub topics create gcs-image-notifications

# Create subscription
gcloud pubsub subscriptions create image-stream-sub \
    --topic=gcs-image-notifications

# Setup bucket notifications
gcloud storage buckets notifications create \
    gs://${BUCKET_NAME}/input-images/ \
    --topic=projects/${PROJECT_ID}/topics/gcs-image-notifications \
    --event-types=OBJECT_FINALIZE
```

3. **Create BigQuery resources**:
```bash
# Create dataset
bq mk --location=us-central1 ml_streaming

# Create table
bq mk --table ml_streaming.image_predictions \
    prediction_id:STRING,label:STRING,confidence:FLOAT64,\
    model_name:STRING,processing_time_ms:INTEGER,\
    image_size_bytes:INTEGER,bucket:STRING,\
    object_name:STRING,processed_at:TIMESTAMP
```

4. **Create Dataproc cluster**:
```bash
bash scripts/create_dataproc_cluster_tf.sh
```

5. **Submit streaming job**:
```bash
gcloud dataproc jobs submit pyspark \
    gs://${BUCKET_NAME}/apps/enhanced_streaming_image_classifier.py \
    --cluster=week9-streaming-cluster \
    --region=us-central1 \
    --packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,\
com.google.cloud.spark:spark-pubsub_2.12:2.4.6,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
    -- \
    --input-source both \
    --kafka-servers week9-streaming-cluster-m:9092 \
    --kafka-topic image-stream \
    --project-id ${PROJECT_ID} \
    --pubsub-subscription image-stream-sub \
    --model-type mobilenet \
    --confidence-threshold 0.1 \
    --checkpoint gs://${BUCKET_NAME}/checkpoints/streaming \
    --output-bq-table ${PROJECT_ID}.ml_streaming.image_predictions \
    --output-gcs-path gs://${BUCKET_NAME}/outputs/predictions
```

## Running the Image Stream Simulator

Generate synthetic images for testing the pipeline:

```bash
# For Kafka streaming
python image_stream_simulator.py \
    --mode kafka \
    --rate 2.0 \
    --duration 300 \
    --kafka-servers MASTER_NODE_IP:9092 \
    --kafka-topic image-stream

# For GCS upload (triggers Pub/Sub)
python image_stream_simulator.py \
    --mode gcs \
    --rate 0.5 \
    --duration 300 \
    --gcs-bucket ${BUCKET_NAME} \
    --gcs-prefix input-images/

# For both simultaneously
python image_stream_simulator.py \
    --mode both \
    --rate 1.0 \
    --duration 300 \
    --kafka-servers MASTER_NODE_IP:9092 \
    --kafka-topic image-stream \
    --gcs-bucket ${BUCKET_NAME}
```

## Monitoring and Validation

### 1. **Monitor Streaming Job**
```bash
# View Spark UI (port forward)
gcloud compute ssh week9-streaming-cluster-m \
    --zone=us-central1-b \
    -- -L 4040:localhost:4040

# Then browse to http://localhost:4040
```

### 2. **Check BigQuery Results**
```sql
-- Query recent predictions
SELECT 
    prediction_id,
    label,
    ROUND(confidence, 3) as confidence,
    model_name,
    processing_time_ms,
    processed_at
FROM `your-project.ml_streaming.image_predictions`
WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY processed_at DESC
LIMIT 100;

-- Aggregated statistics
SELECT 
    model_name,
    COUNT(*) as prediction_count,
    AVG(confidence) as avg_confidence,
    AVG(processing_time_ms) as avg_processing_ms,
    MIN(processed_at) as first_prediction,
    MAX(processed_at) as last_prediction
FROM `your-project.ml_streaming.image_predictions`
GROUP BY model_name;
```

### 3. **Monitor Kafka Topics**
```bash
# SSH to master node
gcloud compute ssh week9-streaming-cluster-m --zone=us-central1-b

# List topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Monitor message flow
/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic image-stream-results \
    --from-beginning \
    --max-messages 10
```

### 4. **Check GCS Outputs**
```bash
# List prediction outputs
gcloud storage ls gs://${BUCKET_NAME}/outputs/predictions/

# Download sample output
gcloud storage cp \
    gs://${BUCKET_NAME}/outputs/predictions/part-00000-*.json \
    sample_predictions.json
```

## Performance Optimization

### Cluster Configuration
- **Master**: n1-standard-4 with 100GB boot disk
- **Workers**: 2x n1-standard-4 with 50GB boot disk each
- **Auto-scaling**: Enabled with max 4 workers
- **Max idle**: 30 minutes
- **Max age**: 3 hours

### Streaming Optimizations
- **Shuffle partitions**: Set to 8 for optimal parallelism
- **Adaptive query execution**: Enabled
- **Checkpoint intervals**: 5 seconds
- **Model caching**: Per-executor caching to avoid repeated downloads

### ML Model Optimizations
- **Batch size**: 1 (real-time processing)
- **Image size**: 224x224 (standard for most models)
- **Preprocessing**: Model-specific optimizations
- **TensorFlow**: Configured for CPU inference

## Troubleshooting

### Common Issues and Solutions

1. **Model loading fails**:
   - Ensure TensorFlow is installed on all workers
   - Check internet connectivity for downloading pre-trained models
   - Verify GCS permissions for custom models

2. **Kafka connection errors**:
   - Verify Kafka is running on master node
   - Check firewall rules allow port 9092
   - Ensure correct bootstrap server address

3. **Pub/Sub subscription not receiving messages**:
   - Verify bucket notifications are configured
   - Check IAM permissions for Pub/Sub
   - Ensure objects are being created in monitored path

4. **BigQuery write failures**:
   - Verify dataset and table exist
   - Check service account has BigQuery Data Editor role
   - Ensure schema matches expected format

5. **Out of memory errors**:
   - Reduce batch size or trigger interval
   - Increase executor memory in Spark configuration
   - Scale cluster by adding more workers

## Cleanup

To avoid ongoing charges, clean up resources when done:

```bash
# Using automated cleanup
python deploy_streaming_pipeline.py --cleanup

# Or manual cleanup
# Delete Dataproc cluster
gcloud dataproc clusters delete week9-streaming-cluster \
    --region=us-central1 --quiet

# Delete Pub/Sub resources
gcloud pubsub subscriptions delete image-stream-sub
gcloud pubsub topics delete gcs-image-notifications

# Delete BigQuery dataset (careful - this deletes all tables!)
bq rm -r -f ml_streaming

# Delete GCS bucket (careful - this deletes all data!)
gcloud storage rm -r gs://${BUCKET_NAME}
```

## Assignment Deliverables

1. **Code Files**:
   - All Python scripts in week-9 directory
   - Configuration and deployment scripts
   - Test scripts and utilities

2. **Documentation**:
   - This README with setup instructions
   - Architecture report (STREAMING_ARCHITECTURE_REPORT.md)
   - Code comments and docstrings

3. **Results**:
   - Screenshots of streaming job running
   - BigQuery query results showing predictions
   - Performance metrics and analysis

## Key Learning Outcomes

- **Streaming Architecture**: Design and implement real-time data pipelines
- **ML Integration**: Deploy ML models in streaming contexts
- **Cloud Services**: Integrate multiple GCP services (Dataproc, Pub/Sub, BigQuery, GCS)
- **Performance Optimization**: Optimize streaming jobs for throughput and latency
- **Production Deployment**: Handle real-world deployment challenges

## Additional Resources

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Google Cloud Dataproc Documentation](https://cloud.google.com/dataproc/docs)
- [TensorFlow Serving](https://www.tensorflow.org/tfx/guide/serving)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

## Support

For questions or issues:
1. Check the troubleshooting section above
2. Review error logs in Dataproc UI
3. Consult course materials and lecture notes
4. Contact course instructor or TA

---

**Note**: Ensure all GCP resources are properly configured and you have necessary permissions before running the deployment. Monitor costs and set up billing alerts to avoid unexpected charges.