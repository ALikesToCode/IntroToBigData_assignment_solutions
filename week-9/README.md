# PyTorch Streaming Image Classifier - Direct Conversion of Lecture 10

## Overview

This implementation provides a **direct conversion** of the batch image classification notebook from **Lecture 10** to a real-time streaming model using Spark Structured Streaming. It maintains the exact same PyTorch approach, model architecture, and data processing logic as the original notebook while adapting it for continuous streaming processing.

## Original vs Streaming Comparison

### Original Lecture 10 Notebook (lecture_10_dl.py)
```python
# Batch processing
images = spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.jpg")
    .load(data_dir)

# Process all at once
predictions = df.withColumn("prediction", mobilenet_v2_udf(col("content")))
predictions.select(col("label"), col("prediction")).show(5)
```

### Streaming Version (pytorch_streaming_classifier.py)
```python
# Real-time streaming
stream_df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", servers)
    .option("subscribe", topic)
    .load()

# Continuous processing
predictions_df = processed_df.withColumn(
    "prediction", 
    mobilenet_v2_udf(col("content"))
)

# Streaming output
predictions_df.writeStream
    .outputMode("append")
    .format("console")
    .start()
```

## Key Features Preserved from Original

1. **Same PyTorch Model**: Uses `models.mobilenet_v2(pretrained=True)`
2. **Same Preprocessing**: Identical ImageNet normalization pipeline
3. **Same UDF Structure**: Maintains `imagenet_model_udf` pattern
4. **Same Label Extraction**: Uses `regexp_extract` for flower categories
5. **Same Output Format**: Maintains `class`, `desc`, `score` structure

## Implementation Files

### Core Implementation
- **`pytorch_streaming_classifier.py`** - Main streaming classifier (direct conversion)
- **`flower_kafka_producer.py`** - Kafka producer maintaining flower dataset structure
- **`deploy_pytorch_streaming_gcp.py`** - GCP deployment script
- **`test_pytorch_streaming.py`** - Comprehensive test suite

### Deployment Scripts
- **`run_pytorch_streaming.sh`** - Local execution script
- **Configuration files for GCP deployment**

## Prerequisites

### Local Development
```bash
# Install PyTorch (CPU version)
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu

# Install other dependencies
pip install pyspark pandas pillow kafka-python google-cloud-storage
```

### GCP Deployment
- Google Cloud Project with billing enabled
- Required APIs: Compute Engine, Dataproc, Storage, Pub/Sub, BigQuery
- `gcloud` CLI configured and authenticated

## Quick Start

### 1. Local Testing
```bash
cd week-9

# Test the implementation
source venv/bin/activate
pip install torch torchvision pillow pyspark pandas
python test_pytorch_streaming.py

# Run file-based streaming
python pytorch_streaming_classifier.py \
    --source file \
    --input-path /path/to/flower_photos \
    --checkpoint /tmp/checkpoint \
    --trigger-seconds 10
```

### 2. GCP Deployment (Complete)
```bash
# Deploy entire pipeline to GCP
python deploy_pytorch_streaming_gcp.py \
    --project-id YOUR_PROJECT_ID \
    --region us-central1

# This automatically:
# - Downloads flower dataset to GCS
# - Creates Dataproc cluster with PyTorch
# - Sets up streaming infrastructure
# - Deploys the streaming job
```

### 3. Stream Images via Kafka
```bash
# Start Kafka producer (maintains flower structure)
python flower_kafka_producer.py \
    --kafka-servers CLUSTER_IP:9092 \
    --topic flower-images \
    --flower-dir /path/to/flower_photos \
    --rate 2.0
```

## Streaming Sources Supported

### 1. File Monitoring
Monitors a directory for new `.jpg` files (like original notebook's `binaryFile`):
```python
stream_df = create_file_stream(spark, "gs://bucket/flower_photos/")
```

### 2. Kafka Streaming  
Real-time image stream with JSON messages:
```json
{
    "image_id": "img_000001",
    "path": "flower_photos/roses/image.jpg",
    "label": "roses",
    "image_data": "base64_encoded_jpeg",
    "timestamp": "2024-08-10T10:00:00"
}
```

### 3. Pub/Sub (GCS Notifications)
Triggered by new images uploaded to GCS bucket.

## Model Options (Same as Original)

### Pre-trained Models
```python
# MobileNetV2 (default - same as notebook)
--model mobilenet_v2

# ResNet50 
--model resnet50

# VGG16
--model vgg16
```

### Custom Models
Load your own PyTorch models from GCS (maintains same caching approach).

## Output Format (Identical to Original)

The streaming output maintains the exact same structure as the original notebook:

```python
# Schema matches original notebook
prediction = {
    "class": "123",           # ImageNet class ID
    "desc": "Egyptian_cat",   # Human-readable description  
    "score": 0.85            # Confidence score
}

# Final output
result = {
    "image_path": "flower_photos/roses/image1.jpg",
    "label": "roses",                    # Extracted from path
    "predicted_class": "123",           # Model prediction
    "predicted_description": "Egyptian_cat",
    "confidence": 0.85,
    "processed_at": timestamp
}
```

## GCP Architecture

```
[Flower Dataset] → [GCS Bucket] → [Pub/Sub Topic]
                                       ↓
[Kafka Producer] → [Kafka Topic] → [Dataproc Cluster]
                                       ↓
                                 [PyTorch Model]
                                       ↓
                              [BigQuery Table]
                              [GCS Output]
                              [Console Logs]
```

## Performance Optimizations

### Model Caching (Enhanced from Original)
```python
# Executor-level model caching
_EXECUTOR_STATE = {
    "model": None,
    "model_lock": threading.Lock(),
    "device": None  # CPU/GPU detection
}
```

### Streaming Configurations
```python
# Optimized for continuous processing
.config("spark.streaming.stopGracefullyOnShutdown", "true")
.config("spark.sql.streaming.metricsEnabled", "true")
.config("spark.sql.adaptive.enabled", "true")
```

## Monitoring and Debugging

### Check Streaming Status
```bash
# View Spark UI
gcloud compute ssh cluster-m --zone=us-central1-b -- -L 4040:localhost:4040

# Check streaming queries
http://localhost:4040/StreamingQuery/
```

### Query Results
```sql
-- BigQuery results (same structure as notebook output)
SELECT 
    image_path,
    label,
    predicted_description,
    confidence,
    processed_at
FROM `project.pytorch_streaming.flower_predictions`
ORDER BY processed_at DESC
LIMIT 10;
```

### Monitor Kafka
```bash
# SSH to cluster and check topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Monitor messages  
/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flower-images \
    --from-beginning
```

## Troubleshooting

### Common Issues

1. **PyTorch Model Loading**
   ```bash
   # Ensure PyTorch is installed on all workers
   pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
   ```

2. **Memory Issues**
   ```python
   # Increase executor memory
   .config("spark.executor.memory", "4g")
   .config("spark.driver.memory", "4g")
   ```

3. **Streaming Checkpoint Issues**
   ```bash
   # Clean checkpoint directory if schema changes
   gcloud storage rm -r gs://bucket/checkpoints/
   ```

## Testing

### Run Test Suite
```bash
python test_pytorch_streaming.py
```

### Manual Verification
```bash
# Test with single image
python -c "
from pytorch_streaming_classifier import *
import torch
from torchvision import models

# Load model (same as notebook)
model = models.mobilenet_v2(pretrained=True)
print('Model loaded successfully')
"
```

## Cleanup

### Local Cleanup
```bash
rm -rf /tmp/pytorch-streaming-*
```

### GCP Cleanup
```bash
python deploy_pytorch_streaming_gcp.py --cleanup
```

## Comparison: Batch vs Streaming

| Aspect | Original Batch | Streaming Version |
|--------|----------------|-------------------|
| Data Loading | `spark.read.format("binaryFile")` | `spark.readStream.format("kafka/file")` |
| Processing | One-time batch | Continuous micro-batches |
| Output | `.show()`, `.write()` | `.writeStream()` |
| Model Loading | Once per job | Cached per executor |
| Throughput | All data at once | Configurable intervals |
| Latency | High (wait for all) | Low (real-time) |
| Fault Tolerance | Job restart | Checkpointing |

## Success Metrics

### Functional Requirements ✅
- [x] Direct conversion of Lecture 10 notebook
- [x] Same PyTorch MobileNetV2 model  
- [x] Same preprocessing pipeline
- [x] Same UDF structure and output format
- [x] Flower dataset compatibility

### Technical Requirements ✅  
- [x] Multiple streaming sources (File, Kafka, Pub/Sub)
- [x] GCP deployment automation
- [x] Real-time processing with low latency
- [x] Fault tolerance with checkpointing
- [x] Scalable architecture

### Performance Requirements ✅
- [x] Model caching for efficiency
- [x] Configurable processing intervals
- [x] Memory optimization
- [x] Monitoring and debugging tools

## Conclusion

This implementation successfully converts the batch image classification from Lecture 10 to a production-ready streaming system while maintaining complete fidelity to the original approach. The streaming version provides:

- **Real-time processing** of flower images
- **Identical model behavior** and output format  
- **Production scalability** with GCP deployment
- **Multiple ingestion sources** for flexibility
- **Comprehensive monitoring** and debugging capabilities

The system is ready for production deployment and can process thousands of images per hour while maintaining the same classification quality as the original notebook.