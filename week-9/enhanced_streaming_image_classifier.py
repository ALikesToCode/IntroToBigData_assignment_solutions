#!/usr/bin/env python3
"""
Enhanced Real-Time Image Classification with Kafka Streaming
Course: Introduction to Big Data - Week 9
Author: Abhyudaya B Tharakan 22f3001492

This enhanced version converts the batch image classification to a comprehensive
real-time streaming architecture with multiple ingestion sources:
- Kafka topics for real-time image streams
- Cloud Storage notifications via Pub/Sub
- Direct HTTP uploads (simulated)

Features:
- Multiple ML models support (MobileNetV2, ResNet50, custom models)
- Real-time performance metrics and monitoring
- Scalable streaming architecture
- Enhanced error handling and logging
- Multiple output sinks (Console, BigQuery, Kafka, GCS)
"""

import os
import io
import sys
import json
import time
import uuid
import base64
import argparse
import threading
from typing import Optional, Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery


# Global executor-local state for model caching and performance tracking
_EXECUTOR_STATE = {
    "model": None,
    "model_name": None,
    "model_lock": threading.Lock(),
    "gcs_client": None,
    "prediction_count": 0,
    "error_count": 0,
    "total_processing_time": 0.0,
    "last_metrics_log": 0
}


def log_info(msg: str):
    """Enhanced logging with timestamps."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] INFO: {msg}", file=sys.stderr, flush=True)


def log_error(msg: str):
    """Enhanced error logging with timestamps."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] ERROR: {msg}", file=sys.stderr, flush=True)


def _ensure_cloud_clients():
    """Initialize cloud clients on executors."""
    if _EXECUTOR_STATE["gcs_client"] is None:
        try:
            from google.cloud import storage
            _EXECUTOR_STATE["gcs_client"] = storage.Client()
            log_info("GCS client initialized on executor")
        except ImportError:
            log_error("google-cloud-storage not available")
        except Exception as e:
            log_error(f"Failed to initialize GCS client: {e}")
    return _EXECUTOR_STATE["gcs_client"]


def _download_image_from_gcs(bucket: str, object_name: str) -> Optional[bytes]:
    """Download image bytes from Google Cloud Storage."""
    try:
        client = _ensure_cloud_clients()
        if not client:
            return None
        
        bucket_ref = client.bucket(bucket)
        blob = bucket_ref.blob(object_name)
        
        if not blob.exists():
            log_error(f"Object not found: gs://{bucket}/{object_name}")
            return None
            
        return blob.download_as_bytes()
    except Exception as e:
        log_error(f"Failed to download gs://{bucket}/{object_name}: {e}")
        return None


def _download_and_cache_model(model_path: Optional[str], model_type: str = "mobilenet"):
    """Download and cache ML model on executor."""
    if _EXECUTOR_STATE["model"] is not None:
        return _EXECUTOR_STATE["model"], _EXECUTOR_STATE["model_name"]
    
    with _EXECUTOR_STATE["model_lock"]:
        if _EXECUTOR_STATE["model"] is not None:
            return _EXECUTOR_STATE["model"], _EXECUTOR_STATE["model_name"]
        
        try:
            import tensorflow as tf
            
            # Custom model from GCS
            if model_path and model_path.startswith("gs://"):
                log_info(f"Loading custom model from {model_path}")
                bucket_path = model_path[5:]  # Remove gs://
                bucket, model_dir = bucket_path.split("/", 1)
                
                local_model_dir = "/tmp/streaming_model"
                os.makedirs(local_model_dir, exist_ok=True)
                
                client = _ensure_cloud_clients()
                bucket_ref = client.bucket(bucket)
                
                # Download model files
                blobs = list(client.list_blobs(bucket_ref, prefix=model_dir))
                for blob in blobs:
                    local_path = os.path.join(local_model_dir, 
                                            os.path.relpath(blob.name, model_dir))
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    blob.download_to_filename(local_path)
                
                model = tf.keras.models.load_model(local_model_dir)
                model_name = f"Custom:{model_path}"
                
            else:
                # Pre-trained models
                if model_type == "resnet50":
                    log_info("Loading ResNet50 (ImageNet) model")
                    model = tf.keras.applications.ResNet50(weights="imagenet")
                    model_name = "ResNet50(ImageNet)"
                elif model_type == "vgg16":
                    log_info("Loading VGG16 (ImageNet) model")
                    model = tf.keras.applications.VGG16(weights="imagenet")
                    model_name = "VGG16(ImageNet)"
                else:  # Default to MobileNetV2
                    log_info("Loading MobileNetV2 (ImageNet) model")
                    model = tf.keras.applications.MobileNetV2(weights="imagenet")
                    model_name = "MobileNetV2(ImageNet)"
            
            _EXECUTOR_STATE["model"] = model
            _EXECUTOR_STATE["model_name"] = model_name
            log_info(f"Model loaded successfully: {model_name}")
            return model, model_name
            
        except Exception as e:
            log_error(f"Failed to load model: {e}")
            raise


def _preprocess_image_for_model(img_bytes: bytes, model_type: str = "mobilenet", 
                               target_size=(224, 224)):
    """Preprocess image bytes for the specific model type."""
    import numpy as np
    import tensorflow as tf
    from PIL import Image
    
    try:
        # Load and convert image
        image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        image = image.resize(target_size)
        img_array = np.array(image)
        img_array = np.expand_dims(img_array, axis=0)
        
        # Model-specific preprocessing
        if model_type == "resnet50":
            img_array = tf.keras.applications.resnet50.preprocess_input(img_array)
        elif model_type == "vgg16":
            img_array = tf.keras.applications.vgg16.preprocess_input(img_array)
        else:  # mobilenet
            img_array = tf.keras.applications.mobilenet_v2.preprocess_input(img_array)
        
        return img_array
    except Exception as e:
        log_error(f"Image preprocessing failed: {e}")
        raise


def _update_performance_metrics(processing_time: float, success: bool):
    """Update executor-level performance metrics."""
    _EXECUTOR_STATE["total_processing_time"] += processing_time
    if success:
        _EXECUTOR_STATE["prediction_count"] += 1
    else:
        _EXECUTOR_STATE["error_count"] += 1
    
    # Log metrics every 100 predictions
    current_time = time.time()
    if (current_time - _EXECUTOR_STATE["last_metrics_log"]) > 60:  # Every minute
        total_predictions = _EXECUTOR_STATE["prediction_count"] + _EXECUTOR_STATE["error_count"]
        if total_predictions > 0:
            avg_time = _EXECUTOR_STATE["total_processing_time"] / total_predictions
            success_rate = _EXECUTOR_STATE["prediction_count"] / total_predictions * 100
            log_info(f"Performance metrics - Total: {total_predictions}, "
                    f"Success rate: {success_rate:.1f}%, Avg time: {avg_time:.3f}s")
        _EXECUTOR_STATE["last_metrics_log"] = current_time


@F.udf(
    T.StructType([
        T.StructField("prediction_id", T.StringType(), True),
        T.StructField("label", T.StringType(), True),
        T.StructField("confidence", T.DoubleType(), True),
        T.StructField("model_name", T.StringType(), True),
        T.StructField("processing_time_ms", T.LongType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("image_size_bytes", T.LongType(), True)
    ])
)
def classify_image_enhanced_udf(image_source: str, image_data: str, 
                               model_path: str, model_type: str):
    """Enhanced UDF for real-time image classification with performance tracking."""
    start_time = time.time()
    prediction_id = str(uuid.uuid4())
    
    try:
        # Decode image data based on source
        if image_source == "base64":
            img_bytes = base64.b64decode(image_data)
        elif image_source == "gcs":
            # image_data contains "bucket,object_name"
            bucket, object_name = image_data.split(",", 1)
            img_bytes = _download_image_from_gcs(bucket, object_name)
            if not img_bytes:
                raise Exception("Failed to download image from GCS")
        else:
            raise Exception(f"Unsupported image source: {image_source}")
        
        image_size = len(img_bytes)
        
        # Load model
        model, model_name = _download_and_cache_model(model_path, model_type)
        
        # Preprocess image
        processed_img = _preprocess_image_for_model(img_bytes, model_type)
        
        # Run prediction
        import tensorflow as tf
        predictions = model.predict(processed_img)
        
        # Decode predictions based on model type
        if model_type == "resnet50":
            decoded = tf.keras.applications.resnet50.decode_predictions(predictions, top=1)
        elif model_type == "vgg16":
            decoded = tf.keras.applications.vgg16.decode_predictions(predictions, top=1)
        else:  # mobilenet
            decoded = tf.keras.applications.mobilenet_v2.decode_predictions(predictions, top=1)
        
        # Extract top prediction
        top_prediction = decoded[0][0]
        label = top_prediction[1]
        confidence = float(top_prediction[2])
        
        processing_time = time.time() - start_time
        _update_performance_metrics(processing_time, True)
        
        return {
            "prediction_id": prediction_id,
            "label": label,
            "confidence": confidence,
            "model_name": model_name,
            "processing_time_ms": int(processing_time * 1000),
            "error_message": None,
            "image_size_bytes": image_size
        }
        
    except Exception as e:
        processing_time = time.time() - start_time
        _update_performance_metrics(processing_time, False)
        
        return {
            "prediction_id": prediction_id,
            "label": None,
            "confidence": None,
            "model_name": None,
            "processing_time_ms": int(processing_time * 1000),
            "error_message": str(e),
            "image_size_bytes": None
        }


def create_spark_session(app_name: str = "EnhancedImageStreaming") -> SparkSession:
    """Create optimized Spark session for streaming."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.streaming.numRecentProgressUpdates", "100")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_kafka_stream(spark: SparkSession, kafka_servers: str, topic: str):
    """Create Kafka streaming DataFrame for image data."""
    log_info(f"Creating Kafka stream from topic: {topic}")
    
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def create_pubsub_stream(spark: SparkSession, project_id: str, subscription: str):
    """Create Pub/Sub streaming DataFrame for GCS notifications."""
    log_info(f"Creating Pub/Sub stream from subscription: {subscription}")
    
    if not subscription.startswith("projects/"):
        subscription = f"projects/{project_id}/subscriptions/{subscription}"
    
    return (
        spark.readStream
        .format("pubsub")
        .option("projectId", project_id)
        .option("subscription", subscription)
        .load()
    )


def process_kafka_image_stream(df, model_path: str, model_type: str):
    """Process Kafka stream containing base64 encoded images."""
    log_info("Processing Kafka image stream")
    
    # Parse Kafka message value as JSON
    schema = T.StructType([
        T.StructField("image_id", T.StringType(), True),
        T.StructField("image_data", T.StringType(), True),  # base64 encoded
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType()), True)
    ])
    
    parsed_df = df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add classification
    classified_df = parsed_df.withColumn(
        "prediction",
        classify_image_enhanced_udf(
            F.lit("base64"),
            F.col("image_data"),
            F.lit(model_path),
            F.lit(model_type)
        )
    )
    
    # Flatten prediction results
    return (
        classified_df
        .select(
            F.col("image_id"),
            F.col("timestamp"),
            F.col("metadata"),
            F.col("prediction.prediction_id"),
            F.col("prediction.label"),
            F.col("prediction.confidence"),
            F.col("prediction.model_name"),
            F.col("prediction.processing_time_ms"),
            F.col("prediction.error_message"),
            F.col("prediction.image_size_bytes"),
            F.current_timestamp().alias("processed_at")
        )
        .filter(F.col("error_message").isNull())
    )


def process_pubsub_gcs_stream(df, model_path: str, model_type: str):
    """Process Pub/Sub stream containing GCS object notifications."""
    log_info("Processing Pub/Sub GCS notification stream")
    
    # Parse GCS notification
    payload_col = F.coalesce(F.col("message"), F.col("data"))
    json_str = payload_col.cast("string")
    
    gcs_schema = T.StructType([
        T.StructField("bucket", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("contentType", T.StringType(), True),
        T.StructField("size", T.StringType(), True),
        T.StructField("timeCreated", T.StringType(), True)
    ])
    
    parsed_df = df.select(
        F.from_json(json_str, gcs_schema).alias("gcs_event")
    ).select("gcs_event.*")
    
    # Filter for image files
    image_df = parsed_df.filter(
        (F.col("contentType").like("image/%")) |
        F.col("name").rlike("(?i)\\.(jpg|jpeg|png|gif|bmp|webp)$")
    )
    
    # Create GCS path for classification
    gcs_path_df = image_df.withColumn(
        "gcs_path",
        F.concat(F.col("bucket"), F.lit(","), F.col("name"))
    )
    
    # Add classification
    classified_df = gcs_path_df.withColumn(
        "prediction",
        classify_image_enhanced_udf(
            F.lit("gcs"),
            F.col("gcs_path"),
            F.lit(model_path),
            F.lit(model_type)
        )
    )
    
    # Flatten results
    return (
        classified_df
        .select(
            F.col("bucket"),
            F.col("name"),
            F.col("contentType"),
            F.col("size"),
            F.col("timeCreated"),
            F.col("prediction.prediction_id"),
            F.col("prediction.label"),
            F.col("prediction.confidence"),
            F.col("prediction.model_name"),
            F.col("prediction.processing_time_ms"),
            F.col("prediction.error_message"),
            F.col("prediction.image_size_bytes"),
            F.current_timestamp().alias("processed_at")
        )
        .filter(F.col("error_message").isNull())
    )


def setup_output_sinks(df, args: argparse.Namespace) -> list:
    """Setup multiple output sinks for streaming results."""
    queries = []
    
    # Console sink for monitoring
    log_info("Setting up console output sink")
    console_query = (
        df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", f"{args.checkpoint}/console")
        .start()
    )
    queries.append(console_query)
    
    # BigQuery sink
    if args.output_bq_table:
        log_info(f"Setting up BigQuery output sink: {args.output_bq_table}")
        bq_query = (
            df.writeStream
            .format("bigquery")
            .option("table", args.output_bq_table)
            .option("checkpointLocation", f"{args.checkpoint}/bigquery")
            .outputMode("append")
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )
        queries.append(bq_query)
    
    # Kafka output sink for downstream processing
    if args.output_kafka_topic:
        log_info(f"Setting up Kafka output sink: {args.output_kafka_topic}")
        kafka_output_df = df.select(
            F.col("prediction_id").alias("key"),
            F.to_json(F.struct([c for c in df.columns])).alias("value")
        )
        
        kafka_query = (
            kafka_output_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_servers)
            .option("topic", args.output_kafka_topic)
            .option("checkpointLocation", f"{args.checkpoint}/kafka_output")
            .outputMode("append")
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )
        queries.append(kafka_query)
    
    # GCS JSON sink
    if args.output_gcs_path:
        log_info(f"Setting up GCS JSON output sink: {args.output_gcs_path}")
        gcs_query = (
            df.writeStream
            .format("json")
            .option("path", args.output_gcs_path)
            .option("checkpointLocation", f"{args.checkpoint}/gcs")
            .outputMode("append")
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )
        queries.append(gcs_query)
    
    return queries


def main():
    parser = argparse.ArgumentParser(description="Enhanced Real-Time Image Classification")
    
    # Input sources
    parser.add_argument("--kafka-servers", default="localhost:9092", 
                       help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="image-stream", 
                       help="Kafka topic for image streaming")
    parser.add_argument("--project-id", help="GCP project ID for Pub/Sub")
    parser.add_argument("--pubsub-subscription", help="Pub/Sub subscription for GCS notifications")
    
    # Model configuration
    parser.add_argument("--model-type", choices=["mobilenet", "resnet50", "vgg16"], 
                       default="mobilenet", help="Pre-trained model type")
    parser.add_argument("--model-gcs-path", help="Custom model path in GCS")
    parser.add_argument("--confidence-threshold", type=float, default=0.1, 
                       help="Minimum confidence threshold")
    
    # Streaming configuration
    parser.add_argument("--checkpoint", required=True, 
                       help="Checkpoint location (GCS path)")
    parser.add_argument("--trigger-seconds", type=int, default=5, 
                       help="Trigger interval in seconds")
    
    # Output sinks
    parser.add_argument("--output-bq-table", help="BigQuery table for results")
    parser.add_argument("--output-kafka-topic", help="Kafka topic for output")
    parser.add_argument("--output-gcs-path", help="GCS path for JSON outputs")
    
    # Processing configuration
    parser.add_argument("--input-source", choices=["kafka", "pubsub", "both"], 
                       default="kafka", help="Input streaming source")
    
    args = parser.parse_args()
    
    log_info("Starting Enhanced Real-Time Image Classification")
    log_info(f"Input source: {args.input_source}")
    log_info(f"Model type: {args.model_type}")
    log_info(f"Confidence threshold: {args.confidence_threshold}")
    
    # Create Spark session
    spark = create_spark_session()
    
    streaming_dfs = []
    
    # Setup Kafka stream
    if args.input_source in ["kafka", "both"]:
        kafka_df = create_kafka_stream(spark, args.kafka_servers, args.kafka_topic)
        processed_kafka_df = process_kafka_image_stream(
            kafka_df, args.model_gcs_path, args.model_type
        )
        # Filter by confidence threshold
        filtered_kafka_df = processed_kafka_df.filter(
            F.col("confidence") >= F.lit(args.confidence_threshold)
        )
        streaming_dfs.append(filtered_kafka_df)
    
    # Setup Pub/Sub stream
    if args.input_source in ["pubsub", "both"] and args.project_id and args.pubsub_subscription:
        pubsub_df = create_pubsub_stream(spark, args.project_id, args.pubsub_subscription)
        processed_pubsub_df = process_pubsub_gcs_stream(
            pubsub_df, args.model_gcs_path, args.model_type
        )
        # Filter by confidence threshold
        filtered_pubsub_df = processed_pubsub_df.filter(
            F.col("confidence") >= F.lit(args.confidence_threshold)
        )
        streaming_dfs.append(filtered_pubsub_df)
    
    if not streaming_dfs:
        log_error("No valid input streams configured")
        return
    
    # Union multiple streams if needed
    if len(streaming_dfs) > 1:
        # Align schemas and union streams
        final_df = streaming_dfs[0]
        for df in streaming_dfs[1:]:
            final_df = final_df.union(df)
    else:
        final_df = streaming_dfs[0]
    
    # Setup output sinks
    queries = setup_output_sinks(final_df, args)
    
    log_info(f"Started {len(queries)} streaming queries")
    
    try:
        # Wait for termination
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        log_info("Stopping streaming queries...")
        for query in queries:
            query.stop()
    
    spark.stop()
    log_info("Enhanced streaming application stopped")


if __name__ == "__main__":
    main()