#!/usr/bin/env python3
"""
Direct Conversion of Lecture 10 Batch Image Classification to Spark Streaming
Course: Introduction to Big Data - Week 9
Author: Abhyudaya B Tharakan 22f3001492

This implementation directly converts the batch PyTorch-based image classification
from lecture_10_dl.py to a real-time streaming model using Spark Structured Streaming.
It maintains the same PyTorch MobileNetV2 model and similar UDF structure.
"""

import os
import io
import sys
import json
import argparse
import base64
import threading
from typing import Optional, Iterator
from datetime import datetime

import pandas as pd
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, pandas_udf, regexp_extract

# Import PyTorch components (same as original notebook)
import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import models, transforms

# Try importing TensorFlow's decode_predictions for compatibility
try:
    from tensorflow.keras.applications.imagenet_utils import decode_predictions
except ImportError:
    # Fallback to torchvision's imagenet classes
    import json
    import urllib
    
    def download_imagenet_classes():
        """Download ImageNet class labels if TensorFlow not available."""
        url = "https://raw.githubusercontent.com/anishathalye/imagenet-simple-labels/master/imagenet-simple-labels.json"
        response = urllib.request.urlopen(url)
        classes = json.loads(response.read())
        return classes
    
    IMAGENET_CLASSES = None
    
    def decode_predictions(preds, top=1):
        """Simplified decode_predictions for PyTorch outputs."""
        global IMAGENET_CLASSES
        if IMAGENET_CLASSES is None:
            IMAGENET_CLASSES = download_imagenet_classes()
        
        results = []
        for pred in preds:
            top_indices = pred.argsort()[-top:][::-1]
            top_preds = []
            for idx in top_indices:
                class_name = IMAGENET_CLASSES[idx] if idx < len(IMAGENET_CLASSES) else f"class_{idx}"
                score = float(pred[idx])
                top_preds.append((str(idx), class_name, score))
            results.append(top_preds)
        return results


# Global executor state for model caching (similar to TF version but for PyTorch)
_EXECUTOR_STATE = {
    "model": None,
    "model_lock": threading.Lock(),
    "device": None
}


class ImageNetDataset(Dataset):
    """
    Converts image contents into a PyTorch Dataset with standard ImageNet preprocessing.
    (Same as in the original notebook)
    """
    def __init__(self, contents):
        self.contents = contents

    def __len__(self):
        return len(self.contents)

    def __getitem__(self, index):
        return self._preprocess(self.contents[index])

    def _preprocess(self, content):
        """
        Preprocesses the input image content using standard ImageNet normalization.
        (Same as in the original notebook)
        """
        image = Image.open(io.BytesIO(content))
        transform = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ])
        return transform(image)


def get_or_create_model(model_name: str = "mobilenet_v2"):
    """
    Get or create PyTorch model with caching.
    Similar to the original but with executor-level caching for streaming.
    """
    if _EXECUTOR_STATE["model"] is not None:
        return _EXECUTOR_STATE["model"], _EXECUTOR_STATE["device"]
    
    with _EXECUTOR_STATE["model_lock"]:
        if _EXECUTOR_STATE["model"] is not None:
            return _EXECUTOR_STATE["model"], _EXECUTOR_STATE["device"]
        
        # Determine device
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        _EXECUTOR_STATE["device"] = device
        
        # Load model (same as original notebook)
        if model_name == "mobilenet_v2":
            model = models.mobilenet_v2(pretrained=True)
        elif model_name == "resnet50":
            model = models.resnet50(pretrained=True)
        elif model_name == "vgg16":
            model = models.vgg16(pretrained=True)
        else:
            model = models.mobilenet_v2(pretrained=True)  # Default
        
        model = model.to(device)
        model.eval()
        _EXECUTOR_STATE["model"] = model
        
        print(f"Loaded {model_name} model on {device}", file=sys.stderr)
        return model, device


def extract_label_from_path(path_col):
    """
    Extract label from file path using built-in SQL functions.
    (Same as original notebook's extract_label function)
    """
    # For streaming, paths might be different format
    # Try multiple patterns
    return F.when(
        regexp_extract(path_col, "flower_photos/([^/]+)", 1) != "",
        regexp_extract(path_col, "flower_photos/([^/]+)", 1)
    ).otherwise(
        regexp_extract(path_col, "([^/]+)/[^/]+\\.jpg$", 1)
    )


def imagenet_streaming_model_udf(model_fn=lambda: models.mobilenet_v2(pretrained=True)):
    """
    Adapted version of imagenet_model_udf for streaming.
    Maintains the same PyTorch model approach as the original notebook.
    """
    def predict(content_series_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
        model, device = get_or_create_model("mobilenet_v2")
        
        for content_series in content_series_iter:
            # Handle both base64 and binary content
            contents = []
            for content in content_series:
                if isinstance(content, str):
                    # Assume base64 encoded
                    try:
                        content = base64.b64decode(content)
                    except:
                        pass
                contents.append(content)
            
            if not contents:
                yield pd.DataFrame({"class": [], "desc": [], "score": []})
                continue
            
            # Create dataset and loader (same as original)
            dataset = ImageNetDataset(contents)
            loader = DataLoader(dataset, batch_size=min(64, len(contents)))
            
            all_predictions = []
            with torch.no_grad():
                for image_batch in loader:
                    image_batch = image_batch.to(device)
                    outputs = model(image_batch)
                    predictions = torch.nn.functional.softmax(outputs, dim=1).cpu().numpy()
                    
                    # Decode predictions (same format as original)
                    decoded = decode_predictions(predictions, top=1)
                    for pred in decoded:
                        if pred and len(pred) > 0:
                            top_pred = pred[0]
                            all_predictions.append({
                                "class": top_pred[0],
                                "desc": top_pred[1],
                                "score": float(top_pred[2])
                            })
                        else:
                            all_predictions.append({
                                "class": "unknown",
                                "desc": "unknown",
                                "score": 0.0
                            })
            
            yield pd.DataFrame(all_predictions)
    
    return_type = T.StructType([
        T.StructField("class", T.StringType(), True),
        T.StructField("desc", T.StringType(), True), 
        T.StructField("score", T.FloatType(), True)
    ])
    
    @pandas_udf(returnType=return_type, functionType="scalar_iter")
    def wrapped_predict(image_iter):
        return predict(image_iter)
    
    return wrapped_predict


# Create the UDF (same as original notebook)
mobilenet_v2_udf = imagenet_streaming_model_udf(lambda: models.mobilenet_v2(pretrained=True))


def create_kafka_stream(spark: SparkSession, kafka_servers: str, topic: str):
    """Create Kafka streaming source for images."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def create_file_stream(spark: SparkSession, input_path: str):
    """
    Create file streaming source for images (similar to original's binaryFile).
    Monitors a directory for new image files.
    """
    return (
        spark.readStream
        .format("binaryFile")
        .option("pathGlobFilter", "*.jpg")
        .option("recursiveFileLookup", "true")
        .load(input_path)
    )


def create_pubsub_stream(spark: SparkSession, project_id: str, subscription: str):
    """Create Pub/Sub streaming source for GCS notifications."""
    if not subscription.startswith("projects/"):
        subscription = f"projects/{project_id}/subscriptions/{subscription}"
    
    return (
        spark.readStream
        .format("pubsub")
        .option("projectId", project_id)
        .option("subscription", subscription)
        .load()
    )


def process_file_stream(df):
    """
    Process file-based streaming DataFrame.
    Maintains the same structure as the original batch processing.
    """
    # Extract size UDF (adapted from original)
    @pandas_udf("width: int, height: int", PandasUDFType.SCALAR_ITER)
    def extract_size_udf(content_series_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
        for content_series in content_series_iter:
            sizes = []
            for content in content_series:
                try:
                    image = Image.open(io.BytesIO(content))
                    sizes.append({"width": image.width, "height": image.height})
                except:
                    sizes.append({"width": 0, "height": 0})
            yield pd.DataFrame(sizes)
    
    # Apply transformations (same structure as original)
    processed_df = df.select(
        col("path"),
        col("modificationTime"),
        extract_label_from_path(col("path")).alias("label"),
        extract_size_udf(col("content")).alias("size"),
        col("content")
    )
    
    # Add predictions (same as original)
    predictions_df = processed_df.withColumn(
        "prediction", 
        mobilenet_v2_udf(col("content"))
    )
    
    return predictions_df


def process_kafka_stream(df):
    """
    Process Kafka streaming DataFrame.
    Expects JSON messages with image data.
    """
    # Parse Kafka message
    schema = T.StructType([
        T.StructField("image_id", T.StringType(), True),
        T.StructField("image_data", T.StringType(), True),  # base64 encoded
        T.StructField("path", T.StringType(), True),
        T.StructField("label", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True)
    ])
    
    parsed_df = df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add predictions using the same UDF
    predictions_df = parsed_df.withColumn(
        "prediction",
        mobilenet_v2_udf(col("image_data"))
    )
    
    return predictions_df


def main():
    parser = argparse.ArgumentParser(
        description="PyTorch Streaming Image Classifier - Direct conversion of Lecture 10 notebook"
    )
    
    # Input source options
    parser.add_argument("--source", choices=["file", "kafka", "pubsub"], 
                       default="file", help="Streaming source type")
    parser.add_argument("--input-path", help="Path for file streaming source")
    parser.add_argument("--kafka-servers", default="localhost:9092", 
                       help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="image-stream", 
                       help="Kafka topic")
    parser.add_argument("--project-id", help="GCP project ID")
    parser.add_argument("--pubsub-subscription", help="Pub/Sub subscription")
    
    # Output options
    parser.add_argument("--output-path", help="Output path for results")
    parser.add_argument("--checkpoint", required=True, 
                       help="Checkpoint location")
    parser.add_argument("--trigger-seconds", type=int, default=10, 
                       help="Trigger interval")
    
    # Model options
    parser.add_argument("--model", choices=["mobilenet_v2", "resnet50", "vgg16"],
                       default="mobilenet_v2", help="Model to use")
    
    args = parser.parse_args()
    
    # Create Spark session (similar to original notebook)
    spark = SparkSession.builder \
        .appName("PyTorchStreamingImageClassifier") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Create streaming source based on type
    if args.source == "file":
        if not args.input_path:
            raise ValueError("--input-path required for file source")
        stream_df = create_file_stream(spark, args.input_path)
        predictions_df = process_file_stream(stream_df)
        
    elif args.source == "kafka":
        stream_df = create_kafka_stream(spark, args.kafka_servers, args.kafka_topic)
        predictions_df = process_kafka_stream(stream_df)
        
    elif args.source == "pubsub":
        if not args.project_id or not args.pubsub_subscription:
            raise ValueError("--project-id and --pubsub-subscription required for pubsub source")
        stream_df = create_pubsub_stream(spark, args.project_id, args.pubsub_subscription)
        # Process similar to Kafka (assuming JSON messages)
        predictions_df = process_kafka_stream(stream_df)
    
    # Select relevant columns for output (matching original notebook's display)
    output_df = predictions_df.select(
        col("path").alias("image_path"),
        col("label"),
        col("prediction.class").alias("predicted_class"),
        col("prediction.desc").alias("predicted_description"),
        col("prediction.score").alias("confidence"),
        F.current_timestamp().alias("processed_at")
    )
    
    # Create streaming queries
    queries = []
    
    # Console output (equivalent to original's .show())
    console_query = (
        output_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", f"{args.checkpoint}/console")
        .start()
    )
    queries.append(console_query)
    
    # File output if specified
    if args.output_path:
        file_query = (
            output_df.writeStream
            .outputMode("append")
            .format("json")
            .option("path", args.output_path)
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .option("checkpointLocation", f"{args.checkpoint}/output")
            .start()
        )
        queries.append(file_query)
    
    # Memory sink for debugging (equivalent to display() in notebook)
    memory_query = (
        output_df.writeStream
        .outputMode("append")
        .format("memory")
        .queryName("predictions")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    queries.append(memory_query)
    
    print("=" * 80)
    print("PyTorch Streaming Image Classifier Started")
    print("=" * 80)
    print(f"Source: {args.source}")
    print(f"Model: {args.model}")
    print(f"Checkpoint: {args.checkpoint}")
    print(f"Trigger: Every {args.trigger_seconds} seconds")
    print("=" * 80)
    print("\nStreaming predictions will appear below:")
    print("-" * 80)
    
    try:
        # Periodically show results from memory table (like display() in notebook)
        import time
        while True:
            time.sleep(args.trigger_seconds)
            # Show latest predictions from memory table
            if spark.catalog.tableExists("predictions"):
                latest = spark.sql("SELECT * FROM predictions ORDER BY processed_at DESC LIMIT 5")
                if latest.count() > 0:
                    print("\nLatest Predictions:")
                    latest.show(truncate=False)
    
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()