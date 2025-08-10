#!/usr/bin/env python3
"""
Structured Streaming image classification on GCP (Pub/Sub → GCS → Spark → TF → sinks)

Overview
- Reads Cloud Storage object-create notifications from Pub/Sub
- For each new object, downloads the image from GCS
- Classifies the image with a TensorFlow/Keras model (MobileNetV2 by default or a SavedModel from GCS)
- Writes predictions to console, optionally to BigQuery or GCS

Notes
- Designed for Dataproc with the Pub/Sub connector and (optionally) BigQuery connector available
- Avoids loading the model repeatedly by using an executor-local singleton
- Keep throughput reasonable; heavy models belong behind batch UDFs or model servers
"""

import os
import io
import sys
import json
import argparse
import base64
import threading
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Global executor-local state
_STATE = {
    "model": None,
    "model_name": None,
    "model_lock": threading.Lock(),
    "gcs_client": None,
}


def _log(msg: str):
    print(msg, file=sys.stderr, flush=True)


def _ensure_google_storage_client():
    """Lazy import and instantiate google-cloud-storage client on executors."""
    if _STATE["gcs_client"] is None:
        from google.cloud import storage  # lazy import to avoid driver dependency if not used
        _STATE["gcs_client"] = storage.Client()
    return _STATE["gcs_client"]


def _download_blob_to_bytes(bucket: str, name: str) -> Optional[bytes]:
    client = _ensure_google_storage_client()
    bucket_ref = client.bucket(bucket)
    blob = bucket_ref.blob(name)
    if not blob.exists():
        return None
    return blob.download_as_bytes()


def _parse_gs_uri(gs_uri: str):
    assert gs_uri.startswith("gs://"), f"Invalid GCS URI: {gs_uri}"
    path = gs_uri[5:]
    bucket, _, name = path.partition("/")
    return bucket, name


def _download_model_if_needed(model_gcs_path: Optional[str]):
    """Ensure a TF model is loaded. If model_gcs_path is provided, download once per executor."""
    if _STATE["model"] is not None:
        return _STATE["model"], _STATE["model_name"]

    with _STATE["model_lock"]:
        if _STATE["model"] is not None:
            return _STATE["model"], _STATE["model_name"]

        try:
            import tensorflow as tf
            # Option A: Load SavedModel from GCS
            if model_gcs_path:
                bucket, name = _parse_gs_uri(model_gcs_path)
                local_dir = "/tmp/streaming_image_model"
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, "saved_model")
                if not os.path.exists(local_path):
                    _log(f"Downloading model from {model_gcs_path} to {local_path}...")
                    client = _ensure_google_storage_client()
                    bucket_ref = client.bucket(bucket)
                    # If path is a directory-like prefix, recursively download
                    blobs = list(client.list_blobs(bucket_ref, prefix=name.rstrip("/") + "/"))
                    if blobs:
                        for b in blobs:
                            rel = b.name[len(name.rstrip("/") + "/"):]
                            dest = os.path.join(local_path, rel)
                            os.makedirs(os.path.dirname(dest), exist_ok=True)
                            b.download_to_filename(dest)
                    else:
                        # Single file (e.g., a .h5 model)
                        os.makedirs(os.path.dirname(local_path), exist_ok=True)
                        bucket_ref.blob(name).download_to_filename(local_path)
                # Load as SavedModel or H5
                if os.path.isdir(local_path):
                    model = tf.keras.models.load_model(local_path)
                else:
                    model = tf.keras.models.load_model(local_path)
                _STATE["model"] = model
                _STATE["model_name"] = f"SavedModel:{model_gcs_path}"
                return model, _STATE["model_name"]

            # Option B: Fallback to MobileNetV2 with ImageNet weights
            _log("Loading MobileNetV2 (ImageNet) as default model...")
            model = tf.keras.applications.MobileNetV2(weights="imagenet")
            _STATE["model"] = model
            _STATE["model_name"] = "MobileNetV2(ImageNet)"
            return model, _STATE["model_name"]

        except Exception as e:
            _log(f"Failed to load model: {e}")
            raise


def _preprocess_image_bytes(img_bytes: bytes, target_size=(224, 224)):
    import numpy as np
    import tensorflow as tf
    from PIL import Image

    image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    image = image.resize(target_size)
    arr = np.array(image)
    arr = tf.keras.applications.mobilenet_v2.preprocess_input(arr)
    arr = np.expand_dims(arr, axis=0)
    return arr


@F.udf(
    T.StructType([
        T.StructField("label", T.StringType(), True),
        T.StructField("score", T.DoubleType(), True),
        T.StructField("model_name", T.StringType(), True),
        T.StructField("error", T.StringType(), True),
    ])
)
def classify_image_udf(bucket: str, name: str, model_gcs_path: Optional[str]):
    """Executor-side UDF: download image and run prediction."""
    try:
        img_bytes = _download_blob_to_bytes(bucket, name)
        if not img_bytes:
            return {"label": None, "score": None, "model_name": None, "error": "blob_not_found"}

        model, model_name = _download_model_if_needed(model_gcs_path)

        arr = _preprocess_image_bytes(img_bytes)

        import tensorflow as tf
        preds = model.predict(arr)
        decoded = tf.keras.applications.mobilenet_v2.decode_predictions(preds, top=1)
        top = decoded[0][0]
        label, score = top[1], float(top[2])
        return {"label": label, "score": score, "model_name": model_name, "error": None}
    except Exception as e:
        return {"label": None, "score": None, "model_name": None, "error": str(e)}


def build_spark(app_name: str = "StreamingImageClassifier") -> SparkSession:
    # On Dataproc, prefer passing connectors via --packages/--jars at submit time
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Image Classifier (GCP)")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--subscription", required=True, help="Pub/Sub subscription (name or full path)")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint GCS path, e.g. gs://bucket/chkpt/image-stream")
    parser.add_argument("--model-gcs-path", default=None, help="Optional gs:// path to SavedModel/H5")
    parser.add_argument("--confidence-threshold", type=float, default=0.0, help="Min score to emit")
    parser.add_argument("--output-bq-table", default=None, help="Optional BigQuery table: project.dataset.table")
    parser.add_argument("--output-gcs-path", default=None, help="Optional GCS path for JSONL outputs")
    parser.add_argument("--trigger-seconds", type=int, default=10, help="Trigger interval seconds")
    args = parser.parse_args()

    spark = build_spark()

    # Pub/Sub read (Google Cloud Spark Pub/Sub connector)
    # The connector may expose either `message` or `data` column depending on version; handle both.
    subscription = args.subscription
    if not subscription.startswith("projects/"):
        subscription = f"projects/{args.project_id}/subscriptions/{subscription}"

    input_df = (
        spark.readStream
        .format("pubsub")
        .option("projectId", args.project_id)
        .option("subscription", subscription)
        .load()
    )

    # Normalize payload column to string
    payload_col = F.coalesce(F.col("message"), F.col("data"))
    json_str = payload_col.cast("string").alias("json_str")
    events = input_df.select(json_str)

    # GCS notification message schema (simplified / permissive)
    schema = T.StructType([
        T.StructField("kind", T.StringType(), True),
        T.StructField("id", T.StringType(), True),
        T.StructField("selfLink", T.StringType(), True),
        T.StructField("name", T.StringType(), True),  # object name
        T.StructField("bucket", T.StringType(), True),
        T.StructField("contentType", T.StringType(), True),
        T.StructField("size", T.StringType(), True),
        T.StructField("timeCreated", T.StringType(), True),
        T.StructField("updated", T.StringType(), True),
        T.StructField("metageneration", T.StringType(), True),
        T.StructField("generation", T.StringType(), True),
    ])

    parsed = events.select(F.from_json(F.col("json_str"), schema).alias("e")).select("e.*")

    # Filter for likely images (by contentType or name suffix)
    is_image = (
        (F.col("contentType").like("image/%"))
        | F.col("name").rlike("(?i)\\.(jpg|jpeg|png|gif|bmp|webp)$")
    )
    images = parsed.where(is_image)

    # Classify via UDF
    with_preds = images.withColumn(
        "pred",
        classify_image_udf(F.col("bucket"), F.col("name"), F.lit(args.model_gcs_path))
    )

    flattened = (
        with_preds
        .withColumn("label", F.col("pred.label"))
        .withColumn("score", F.col("pred.score"))
        .withColumn("model_name", F.col("pred.model_name"))
        .withColumn("error", F.col("pred.error"))
        .withColumn("event_time", F.current_timestamp())
        .drop("pred")
    )

    results = flattened.where((F.col("error").isNull()) & (F.col("score") >= F.lit(args.confidence_threshold)))

    # Write sinks
    queries = []

    # Console for quick visibility
    queries.append(
        results.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", os.path.join(args.checkpoint, "console"))
        .start()
    )

    # Optional BigQuery sink
    if args.output_bq_table:
        queries.append(
            results.writeStream
            .format("bigquery")
            .option("table", args.output_bq_table)
            .option("checkpointLocation", os.path.join(args.checkpoint, "bigquery"))
            .outputMode("append")
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )

    # Optional JSONL to GCS
    if args.output_gcs_path:
        out_cols = [
            "bucket", "name", "contentType", "size", "timeCreated", "updated",
            "label", "score", "model_name", "event_time"
        ]
        queries.append(
            results.select(*[F.col(c) for c in out_cols])
            .writeStream
            .format("json")
            .option("path", args.output_gcs_path)
            .option("checkpointLocation", os.path.join(args.checkpoint, "gcs"))
            .outputMode("append")
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )

    # Keep alive
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()

