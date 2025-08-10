#!/usr/bin/env bash
set -euo pipefail

# Submit the image streaming job to an existing Dataproc cluster

# Required (override via env or export before running)
PROJECT_ID=${PROJECT_ID:-your-project}
REGION=${REGION:-us-central1}
CLUSTER=${CLUSTER:-image-stream-cluster}

# Pub/Sub subscription (name or full path). If name, the script expands it.
SUBSCRIPTION=${SUBSCRIPTION:-image-stream-sub}

# Checkpoint directory in GCS (must be writable by the job SA)
CHECKPOINT_GCS=${CHECKPOINT_GCS:-gs://your-bucket/checkpoints/image-stream}

# Optional outputs
OUT_BQ=${OUT_BQ:-}        # project.dataset.table
OUT_GCS=${OUT_GCS:-}      # gs://bucket/path

# Optional model location (SavedModel dir or .h5)
MODEL_GCS_PATH=${MODEL_GCS_PATH:-}

# Trigger interval seconds
TRIGGER_SECONDS=${TRIGGER_SECONDS:-10}

# Connector versions (tune to your Spark minor if needed)
PUBSUB_PKG=${PUBSUB_PKG:-com.google.cloud.spark:spark-pubsub_2.12:2.4.6}
BIGQUERY_PKG=${BIGQUERY_PKG:-com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1}

PKGS="${PUBSUB_PKG}"
if [[ -n "${OUT_BQ}" ]]; then
  PKGS+=" ,${BIGQUERY_PKG}"
fi

# Expand subscription if not full path
if [[ "${SUBSCRIPTION}" != projects/* ]]; then
  SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION}"
fi

ARGS=(
  week-9/streaming_image_classifier.py
  --project-id "${PROJECT_ID}"
  --subscription "${SUBSCRIPTION}"
  --checkpoint "${CHECKPOINT_GCS}"
  --trigger-seconds "${TRIGGER_SECONDS}"
)

if [[ -n "${MODEL_GCS_PATH}" ]]; then
  ARGS+=(--model-gcs-path "${MODEL_GCS_PATH}")
fi
if [[ -n "${OUT_BQ}" ]]; then
  ARGS+=(--output-bq-table "${OUT_BQ}")
fi
if [[ -n "${OUT_GCS}" ]]; then
  ARGS+=(--output-gcs-path "${OUT_GCS}")
fi

echo "Submitting job to cluster ${CLUSTER} in ${REGION} (project ${PROJECT_ID})"
echo "Packages: ${PKGS}"
echo "Subscription: ${SUBSCRIPTION}  Checkpoint: ${CHECKPOINT_GCS}"

gcloud dataproc jobs submit pyspark \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --cluster="${CLUSTER}" \
  --packages="${PKGS}" \
  --properties=spark.sql.shuffle.partitions=8 \
  -- "${ARGS[@]}"

