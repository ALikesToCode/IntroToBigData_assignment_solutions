Streaming Image Classification on GCP with Spark Structured Streaming

Summary
- Source: Cloud Storage object-create events via Pub/Sub
- Transform: Download image, classify with TensorFlow (MobileNetV2 by default or a SavedModel from GCS)
- Sinks: Console, optional BigQuery, optional JSON to GCS
- Runtime: Dataproc cluster with Pub/Sub and (optionally) BigQuery connectors

Repo Files
- `week-9/streaming_image_classifier.py`: Main Structured Streaming app
- `week-9/scripts/create_dataproc_cluster_tf.sh`: Creates a Dataproc cluster and installs TensorFlow/Pillow via init actions
- `week-9/scripts/submit_image_stream.sh`: Submits the streaming job with configurable variables

Architecture
- GCS Bucket (new images uploaded)
- Cloud Storage Notification → Pub/Sub Topic
- Pub/Sub Subscription → Spark Structured Streaming (connector)
- UDF downloads `gs://bucket/object` and runs TF model → predictions
- Outputs to Console/BigQuery/GCS

Prerequisites
- GCP project with permissions to use Dataproc, Pub/Sub, GCS, and BigQuery
- Roles for Dataproc job SA: `roles/storage.objectViewer`, `roles/pubsub.subscriber`, `roles/bigquery.dataEditor` (if writing to BQ)
- Create a Pub/Sub topic and subscription wired to your bucket events:
  - Enable bucket notifications to Pub/Sub (one of the options):
    - Using Eventarc (recommended) or
    - Legacy notifications: `gsutil notification create -t your-topic -f json gs://your-bucket`
  - Create a pull subscription on that topic

Dataproc Cluster
- Image: Dataproc 2.0+ (Spark 3.1+) or 2.1+ (Spark 3.3+)
- Connectors (pass with `--packages` during job submit):
  - Pub/Sub: `com.google.cloud.spark:spark-pubsub_2.12:2.4.6` (match to your Spark minor if needed)
  - BigQuery (optional): `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1`
- Python deps on cluster workers (if using default model or SavedModel):
  - `tensorflow` and `Pillow` (install via an init action or custom image)

Model Options
- Default: MobileNetV2 (ImageNet weights). Requires internet to download weights at first run – consider warming the image or providing a SavedModel.
- Custom: Provide `--model-gcs-path gs://bucket/path/to/saved_model` (SavedModel dir) or an `.h5` file. The job downloads once per executor to `/tmp/streaming_image_model`.

Submit the Job
Replace placeholders with your values.

```
PROJECT_ID=your-project
REGION=us-central1
CLUSTER=your-dataproc-cluster
SUBSCRIPTION=your-subscription-name    # or full path projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION
CHKPT=gs://your-bucket/checkpoints/image-stream
OUT_BQ=${PROJECT_ID}.ml_predictions.image_stream    # optional
OUT_GCS=gs://your-bucket/outputs/image_preds        # optional

gcloud dataproc jobs submit pyspark \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --cluster=${CLUSTER} \
  --properties=spark.sql.shuffle.partitions=8 \
  --packages=com.google.cloud.spark:spark-pubsub_2.12:2.4.6,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
  -- \
  week-9/streaming_image_classifier.py \
  --project-id ${PROJECT_ID} \
  --subscription ${SUBSCRIPTION} \
  --checkpoint ${CHKPT} \
  --confidence-threshold 0.0 \
  --output-bq-table ${OUT_BQ} \
  --output-gcs-path ${OUT_GCS}
```

If passing a custom model:

```
  --model-gcs-path gs://your-bucket/models/imagenet_saved_model
```

Helper Scripts
- Create cluster with TF/Pillow preinstalled:
  - Edit env defaults or export values, then run:
  - `bash week-9/scripts/create_dataproc_cluster_tf.sh`
- Submit the streaming job:
  - Export required envs (PROJECT_ID, REGION, CLUSTER, SUBSCRIPTION, CHECKPOINT_GCS)
  - Optionally export OUT_BQ, OUT_GCS, MODEL_GCS_PATH
  - `bash week-9/scripts/submit_image_stream.sh`

BigQuery Table
- If the table does not exist, the connector can create it.
- Schema (fields written):
  - `bucket STRING`, `name STRING`, `contentType STRING`, `size STRING`, `timeCreated STRING`, `updated STRING`
  - `label STRING`, `score FLOAT64`, `model_name STRING`, `event_time TIMESTAMP`

Local/Dev Hints
- Use a small test bucket and low event rates first.
- Monitor driver logs for any TensorFlow errors or connector classpath issues.
- If you see `ClassNotFound` for `pubsub`, verify `--packages` and Dataproc image compatibility.
- Ensure workers have `tensorflow` and `Pillow` available; use an init action to `pip install` them if not using a custom image.

Troubleshooting
- Empty predictions: check that bucket/object in the Pub/Sub message refers to an image and is readable by the job’s service account.
- Model loading slow: prefer a SavedModel in GCS, pass `--model-gcs-path`, and consider caching on local SSD.
- Throughput: scale Dataproc workers, tune partitions, and consider vectorized Pandas UDFs for batch inference if needed.
