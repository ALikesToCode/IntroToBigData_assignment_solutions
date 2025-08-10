#!/usr/bin/env bash
set -euo pipefail

# Create a Dataproc cluster with TensorFlow, Pillow, and google-cloud-storage installed on all nodes
# Dependencies installed via the official Dataproc pip init action.

# Required (override as needed)
PROJECT_ID=${PROJECT_ID:-your-project}
REGION=${REGION:-us-central1}
ZONE=${ZONE:-us-central1-a}
CLUSTER=${CLUSTER:-image-stream-cluster}
IMAGE_VERSION=${IMAGE_VERSION:-2.1-debian12}
MASTER_MACHINE_TYPE=${MASTER_MACHINE_TYPE:-n1-standard-4}
WORKER_MACHINE_TYPE=${WORKER_MACHINE_TYPE:-n1-standard-4}
WORKER_NUM=${WORKER_NUM:-2}

# Pip packages to install on nodes
PIP_PACKAGES=${PIP_PACKAGES:-"tensorflow pillow google-cloud-storage"}

# Dataproc init action bucket is region-scoped
IA_BUCKET=gs://goog-dataproc-initialization-actions-${REGION}
PIP_INIT=${IA_BUCKET}/python/pip-install.sh

echo "Creating Dataproc cluster: ${CLUSTER} in ${REGION}/${ZONE} (project ${PROJECT_ID})"
echo "Image: ${IMAGE_VERSION}  Master: ${MASTER_MACHINE_TYPE}  Workers: ${WORKER_NUM} x ${WORKER_MACHINE_TYPE}"
echo "Init action: ${PIP_INIT}  PIP_PACKAGES: ${PIP_PACKAGES}"

gcloud dataproc clusters create "${CLUSTER}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --zone="${ZONE}" \
  --image-version="${IMAGE_VERSION}" \
  --master-machine-type="${MASTER_MACHINE_TYPE}" \
  --worker-machine-type="${WORKER_MACHINE_TYPE}" \
  --num-workers="${WORKER_NUM}" \
  --initialization-actions="${PIP_INIT}" \
  --metadata=PIP_PACKAGES="${PIP_PACKAGES}" \
  --properties="spark.sql.adaptive.enabled=true"

echo "Cluster ${CLUSTER} created."

