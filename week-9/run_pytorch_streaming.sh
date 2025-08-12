#!/bin/bash

# Run PyTorch Streaming Image Classifier - Direct Conversion of Lecture 10
# This script demonstrates how to run the streaming version that directly
# converts the batch notebook to streaming

echo "======================================================================"
echo "PyTorch Streaming Image Classifier - Lecture 10 Conversion"
echo "======================================================================"

# Configuration
CHECKPOINT_DIR="/tmp/pytorch-streaming-checkpoint"
OUTPUT_DIR="/tmp/pytorch-streaming-output"
INPUT_DIR="${1:-/path/to/flower_photos}"  # Pass as argument or set default

# Clean previous runs
rm -rf $CHECKPOINT_DIR $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

echo "Configuration:"
echo "  Input Directory: $INPUT_DIR"
echo "  Checkpoint: $CHECKPOINT_DIR"
echo "  Output: $OUTPUT_DIR"
echo ""

# Option 1: File-based streaming (monitors directory for new images)
echo "Option 1: File-based Streaming (monitors directory for new .jpg files)"
echo "----------------------------------------------------------------------"
echo "Command:"
echo "python pytorch_streaming_classifier.py \\"
echo "    --source file \\"
echo "    --input-path $INPUT_DIR \\"
echo "    --checkpoint $CHECKPOINT_DIR \\"
echo "    --output-path $OUTPUT_DIR \\"
echo "    --trigger-seconds 5 \\"
echo "    --model mobilenet_v2"
echo ""
echo "To run: Uncomment the command below"
# python pytorch_streaming_classifier.py \
#     --source file \
#     --input-path $INPUT_DIR \
#     --checkpoint $CHECKPOINT_DIR \
#     --output-path $OUTPUT_DIR \
#     --trigger-seconds 5 \
#     --model mobilenet_v2

echo ""
echo "Option 2: Kafka-based Streaming"
echo "----------------------------------------------------------------------"
echo "Command:"
echo "python pytorch_streaming_classifier.py \\"
echo "    --source kafka \\"
echo "    --kafka-servers localhost:9092 \\"
echo "    --kafka-topic flower-images \\"
echo "    --checkpoint $CHECKPOINT_DIR \\"
echo "    --output-path $OUTPUT_DIR \\"
echo "    --trigger-seconds 5 \\"
echo "    --model mobilenet_v2"
echo ""
echo "To run: Start Kafka server and uncomment the command below"
# python pytorch_streaming_classifier.py \
#     --source kafka \
#     --kafka-servers localhost:9092 \
#     --kafka-topic flower-images \
#     --checkpoint $CHECKPOINT_DIR \
#     --output-path $OUTPUT_DIR \
#     --trigger-seconds 5 \
#     --model mobilenet_v2

echo ""
echo "Option 3: GCP Pub/Sub Streaming"
echo "----------------------------------------------------------------------"
echo "Command:"
echo "python pytorch_streaming_classifier.py \\"
echo "    --source pubsub \\"
echo "    --project-id YOUR_PROJECT_ID \\"
echo "    --pubsub-subscription flower-images-sub \\"
echo "    --checkpoint gs://your-bucket/checkpoints/pytorch-streaming \\"
echo "    --output-path gs://your-bucket/outputs/pytorch-predictions \\"
echo "    --trigger-seconds 10 \\"
echo "    --model mobilenet_v2"
echo ""

echo "======================================================================"
echo "To simulate streaming with the flower dataset:"
echo "1. Download the flower photos dataset"
echo "2. Set INPUT_DIR to the flower_photos directory"
echo "3. Run Option 1 for file-based streaming"
echo "4. Copy new images to the directory to trigger processing"
echo ""
echo "Example:"
echo "  # In another terminal, copy images to trigger streaming"
echo "  cp /path/to/new/image.jpg $INPUT_DIR/streaming/"
echo "======================================================================"