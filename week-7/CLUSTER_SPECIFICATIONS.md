# Week 7 Cluster Specifications - Updated for Memory Optimization

## 🚀 Cluster Configuration Summary

### Machine Types
- **Master Node**: `e2-standard-2` (2 vCPUs, 8GB RAM)
- **Worker Nodes**: `e2-standard-2` (2 vCPUs, 8GB RAM) × 2 instances
- **Preemptible Workers**: None (for streaming reliability)

### Memory Allocation

#### Spark Configuration
- **Driver Memory**: 1GB (coordinator tasks)
- **Executor Memory**: 2-3GB (streaming processing)
- **Executor Instances**: 2 
- **Executor Cores**: 1 (controlled parallelism)

#### Kafka Configuration
- **Heap Size**: 1GB (`-Xmx1g -Xms1g`)
- **Java Version**: OpenJDK 11
- **Kafka Version**: 3.9.1

### Key Improvements Made

#### 1. Memory Optimization
- ✅ **Upgraded from e2-medium to e2-standard-2**
  - Previous: 1 vCPU, 4GB RAM → **Insufficient for streaming**
  - Current: 2 vCPUs, 8GB RAM → **Adequate for Kafka + Spark**

#### 2. Spark Memory Management
- ✅ **Increased executor memory**: 1GB → 2-3GB
- ✅ **Optimized driver memory**: 512MB → 1GB
- ✅ **Added core constraints**: Prevents CPU oversubscription
- ✅ **Checkpoint location**: Reliable state management

#### 3. Kafka Optimization
- ✅ **Doubled heap memory**: 512MB → 1GB
- ✅ **Better Java compatibility**: OpenJDK 11
- ✅ **Optimized topic configuration**: 3 partitions, replication factor 1

### Resource Requirements

#### Per Node Resources
```
Master Node (e2-standard-2):
├── System overhead: ~1GB RAM
├── Spark Driver: 1GB RAM
├── Kafka (if colocated): 1GB RAM
└── Available headroom: ~5GB RAM

Worker Node (e2-standard-2):
├── System overhead: ~1GB RAM  
├── Spark Executor: 2GB RAM
├── YARN overhead: ~1GB RAM
└── Available headroom: ~4GB RAM
```

#### Total Cluster Resources
- **Total vCPUs**: 6 (2×3 nodes)
- **Total RAM**: 24GB (8GB×3 nodes)
- **Effective Streaming Memory**: ~12GB
- **Kafka Heap**: 1GB
- **Spark Processing**: 7GB (1GB driver + 4GB executors + 2GB overhead)

### Performance Expectations

#### Throughput Capacity
- **Kafka Messages**: ~1000 msgs/sec sustainable
- **Spark Micro-batches**: 10-second windows, 5-second slides
- **Data Volume**: Up to 1200 customer transactions efficiently
- **Latency**: <30 seconds end-to-end processing

#### Reliability Features
- **No preemptible instances**: Consistent availability
- **Adaptive query execution**: Dynamic optimization
- **Checkpoint recovery**: Fault tolerance
- **Auto-scaling disabled**: Predictable resource usage

### Deployment Command

```bash
python scripts/deploy_to_dataproc.py \
    --project-id YOUR_PROJECT_ID \
    --region us-central1 \
    --zone us-central1-b
```

### Monitoring & Verification

#### Health Checks
1. **Cluster Status**: `gcloud dataproc clusters describe week7-streaming-cluster`
2. **Kafka Topics**: SSH to master and run `kafka-topics.sh --list --bootstrap-server localhost:9092`
3. **Spark Jobs**: `gcloud dataproc jobs list --cluster=week7-streaming-cluster`
4. **Memory Usage**: Dataproc monitoring dashboard

#### Expected Behavior
- ✅ **No SIGTERM/SIGKILL errors** (memory pressure resolved)
- ✅ **Stable streaming jobs** (adequate resources)
- ✅ **Kafka topic creation** (proper initialization)
- ✅ **Checkpoint persistence** (GCS integration)

## 🎯 Resolution Summary

The updated cluster specifications resolve the previous memory pressure issues by:

1. **Doubling available memory** per instance (4GB → 8GB)
2. **Optimizing memory distribution** across components
3. **Adding resource constraints** to prevent overallocation
4. **Improving fault tolerance** with checkpointing

The cluster is now production-ready for the Week 7 Kafka + Spark streaming pipeline!