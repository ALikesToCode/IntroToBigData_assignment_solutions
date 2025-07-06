# Active Context: Week 4 - SCD Type II Implementation

## Current Focus
Implementing Slowly Changing Dimensions Type II (SCD Type II) using PySpark on Google Cloud Dataproc cluster.

## Assignment Requirements
- Create input files based on lecture examples for customer master data
- Write PySpark code to implement SCD Type II logic
- Execute on Dataproc cluster
- **Constraint**: Do NOT use SparkSQL - use DataFrame/RDD operations only
- Multiple implementation approaches are acceptable

## Key SCD Type II Concepts
- Track historical changes in dimensional data
- Maintain current and historical records
- Use effective dates (start/end dates) to manage versions
- Preserve data lineage and change history

## Implementation Approach
1. Create sample customer master data with changes over time
2. Implement SCD Type II logic using PySpark DataFrame operations
3. Handle new records, updates, and unchanged records
4. Manage effective dating and current record flags
5. Deploy and test on Dataproc cluster

## Next Steps
1. Create week-4 directory structure
2. Generate sample input data files
3. Implement PySpark SCD Type II solution
4. Create deployment scripts for Dataproc
5. Test and validate results

## Dependencies from Previous Weeks
- Google Cloud setup and credentials from Week 3
- Dataproc cluster deployment patterns
- PySpark development environment 