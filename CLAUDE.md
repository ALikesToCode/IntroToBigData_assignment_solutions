# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains weekly assignment solutions for an Introduction to Big Data course, demonstrating practical implementation of big data technologies, frameworks, and cloud deployment patterns. Each week builds incrementally on previous concepts.

## Common Commands

### Local Testing
```bash
# Week 1: Google Cloud Storage line counting
cd week-1 && python count_gcs_lines.py BUCKET_NAME FILE_PATH

# Week 2: JSON data processing
cd week-2 && python main.py

# Week 3: Spark click analysis
cd week-3 && python test_spark_local.py
cd week-3 && python click_analysis_spark.py

# Week 4: PySpark SCD Type II implementation
cd week-4 && python test_scd_local.py

# Week 5: SparkSQL SCD Type II implementation
cd week-5 && python test_sparksql_local.py
```

### Cloud Deployment
```bash
# Week 3: Deploy to Dataproc
cd week-3 && bash gcloud_deploy.sh

# Week 4: Deploy SCD Type II to Dataproc
cd week-4 && python deploy_to_dataproc.py

# Week 5: Deploy SparkSQL implementation to Dataproc
cd week-5 && python deploy_to_dataproc.py
```

### Dependency Installation
```bash
# Each week has its own requirements
cd week-X && pip install -r requirements.txt

# Or use virtual environment (recommended)
cd week-X && python -m venv env && source env/bin/activate && pip install -r requirements.txt
```

## Architecture Overview

### Technology Stack
- **Apache Spark/PySpark**: Primary big data processing framework for distributed computing
- **Google Cloud Platform**: Cloud infrastructure (Dataproc, GCS, Compute Engine)
- **Python 3.7+**: Primary development language with data science libraries
- **SparkSQL**: SQL interface for Spark DataFrame operations

### Project Structure Pattern
Each week follows this consistent structure:
```
week-X/
├── README.md                    # Comprehensive assignment documentation
├── requirements.txt            # Python dependencies
├── {main_implementation}.py    # Core assignment solution
├── test_{implementation}_local.py  # Local testing script
├── deploy_to_dataproc.py      # Cloud deployment automation (weeks 3-5)
├── data/                      # Input data files
└── archive/                   # Historical/reference files
```

### Data Processing Patterns
- **SCD Type II Implementation**: Slowly Changing Dimensions pattern for data warehousing (weeks 4-5)
- **DataFrame API vs SparkSQL**: Week 4 uses DataFrame operations, Week 5 uses pure SQL
- **Full Outer Joins**: Core pattern for identifying unchanged, changed, and new records
- **Window Functions**: Used for surrogate key generation and data partitioning
- **Cloud-First Design**: All solutions designed for local testing and cloud deployment

### Google Cloud Deployment Architecture
- **Dataproc Clusters**: Managed Spark clusters for job execution
- **GCS Storage**: Data lake storage for input data, scripts, and results
- **Automated Deployment**: Scripts handle cluster creation, job submission, and cleanup
- **Cost Optimization**: Auto-deletion policies and minimal cluster sizing

## Key Implementation Notes

### SCD Type II Logic (Weeks 4-5)
The core business logic identifies three types of records:
- **Unchanged**: Records with no attribute changes (preserved as-is)
- **Changed**: Records with modified attributes (old version closed, new version created)
- **New**: Completely new records (added with current effective dates)

### Spark Configuration
Standard configurations used across implementations:
```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Error Handling Patterns
- Comprehensive input validation and schema enforcement
- Graceful handling of cloud resource limitations and quotas
- Detailed logging for troubleshooting and monitoring
- Fallback strategies for deployment failures

### Testing Strategy
- **Local First**: All implementations tested locally before cloud deployment
- **Dependency Checking**: Automated verification of Java, PySpark, and cloud tools
- **Data Validation**: Schema and content validation for input files
- **Result Verification**: Automated validation of output correctness

## Development Workflow

1. **Local Development**: Implement and test using local Spark environment
2. **Dependency Management**: Install requirements in isolated virtual environments
3. **Input Validation**: Verify data files and schemas before processing
4. **Local Testing**: Run test scripts to validate logic
5. **Cloud Deployment**: Use deployment scripts for Dataproc execution
6. **Result Verification**: Download and validate cloud processing results

## Important Considerations

- **Java Dependency**: Spark requires Java 8+ to be installed and in PATH
- **Memory Management**: Large datasets may require cluster scaling or memory tuning
- **Cloud Quotas**: GCP projects have resource quotas that may limit cluster creation
- **Cost Management**: Dataproc clusters have auto-deletion policies to prevent runaway costs
- **Authentication**: Requires `gcloud auth login` and proper IAM permissions for cloud operations

## Memory Bank Context

The `memory-bank/` directory contains project context and progress tracking. These files provide background on assignment requirements and technical implementation decisions but are not part of the executable codebase.