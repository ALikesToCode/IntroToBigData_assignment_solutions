# Technical Context

## Core Technologies
- **Apache Spark**: Primary big data processing framework
- **PySpark**: Python API for Spark DataFrame and RDD operations
- **Google Cloud Dataproc**: Managed Spark cluster service
- **Google Cloud Storage (GCS)**: Data storage and file management
- **Python 3.7+**: Primary development language

## Development Patterns
- Local development with testing scripts
- Cloud deployment automation
- DataFrame operations preferred over RDD for complex logic
- Comprehensive error handling and logging
- Modular code structure with clear separation of concerns

## Project Structure Pattern
```
week-X/
├── README.md                    # Comprehensive documentation
├── requirements.txt            # Python dependencies
├── {main_script}.py           # Primary implementation
├── test_{script}_local.py     # Local testing
├── data/                      # Input data files
├── {notebook}.ipynb          # Analysis notebooks (when applicable)
└── archive/                   # Non-essential files after cleanup
```

## Cloud Deployment
- Google Cloud Dataproc clusters for Spark job execution
- GCS buckets for data storage and script deployment
- Automated cluster creation and job submission scripts
- Environment variable management for credentials

## Data Processing Patterns
- Input validation and error handling
- Distributed processing using Spark transformations
- Efficient data partitioning and caching strategies
- Result aggregation and output formatting

## Development Environment
- Java 8+ (Spark requirement)
- PySpark library installation
- Local Spark testing capabilities
- Google Cloud SDK for cluster management 