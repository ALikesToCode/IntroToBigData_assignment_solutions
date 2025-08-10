#!/usr/bin/env python3
"""
Decision Trees with CrossValidator AutoTuner - FAST VERSION
Course: Introduction to Big Data - Week 8

This is a fast demo version that completes in ~1 minute with reduced dataset and parameters.

Author: Abhyudaya B Tharakan 22f3001492
Date: August 2025
"""

import time
import logging
import argparse
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when, isnan, count, desc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FastDecisionTreeCrossValidator:
    """
    Fast Decision Trees analysis with CrossValidator - reduced dataset and parameters.
    """
    
    def __init__(self, app_name="FastDecisionTreeCrossValidator"):
        """Initialize Spark session."""
        self.app_name = app_name
        self.spark = self._create_spark_session()
        self.data_train = None
        self.data_test = None
        
    def _create_spark_session(self):
        """Create optimized Spark session for fast execution."""
        logger.info("🚀 Creating Spark session...")
        return SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
    
    def load_data(self, train_path, test_path, data_format="libsvm"):
        """Load training and testing data."""
        logger.info(f"📂 Loading data from:")
        logger.info(f"   • Training: {train_path}")
        logger.info(f"   • Testing: {test_path}")
        
        start_time = time.time()
        
        if data_format == "libsvm":
            self.data_train = self.spark.read.format("libsvm").load(train_path)
            self.data_test = self.spark.read.format("libsvm").load(test_path)
        else:
            raise ValueError(f"Unsupported data format: {data_format}")
        
        # Cache for faster access
        self.data_train.cache()
        self.data_test.cache()
        
        # Get counts
        train_count = self.data_train.count()
        test_count = self.data_test.count()
        
        load_time = time.time() - start_time
        logger.info(f"✅ Data loaded in {load_time:.1f}s:")
        logger.info(f"   • Training samples: {train_count:,}")
        logger.info(f"   • Test samples: {test_count:,}")
        
        return self.data_train, self.data_test
    
    def create_pipeline(self):
        """Create ML pipeline with minimal preprocessing."""
        logger.info("🔧 Creating ML pipeline...")
        
        # Simplified pipeline - no label indexing needed for numeric labels
        dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
        
        # Simple pipeline
        pipeline = Pipeline(stages=[dt])
        
        logger.info("✅ Pipeline created successfully")
        return pipeline
    
    def create_fast_param_grid(self, pipeline):
        """Create reduced parameter grid for fast execution."""
        logger.info("⚙️ Creating FAST parameter grid...")
        
        dt = pipeline.getStages()[0]  # DecisionTreeClassifier
        
        # Minimal parameter grid for speed
        param_grid = ParamGridBuilder() \
            .addGrid(dt.maxDepth, [3, 5]) \
            .addGrid(dt.minInstancesPerNode, [5, 10]) \
            .build()
        
        logger.info(f"📊 Parameter grid created:")
        logger.info(f"   • Total combinations: {len(param_grid)}")
        logger.info(f"   • Max depth values: [3, 5]")
        logger.info(f"   • Min instances per node: [5, 10]")
        
        return param_grid
    
    def train_with_crossvalidator(self, pipeline, param_grid, num_folds=2):
        """Train models using CrossValidator with minimal folds for speed."""
        logger.info("🚀 Starting FAST CrossValidator training...")
        
        start_time = time.time()
        
        # Create evaluator
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction", 
            metricName="accuracy"
        )
        
        # Create CrossValidator with minimal folds
        crossval = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=num_folds,
            parallelism=2  # Parallel execution
        )
        
        logger.info(f"📊 Training Progress:")
        logger.info(f"   • Parameter combinations: {len(param_grid)}")
        logger.info(f"   • Cross-validation folds: {num_folds}")
        logger.info(f"   • Total training runs: {len(param_grid) * num_folds}")
        
        # Fit the model
        logger.info("🏃‍♂️ Running CrossValidator...")
        cv_model = crossval.fit(self.data_train)
        
        training_time = time.time() - start_time
        logger.info(f"✅ CrossValidator completed in {training_time:.1f}s")
        
        return cv_model, evaluator
    
    def evaluate_model(self, cv_model, evaluator):
        """Evaluate the best model and show results."""
        logger.info("📊 Evaluating best model...")
        
        start_time = time.time()
        
        # Get best model
        best_model = cv_model.bestModel
        best_dt = best_model.stages[0]  # DecisionTreeClassifier
        
        # Make predictions on test set
        predictions = best_model.transform(self.data_test)
        
        # Calculate accuracy
        accuracy = evaluator.evaluate(predictions)
        
        eval_time = time.time() - start_time
        
        logger.info(f"🎯 FAST DEMO RESULTS:")
        logger.info(f"=" * 50)
        logger.info(f"📊 BEST MODEL PARAMETERS:")
        logger.info(f"   • Max Depth: {best_dt.getMaxDepth()}")
        logger.info(f"   • Min Instances Per Node: {best_dt.getMinInstancesPerNode()}")
        logger.info(f"   • Min Info Gain: {best_dt.getMinInfoGain()}")
        logger.info(f"   • Impurity: {best_dt.getImpurity()}")
        
        logger.info(f"📈 PERFORMANCE METRICS:")
        logger.info(f"   • Test Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
        logger.info(f"   • Evaluation Time: {eval_time:.1f}s")
        
        # Show sample predictions
        logger.info(f"📋 SAMPLE PREDICTIONS:")
        sample_predictions = predictions.select("label", "prediction", "probability").limit(10)
        sample_predictions.show(10, truncate=False)
        
        logger.info(f"=" * 50)
        logger.info(f"✅ FAST DEMO COMPLETE!")
        
        return accuracy, best_model

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Fast Decision Tree CrossValidator Demo")
    parser.add_argument("--train-data", default="gs://steady-triumph-447006-f8-week8-ml/data/mnist_train_fast.txt", 
                       help="Training data path")
    parser.add_argument("--test-data", default="gs://steady-triumph-447006-f8-week8-ml/data/mnist_test_fast.txt",
                       help="Test data path")
    parser.add_argument("--data-format", default="libsvm", choices=["libsvm"],
                       help="Data format")
    parser.add_argument("--cv-folds", type=int, default=2,
                       help="Number of cross-validation folds")
    parser.add_argument("--output-report", 
                       default="gs://steady-triumph-447006-f8-week8-ml/output/fast_demo_report.txt",
                       help="Output report path")
    
    args = parser.parse_args()
    
    overall_start = time.time()
    logger.info("🚀 Starting Fast Decision Tree CrossValidator Demo")
    logger.info("=" * 60)
    
    try:
        # Initialize analyzer
        analyzer = FastDecisionTreeCrossValidator()
        
        # Load data
        train_data, test_data = analyzer.load_data(
            args.train_data, 
            args.test_data, 
            args.data_format
        )
        
        # Create pipeline
        pipeline = analyzer.create_pipeline()
        
        # Create parameter grid
        param_grid = analyzer.create_fast_param_grid(pipeline)
        
        # Train with CrossValidator
        cv_model, evaluator = analyzer.train_with_crossvalidator(
            pipeline, param_grid, args.cv_folds
        )
        
        # Evaluate model
        accuracy, best_model = analyzer.evaluate_model(cv_model, evaluator)
        
        # Generate report
        total_time = time.time() - overall_start
        
        report = f"""
FAST DECISION TREE CROSSVALIDATOR DEMO REPORT
=============================================
Execution Time: {total_time:.1f} seconds
Training Data: {args.train_data}
Test Data: {args.test_data}

BEST MODEL PARAMETERS:
- Max Depth: {best_model.stages[0].getMaxDepth()}
- Min Instances Per Node: {best_model.stages[0].getMinInstancesPerNode()}
- Min Info Gain: {best_model.stages[0].getMinInfoGain()}
- Impurity: {best_model.stages[0].getImpurity()}

PERFORMANCE:
- Test Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)

DATASET INFO:
- Training samples: {train_data.count():,}
- Test samples: {test_data.count():,}
- Cross-validation folds: {args.cv_folds}

This is a fast demo version with reduced dataset and parameters
for quick validation of CrossValidator functionality.
"""
        
        # Save report to GCS
        if args.output_report:
            logger.info(f"💾 Saving report to: {args.output_report}")
            report_rdd = analyzer.spark.sparkContext.parallelize([report])
            report_rdd.saveAsTextFile(args.output_report)
            
        logger.info(f"🏁 DEMO COMPLETED in {total_time:.1f} seconds!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Error during execution: {str(e)}")
        sys.exit(1)
    
    finally:
        # Clean up
        if 'analyzer' in locals():
            analyzer.spark.stop()

if __name__ == "__main__":
    main()