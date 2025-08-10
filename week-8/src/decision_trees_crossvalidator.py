#!/usr/bin/env python3
"""
Decision Trees with CrossValidator AutoTuner
Course: Introduction to Big Data - Week 8

This script converts the Scala MLlib decision trees code from Databricks to PySpark
with CrossValidator for automated hyperparameter tuning. It includes comprehensive
parameter grid search and performance analysis.

Original Source: https://docs.databricks.com/_extras/notebooks/source/decision-trees.html
Converted to: PySpark with CrossValidator AutoTuner

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import time
import logging
import argparse
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, IndexToString
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when, isnan, count, desc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DecisionTreeCrossValidatorAnalysis:
    """
    Decision Trees analysis with CrossValidator for automated hyperparameter tuning.
    """
    
    def __init__(self, app_name="DecisionTreeCrossValidator"):
        """
        Initialize Spark session and analysis framework.
        
        Args:
            app_name: Spark application name
        """
        self.app_name = app_name
        self.spark = self._create_spark_session()
        self.training_data = None
        self.test_data = None
        self.best_model = None
        self.cv_model = None
        self.predictions = None
        
        logger.info(f"✅ Decision Tree CrossValidator Analysis initialized")
        logger.info(f"   Spark version: {self.spark.version}")
    
    def _create_spark_session(self):
        """
        Create Spark session with optimized configurations.
        
        Returns:
            SparkSession object
        """
        try:
            spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            return spark
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            raise
    
    def load_data(self, data_format="libsvm", train_path=None, test_path=None):
        """
        Load training and test datasets.
        
        Args:
            data_format: Data format (libsvm, csv, parquet)
            train_path: Path to training data
            test_path: Path to test data
        """
        try:
            logger.info(f"📖 Loading data in {data_format} format...")
            
            if data_format == "libsvm":
                # Check if we need to load MNIST from library
                if train_path is None or test_path is None:
                    # Load MNIST from ML library (sklearn or keras)
                    logger.info("📥 Loading MNIST dataset from ML library...")
                    train_path, test_path = self._load_mnist_from_library()
                
                logger.info(f"   Training data: {train_path}")
                logger.info(f"   Test data: {test_path}")
                
                try:
                    self.training_data = self.spark.read.format("libsvm").load(train_path)
                    self.test_data = self.spark.read.format("libsvm").load(test_path)
                except Exception as e:
                    logger.warning(f"⚠️ Could not load datasets: {e}")
                    logger.info("📝 Generating synthetic dataset instead...")
                    self._generate_synthetic_data()
                    return
            
            elif data_format == "csv":
                # Load CSV data with headers
                self.training_data = self.spark.read.option("header", "true").csv(train_path)
                self.test_data = self.spark.read.option("header", "true").csv(test_path)
            
            elif data_format == "parquet":
                # Load Parquet data
                self.training_data = self.spark.read.parquet(train_path)
                self.test_data = self.spark.read.parquet(test_path)
            
            # Cache datasets for better performance
            self.training_data.cache()
            self.test_data.cache()
            
            # Display basic statistics
            train_count = self.training_data.count()
            test_count = self.test_data.count()
            
            logger.info(f"✅ Data loaded successfully")
            logger.info(f"   Training samples: {train_count:,}")
            logger.info(f"   Test samples: {test_count:,}")
            
            # Show schema and sample data
            logger.info(f"📋 Training data schema:")
            self.training_data.printSchema()
            
            logger.info(f"📋 Sample training data:")
            self.training_data.show(5, truncate=False)
            
            # Analyze label distribution
            self._analyze_label_distribution()
            
        except Exception as e:
            logger.error(f"❌ Failed to load data: {e}")
            raise
    
    def _load_mnist_from_library(self):
        """
        Load MNIST dataset using ML libraries and convert to LibSVM format.
        """
        import os
        import numpy as np
        
        try:
            logger.info("📥 Loading MNIST dataset from ML library...")
            
            # Try Keras/TensorFlow first (more reliable on cloud)
            try:
                logger.info("   Using Keras/TensorFlow...")
                import tensorflow as tf
                (X_train, y_train), (X_test, y_test) = tf.keras.datasets.mnist.load_data()
                
                # Reshape and normalize
                X_train = X_train.reshape(X_train.shape[0], -1).astype(float)
                X_test = X_test.reshape(X_test.shape[0], -1).astype(float)
                
                logger.info(f"   ✅ Loaded MNIST via Keras: {X_train.shape[0]:,} train, {X_test.shape[0]:,} test")
                
            except ImportError:
                logger.info("   TensorFlow not available, trying sklearn...")
                
                # Fallback to sklearn
                from sklearn.datasets import fetch_openml
                logger.info("   Using sklearn fetch_openml...")
                
                # Load MNIST dataset
                mnist = fetch_openml('mnist_784', version=1, as_frame=False)
                X, y = mnist.data, mnist.target.astype(int)
                
                logger.info(f"   ✅ Loaded MNIST: {X.shape[0]:,} samples, {X.shape[1]} features")
                
                # Split into train/test (first 60k for train, rest for test)
                train_size = 60000
                X_train, X_test = X[:train_size], X[train_size:]
                y_train, y_test = y[:train_size], y[train_size:]
                
                logger.info(f"   Training samples: {X_train.shape[0]:,}")
                logger.info(f"   Test samples: {X_test.shape[0]:,}")
            
            # Create temporary files in LibSVM format
            data_dir = "/tmp/mnist_data"
            os.makedirs(data_dir, exist_ok=True)
            
            train_path = os.path.join(data_dir, "mnist_train.txt")
            test_path = os.path.join(data_dir, "mnist_test.txt")
            
            # Convert to LibSVM format and save
            self._save_as_libsvm(X_train, y_train, train_path)
            self._save_as_libsvm(X_test, y_test, test_path)
            
            logger.info(f"   ✅ Converted to LibSVM format")
            logger.info(f"   Training file: {train_path}")
            logger.info(f"   Test file: {test_path}")
            
            return train_path, test_path
            
        except Exception as e:
            logger.error(f"   Failed to load MNIST from library: {e}")
            raise
    
    def _save_as_libsvm(self, X, y, filepath):
        """
        Save data in LibSVM format.
        
        Args:
            X: Feature matrix
            y: Labels
            filepath: Output file path
        """
        with open(filepath, 'w') as f:
            for i in range(len(X)):
                label = y[i]
                features = []
                
                # Only include non-zero features for LibSVM format
                for j, value in enumerate(X[i]):
                    if value != 0:
                        features.append(f"{j+1}:{value}")
                
                # Write in LibSVM format: label feature1:value1 feature2:value2 ...
                f.write(f"{label} {' '.join(features)}\n")
    
    def _generate_synthetic_data(self):
        """
        Generate synthetic classification data for demonstration.
        """
        try:
            logger.info("🔧 Generating synthetic classification dataset...")
            
            from pyspark.ml.linalg import Vectors
            import random
            
            # Set random seed for reproducibility
            random.seed(42)
            
            # Generate synthetic data
            num_features = 20
            train_samples = 5000  # Production size
            test_samples = 1000    # Production size
            num_classes = 5
            
            def generate_sample():
                # Generate random features
                features = [random.gauss(0, 1) for _ in range(num_features)]
                
                # Create label based on feature combinations (for realistic relationships)
                label = 0
                if features[0] + features[1] > 0:
                    label += 1
                if features[2] * features[3] > 0.5:
                    label += 1
                if sum(features[4:8]) > 0:
                    label += 1
                if features[8] > features[9]:
                    label += 1
                
                label = label % num_classes
                
                return (float(label), Vectors.dense(features))
            
            # Generate training data
            train_data = [generate_sample() for _ in range(train_samples)]
            test_data = [generate_sample() for _ in range(test_samples)]
            
            # Create DataFrames - PySpark will automatically infer the schema
            self.training_data = self.spark.createDataFrame(train_data, ["label", "features"])
            self.test_data = self.spark.createDataFrame(test_data, ["label", "features"])
            
            # Cache datasets
            self.training_data.cache()
            self.test_data.cache()
            
            logger.info(f"✅ Synthetic data generated")
            logger.info(f"   Training samples: {train_samples:,}")
            logger.info(f"   Test samples: {test_samples:,}")
            logger.info(f"   Features: {num_features}")
            logger.info(f"   Classes: {num_classes}")
            
        except Exception as e:
            logger.error(f"❌ Failed to generate synthetic data: {e}")
            raise
    
    def _analyze_label_distribution(self):
        """
        Analyze and display label distribution in the training data.
        """
        try:
            logger.info("📊 Analyzing label distribution...")
            
            # Count labels
            label_counts = self.training_data.groupBy("label").count().orderBy("label")
            
            logger.info("   Label distribution:")
            label_counts.show()
            
            # Calculate class balance
            total_samples = self.training_data.count()
            label_stats = label_counts.collect()
            
            for row in label_stats:
                label = row['label']
                count = row['count']
                percentage = (count / total_samples) * 100
                logger.info(f"     Label {label}: {count:,} samples ({percentage:.1f}%)")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not analyze label distribution: {e}")
    
    def setup_pipeline(self):
        """
        Set up the ML pipeline with feature processing and decision tree classifier.
        
        Returns:
            Pipeline object
        """
        try:
            logger.info("🔧 Setting up ML pipeline...")
            
            # Index categorical features automatically
            feature_indexer = VectorIndexer() \
                .setInputCol("features") \
                .setOutputCol("indexedFeatures") \
                .setMaxCategories(10)  # Features with > 10 distinct values are continuous
            
            # Decision Tree Classifier
            decision_tree = DecisionTreeClassifier() \
                .setLabelCol("label") \
                .setFeaturesCol("indexedFeatures") \
                .setPredictionCol("prediction") \
                .setProbabilityCol("probability") \
                .setRawPredictionCol("rawPrediction") \
                .setSeed(42)  # For reproducibility
            
            # Create pipeline (simplified without label indexing since we have numeric labels)
            pipeline = Pipeline(stages=[
                feature_indexer,
                decision_tree
            ])
            
            logger.info("✅ ML pipeline configured")
            logger.info("   Stages: Feature Indexer → Decision Tree")
            
            return pipeline, decision_tree
            
        except Exception as e:
            logger.error(f"❌ Failed to setup pipeline: {e}")
            raise
    
    def create_parameter_grid(self, decision_tree):
        """
        Create comprehensive parameter grid for hyperparameter tuning.
        
        Args:
            decision_tree: DecisionTreeClassifier instance
            
        Returns:
            Parameter grid for CrossValidator
        """
        try:
            logger.info("🎛️ Creating parameter grid for hyperparameter tuning...")
            
            # Comprehensive parameter grid
            param_grid = ParamGridBuilder() \
                .addGrid(decision_tree.maxDepth, [3, 5, 7, 10, 15]) \
                .addGrid(decision_tree.minInstancesPerNode, [1, 5, 10, 20]) \
                .addGrid(decision_tree.minInfoGain, [0.0, 0.01, 0.05]) \
                .addGrid(decision_tree.impurity, ["gini", "entropy"]) \
                .addGrid(decision_tree.maxBins, [16, 32, 64]) \
                .build()
            
            logger.info(f"✅ Parameter grid created")
            logger.info(f"   Total parameter combinations: {len(param_grid)}")
            logger.info(f"   Parameters to tune:")
            logger.info(f"     - maxDepth: [3, 5, 7, 10, 15]")
            logger.info(f"     - minInstancesPerNode: [1, 5, 10, 20]")
            logger.info(f"     - minInfoGain: [0.0, 0.01, 0.05, 0.1]")
            logger.info(f"     - impurity: ['gini', 'entropy']")
            logger.info(f"     - maxBins: [16, 32, 64]")
            
            return param_grid
            
        except Exception as e:
            logger.error(f"❌ Failed to create parameter grid: {e}")
            raise
    
    def train_with_crossvalidator(self, pipeline, param_grid, num_folds=5):
        """
        Train model using CrossValidator for automated hyperparameter tuning.
        
        Args:
            pipeline: ML pipeline
            param_grid: Parameter grid for tuning
            num_folds: Number of cross-validation folds
            
        Returns:
            Trained CrossValidator model
        """
        try:
            logger.info(f"🚀 Starting CrossValidator training...")
            logger.info(f"   Cross-validation folds: {num_folds}")
            logger.info(f"   Parameter combinations: {len(param_grid)}")
            logger.info(f"   Total training runs: {len(param_grid) * num_folds}")
            
            # Create evaluator
            evaluator = MulticlassClassificationEvaluator() \
                .setLabelCol("label") \
                .setPredictionCol("prediction") \
                .setMetricName("accuracy")
            
            # Create CrossValidator
            cross_validator = CrossValidator(
                estimator=pipeline,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                numFolds=num_folds,
                parallelism=2,  # Number of parallel threads
                seed=42  # For reproducibility
            )
            
            # Train model
            start_time = time.time()
            logger.info("⏳ Training in progress... This may take several minutes.")
            
            self.cv_model = cross_validator.fit(self.training_data)
            
            training_time = time.time() - start_time
            
            logger.info(f"✅ CrossValidator training completed")
            logger.info(f"   Training time: {training_time:.2f} seconds")
            logger.info(f"   Training time per combination: {training_time / len(param_grid):.2f} seconds")
            
            # Get best model
            self.best_model = self.cv_model.bestModel
            
            # Extract best parameters
            self._analyze_best_parameters()
            
            return self.cv_model
            
        except Exception as e:
            logger.error(f"❌ CrossValidator training failed: {e}")
            raise
    
    def _analyze_best_parameters(self):
        """
        Analyze and report the best hyperparameters found by CrossValidator.
        """
        try:
            logger.info("🏆 Analyzing best model parameters...")
            
            # Extract decision tree from the best pipeline
            best_dt = None
            for stage in self.best_model.stages:
                if hasattr(stage, 'getMaxDepth'):  # Decision tree stage
                    best_dt = stage
                    break
            
            if best_dt is None:
                logger.warning("⚠️ Could not extract decision tree from best model")
                return
            
            # Get best parameters
            best_params = {
                'maxDepth': best_dt.getMaxDepth(),
                'minInstancesPerNode': best_dt.getMinInstancesPerNode(),
                'minInfoGain': best_dt.getMinInfoGain(),
                'impurity': best_dt.getImpurity(),
                'maxBins': best_dt.getMaxBins()
            }
            
            logger.info(f"🎯 Best hyperparameters found:")
            for param, value in best_params.items():
                logger.info(f"   {param}: {value}")
            
            # Get cross-validation metrics
            avg_metrics = self.cv_model.avgMetrics
            best_metric = max(avg_metrics)
            best_metric_idx = avg_metrics.index(best_metric)
            
            logger.info(f"📊 Cross-validation results:")
            logger.info(f"   Best CV accuracy: {best_metric:.4f}")
            logger.info(f"   Best parameter set index: {best_metric_idx}")
            logger.info(f"   CV accuracy range: {min(avg_metrics):.4f} - {max(avg_metrics):.4f}")
            logger.info(f"   CV accuracy std: {self._calculate_std(avg_metrics):.4f}")
            
            return best_params
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze best parameters: {e}")
            return None
    
    def _calculate_std(self, values):
        """Calculate standard deviation of a list of values."""
        if len(values) <= 1:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
    
    def evaluate_model(self):
        """
        Evaluate the best model on test data and provide comprehensive metrics.
        """
        try:
            logger.info("📊 Evaluating best model on test data...")
            
            if self.best_model is None:
                logger.error("❌ No trained model available for evaluation")
                return None
            
            # Make predictions on test data
            start_time = time.time()
            self.predictions = self.best_model.transform(self.test_data)
            prediction_time = time.time() - start_time
            
            # Cache predictions for multiple evaluations
            self.predictions.cache()
            
            test_count = self.test_data.count()
            
            logger.info(f"✅ Predictions completed")
            logger.info(f"   Test samples: {test_count:,}")
            logger.info(f"   Prediction time: {prediction_time:.2f} seconds")
            logger.info(f"   Predictions per second: {test_count / prediction_time:.0f}")
            
            # Calculate multiple metrics
            metrics = self._calculate_comprehensive_metrics()
            
            # Show sample predictions
            logger.info(f"📋 Sample predictions:")
            self.predictions.select("label", "prediction", "probability") \
                .show(10, truncate=False)
            
            # Confusion matrix analysis
            self._analyze_confusion_matrix()
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Model evaluation failed: {e}")
            return None
    
    def _calculate_comprehensive_metrics(self):
        """
        Calculate comprehensive evaluation metrics.
        
        Returns:
            Dictionary of metrics
        """
        try:
            logger.info("🧮 Calculating comprehensive evaluation metrics...")
            
            # Create evaluators for different metrics
            evaluators = {
                'accuracy': MulticlassClassificationEvaluator(
                    labelCol="label", predictionCol="prediction", metricName="accuracy"
                ),
                'f1': MulticlassClassificationEvaluator(
                    labelCol="label", predictionCol="prediction", metricName="f1"
                ),
                'weightedPrecision': MulticlassClassificationEvaluator(
                    labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
                ),
                'weightedRecall': MulticlassClassificationEvaluator(
                    labelCol="label", predictionCol="prediction", metricName="weightedRecall"
                )
            }
            
            # Calculate metrics
            metrics = {}
            for metric_name, evaluator in evaluators.items():
                metrics[metric_name] = evaluator.evaluate(self.predictions)
            
            # Display metrics
            logger.info(f"📈 Test Set Performance Metrics:")
            for metric_name, value in metrics.items():
                logger.info(f"   {metric_name}: {value:.4f}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate metrics: {e}")
            return {}
    
    def _analyze_confusion_matrix(self):
        """
        Analyze and display confusion matrix.
        """
        try:
            logger.info("🔍 Analyzing confusion matrix...")
            
            # Create confusion matrix
            confusion_matrix = self.predictions.groupBy("label", "prediction").count()
            
            logger.info("   Confusion Matrix:")
            confusion_matrix.orderBy("label", "prediction").show()
            
            # Calculate per-class metrics
            total_predictions = self.predictions.count()
            correct_predictions = self.predictions.filter(col("label") == col("prediction")).count()
            
            overall_accuracy = correct_predictions / total_predictions
            logger.info(f"   Overall Accuracy: {overall_accuracy:.4f} ({correct_predictions}/{total_predictions})")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not analyze confusion matrix: {e}")
    
    def analyze_parameter_importance(self):
        """
        Analyze the importance of different hyperparameters.
        """
        try:
            logger.info("🔬 Analyzing hyperparameter importance...")
            
            if not hasattr(self.cv_model, 'avgMetrics'):
                logger.warning("⚠️ Cross-validation metrics not available")
                return
            
            # Get all metrics and parameters
            avg_metrics = self.cv_model.avgMetrics
            param_maps = self.cv_model.getEstimatorParamMaps()
            
            # Analyze parameter impact
            param_analysis = {}
            
            for i, (metric, param_map) in enumerate(zip(avg_metrics, param_maps)):
                for param, value in param_map.items():
                    param_name = param.name
                    if param_name not in param_analysis:
                        param_analysis[param_name] = []
                    param_analysis[param_name].append((value, metric))
            
            # Display parameter analysis
            logger.info("📊 Parameter Impact Analysis:")
            for param_name, value_metrics in param_analysis.items():
                # Group by parameter value and calculate average metric
                value_groups = {}
                for value, metric in value_metrics:
                    if value not in value_groups:
                        value_groups[value] = []
                    value_groups[value].append(metric)
                
                logger.info(f"   {param_name}:")
                for value in sorted(value_groups.keys()):
                    metrics = value_groups[value]
                    avg_metric = sum(metrics) / len(metrics)
                    logger.info(f"     {value}: {avg_metric:.4f} (avg from {len(metrics)} runs)")
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze parameter importance: {e}")
    
    def generate_model_report(self, output_path=None):
        """
        Generate comprehensive model analysis report.
        
        Args:
            output_path: Path to save the report (optional)
        """
        try:
            logger.info("📝 Generating comprehensive model report...")
            
            report_lines = []
            report_lines.append("=" * 80)
            report_lines.append("DECISION TREE CROSSVALIDATOR ANALYSIS REPORT")
            report_lines.append("=" * 80)
            report_lines.append(f"Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"Spark Version: {self.spark.version}")
            report_lines.append("")
            
            # Dataset information
            if self.training_data and self.test_data:
                train_count = self.training_data.count()
                test_count = self.test_data.count()
                report_lines.append("DATASET INFORMATION")
                report_lines.append("-" * 40)
                report_lines.append(f"Training samples: {train_count:,}")
                report_lines.append(f"Test samples: {test_count:,}")
                report_lines.append(f"Total samples: {train_count + test_count:,}")
                report_lines.append("")
            
            # Best model parameters
            if self.best_model:
                report_lines.append("BEST MODEL PARAMETERS")
                report_lines.append("-" * 40)
                best_params = self._analyze_best_parameters()
                if best_params:
                    for param, value in best_params.items():
                        report_lines.append(f"{param}: {value}")
                report_lines.append("")
            
            # Performance metrics
            if self.predictions:
                report_lines.append("PERFORMANCE METRICS")
                report_lines.append("-" * 40)
                metrics = self._calculate_comprehensive_metrics()
                for metric_name, value in metrics.items():
                    report_lines.append(f"{metric_name}: {value:.4f}")
                report_lines.append("")
            
            # Cross-validation results
            if hasattr(self.cv_model, 'avgMetrics'):
                report_lines.append("CROSS-VALIDATION RESULTS")
                report_lines.append("-" * 40)
                avg_metrics = self.cv_model.avgMetrics
                report_lines.append(f"Best CV accuracy: {max(avg_metrics):.4f}")
                report_lines.append(f"Worst CV accuracy: {min(avg_metrics):.4f}")
                report_lines.append(f"CV accuracy std: {self._calculate_std(avg_metrics):.4f}")
                report_lines.append(f"Total parameter combinations tested: {len(avg_metrics)}")
                report_lines.append("")
            
            report_lines.append("=" * 80)
            
            # Print report
            report_text = "\n".join(report_lines)
            print("\n" + report_text)
            
            # Save report if path provided
            if output_path:
                output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w') as f:
                    f.write(report_text)
                
                logger.info(f"📄 Report saved to: {output_path}")
            
            return report_text
            
        except Exception as e:
            logger.error(f"❌ Failed to generate report: {e}")
            return None
    
    def stop(self):
        """
        Stop Spark session and clean up resources.
        """
        try:
            logger.info("🔒 Stopping Spark session...")
            if self.spark:
                self.spark.stop()
            logger.info("✅ Spark session stopped")
        except Exception as e:
            logger.error(f"❌ Error stopping Spark session: {e}")

def main():
    """
    Main function to run Decision Tree CrossValidator analysis.
    """
    parser = argparse.ArgumentParser(description='Decision Tree CrossValidator Analysis')
    parser.add_argument('--data-format', default='libsvm', choices=['libsvm', 'csv', 'parquet'],
                       help='Input data format (default: libsvm)')
    parser.add_argument('--train-path',
                       help='Path to training data file (local, HDFS, or gs://)')
    parser.add_argument('--test-path',
                       help='Path to test data file (local, HDFS, or gs://)')
    parser.add_argument('--cv-folds', type=int, default=5,
                       help='Number of cross-validation folds (default: 5)')
    parser.add_argument('--output-report',
                       help='Path to save analysis report')
    parser.add_argument('--auto-download', action='store_true',
                       help='Automatically download MNIST dataset if not available')
    parser.add_argument('--gcs-bucket',
                       help='GCS bucket for storing downloaded data')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    analyzer = None
    
    try:
        # Create analyzer
        analyzer = DecisionTreeCrossValidatorAnalysis()
        
        # If GCS bucket specified and no paths given, use GCS paths
        if args.gcs_bucket and not args.train_path:
            args.train_path = f"gs://{args.gcs_bucket}/mnist/mnist_train.txt"
            args.test_path = f"gs://{args.gcs_bucket}/mnist/mnist_test.txt"
            logger.info(f"Using GCS paths from bucket: {args.gcs_bucket}")
        
        # Load data
        analyzer.load_data(
            data_format=args.data_format,
            train_path=args.train_path,
            test_path=args.test_path
        )
        
        # Setup pipeline
        pipeline, decision_tree = analyzer.setup_pipeline()
        
        # Create parameter grid
        param_grid = analyzer.create_parameter_grid(decision_tree)
        
        # Train with CrossValidator
        cv_model = analyzer.train_with_crossvalidator(
            pipeline=pipeline,
            param_grid=param_grid,
            num_folds=args.cv_folds
        )
        
        # Evaluate model
        metrics = analyzer.evaluate_model()
        
        # Analyze parameter importance
        analyzer.analyze_parameter_importance()
        
        # Generate comprehensive report
        analyzer.generate_model_report(output_path=args.output_report)
        
        logger.info("🎉 Analysis completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("👋 Analysis interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Analysis failed: {e}")
        sys.exit(1)
    finally:
        if analyzer:
            analyzer.stop()

if __name__ == '__main__':
    main()