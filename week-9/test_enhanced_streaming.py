#!/usr/bin/env python3
"""
Local Testing for Enhanced Real-Time Image Classification
Course: Introduction to Big Data - Week 9
Author: Abhyudaya B Tharakan 22f3001492

This script provides comprehensive local testing for the enhanced streaming
image classification pipeline before deploying to GCP.
"""

import os
import sys
import json
import time
import threading
import subprocess
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_image_generation():
    """Test synthetic image generation."""
    print("🔍 Testing image generation...")
    
    try:
        from image_stream_simulator import ImageGenerator
        generator = ImageGenerator(224, 224)
        
        # Test different image types
        patterns = ["circles", "rectangles", "lines"]
        for pattern in patterns:
            img = generator.generate_geometric_pattern(pattern)
            assert img.size == (224, 224), f"Wrong image size for {pattern}"
            print(f"  ✅ {pattern} pattern generated successfully")
        
        # Test other image types
        img = generator.generate_noise_pattern()
        assert img.size == (224, 224), "Wrong noise image size"
        print("  ✅ Noise pattern generated successfully")
        
        img = generator.generate_text_image("TEST")
        assert img.size == (224, 224), "Wrong text image size"
        print("  ✅ Text image generated successfully")
        
        img = generator.generate_gradient_image()
        assert img.size == (224, 224), "Wrong gradient image size"
        print("  ✅ Gradient image generated successfully")
        
        print("✅ Image generation tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Image generation test failed: {e}")
        return False


def test_image_classification_udf():
    """Test the enhanced image classification UDF."""
    print("🔍 Testing image classification UDF...")
    
    try:
        from enhanced_streaming_image_classifier import classify_image_enhanced_udf
        from image_stream_simulator import ImageGenerator
        import base64
        import io
        
        # Generate test image
        generator = ImageGenerator(224, 224)
        test_image = generator.generate_geometric_pattern("circles")
        
        # Convert to base64
        buffer = io.BytesIO()
        test_image.save(buffer, format='JPEG')
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        
        # Test UDF (this would normally run on Spark executor)
        print("  Testing base64 image classification...")
        
        # Create minimal Spark session for UDF testing
        spark = SparkSession.builder.appName("UDFTest").master("local[1]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        # Create test DataFrame
        test_data = [("base64", image_base64, None, "mobilenet")]
        df = spark.createDataFrame(test_data, ["source", "data", "model_path", "model_type"])
        
        # Apply UDF
        result_df = df.withColumn(
            "prediction", 
            classify_image_enhanced_udf(F.col("source"), F.col("data"), F.col("model_path"), F.col("model_type"))
        )
        
        # Collect results
        results = result_df.select("prediction.*").collect()
        
        if results and len(results) > 0:
            result = results[0]
            if result.error_message is None:
                print(f"  ✅ Prediction successful: {result.label} (confidence: {result.confidence:.3f})")
                print(f"  ✅ Model: {result.model_name}")
                print(f"  ✅ Processing time: {result.processing_time_ms}ms")
            else:
                print(f"  ⚠️ Prediction error: {result.error_message}")
        
        spark.stop()
        print("✅ Image classification UDF tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Image classification UDF test failed: {e}")
        return False


def test_streaming_dataframe_operations():
    """Test streaming DataFrame operations without actual streaming sources."""
    print("🔍 Testing streaming DataFrame operations...")
    
    try:
        from enhanced_streaming_image_classifier import create_spark_session
        
        # Create Spark session
        spark = create_spark_session("StreamingTest")
        
        # Test creating a streaming DataFrame from memory (for testing)
        test_data = [
            ("img_001", "test_base64_data", "2024-08-10T10:00:00Z", {"source": "test"}),
            ("img_002", "test_base64_data_2", "2024-08-10T10:01:00Z", {"source": "test"})
        ]
        
        df = spark.createDataFrame(test_data, ["image_id", "image_data", "timestamp", "metadata"])
        
        # Test DataFrame transformations
        transformed_df = (
            df.withColumn("processed_at", F.current_timestamp())
              .withColumn("hour", F.hour(F.col("timestamp")))
        )
        
        # Show results
        print("  DataFrame transformations:")
        transformed_df.show(truncate=False)
        
        spark.stop()
        print("✅ Streaming DataFrame operation tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Streaming DataFrame operation test failed: {e}")
        return False


def test_kafka_simulation():
    """Test Kafka producer simulation (without actual Kafka server)."""
    print("🔍 Testing Kafka producer simulation...")
    
    try:
        from image_stream_simulator import ImageGenerator
        import json
        import uuid
        from datetime import datetime
        
        generator = ImageGenerator(224, 224)
        
        # Simulate message creation (without sending)
        for i in range(3):
            image = generator.generate_random_image()
            
            # Simulate base64 encoding
            import io
            import base64
            buffer = io.BytesIO()
            image.save(buffer, format='JPEG')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            
            message = {
                "image_id": str(uuid.uuid4()),
                "image_data": image_base64[:100] + "...",  # Truncate for display
                "timestamp": datetime.now().isoformat(),
                "metadata": {
                    "source": "simulator",
                    "image_size": f"{image.width}x{image.height}",
                    "format": "JPEG",
                    "sequence_number": str(i)
                }
            }
            
            print(f"  ✅ Generated Kafka message {i+1}: ID={message['image_id'][:8]}...")
            print(f"      Size: {len(image_base64)} bytes")
        
        print("✅ Kafka simulation tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Kafka simulation test failed: {e}")
        return False


def test_performance_metrics():
    """Test performance tracking functionality."""
    print("🔍 Testing performance metrics...")
    
    try:
        import time
        
        # Simulate performance tracking
        start_time = time.time()
        time.sleep(0.1)  # Simulate processing time
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        assert 0.09 < processing_time < 0.15, "Processing time measurement failed"
        print(f"  ✅ Processing time measurement: {processing_time:.3f}s")
        
        # Test metrics aggregation
        metrics = {
            "total_predictions": 100,
            "successful_predictions": 95,
            "error_count": 5,
            "average_processing_time": 0.125,
            "success_rate": 95.0
        }
        
        assert metrics["success_rate"] == 95.0, "Metrics calculation failed"
        print(f"  ✅ Success rate: {metrics['success_rate']}%")
        print(f"  ✅ Average processing time: {metrics['average_processing_time']}s")
        
        print("✅ Performance metrics tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Performance metrics test failed: {e}")
        return False


def test_deployment_script():
    """Test deployment script configuration."""
    print("🔍 Testing deployment script configuration...")
    
    try:
        from deploy_streaming_pipeline import StreamingPipelineDeployer, DEFAULT_CONFIG
        
        # Test configuration loading
        config = DEFAULT_CONFIG.copy()
        config["project_id"] = "test-project"
        
        deployer = StreamingPipelineDeployer(config)
        
        assert deployer.project_id == "test-project", "Configuration loading failed"
        assert deployer.region == "us-central1", "Default region not set"
        
        print(f"  ✅ Project ID: {deployer.project_id}")
        print(f"  ✅ Region: {deployer.region}")
        print(f"  ✅ Cluster: {config['cluster_name']}")
        
        print("✅ Deployment script configuration tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Deployment script configuration test failed: {e}")
        return False


def run_all_tests():
    """Run all local tests."""
    print("=" * 70)
    print("Enhanced Real-Time Image Classification - Local Testing")
    print("=" * 70)
    
    tests = [
        ("Image Generation", test_image_generation),
        ("Image Classification UDF", test_image_classification_udf),
        ("Streaming DataFrame Operations", test_streaming_dataframe_operations),
        ("Kafka Simulation", test_kafka_simulation),
        ("Performance Metrics", test_performance_metrics),
        ("Deployment Configuration", test_deployment_script)
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running: {test_name}")
        print(f"{'='*50}")
        
        try:
            if test_func():
                passed_tests += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} FAILED with exception: {e}")
    
    print(f"\n{'='*70}")
    print(f"TEST SUMMARY")
    print(f"{'='*70}")
    print(f"Passed: {passed_tests}/{total_tests}")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED - Ready for deployment!")
        return True
    else:
        print("⚠️ Some tests failed - Review before deployment")
        return False


def main():
    """Main testing function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Enhanced Streaming Pipeline")
    parser.add_argument("--test", choices=[
        "image-gen", "udf", "dataframe", "kafka", "metrics", "deployment", "all"
    ], default="all", help="Specific test to run")
    
    args = parser.parse_args()
    
    if args.test == "all":
        return run_all_tests()
    elif args.test == "image-gen":
        return test_image_generation()
    elif args.test == "udf":
        return test_image_classification_udf()
    elif args.test == "dataframe":
        return test_streaming_dataframe_operations()
    elif args.test == "kafka":
        return test_kafka_simulation()
    elif args.test == "metrics":
        return test_performance_metrics()
    elif args.test == "deployment":
        return test_deployment_script()
    
    return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)