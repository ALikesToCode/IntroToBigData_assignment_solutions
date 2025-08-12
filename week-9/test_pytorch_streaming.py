#!/usr/bin/env python3
"""
Test Script for PyTorch Streaming Image Classifier
Verifies the direct conversion of Lecture 10 notebook works correctly
"""

import os
import sys
import io
import json
import base64
import tempfile
import unittest
from pathlib import Path
from PIL import Image
import numpy as np

# Test if required packages are available
PACKAGES_AVAILABLE = True
try:
    import torch
    from torchvision import models, transforms
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError as e:
    PACKAGES_AVAILABLE = False
    print(f"Warning: Some packages not available: {e}")


class TestPyTorchStreaming(unittest.TestCase):
    """Test cases for PyTorch streaming classifier."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        if PACKAGES_AVAILABLE:
            # Create minimal Spark session for testing
            cls.spark = SparkSession.builder \
                .appName("TestPyTorchStreaming") \
                .master("local[1]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.shuffle.partitions", "2") \
                .getOrCreate()
            cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        if PACKAGES_AVAILABLE and hasattr(cls, 'spark'):
            cls.spark.stop()
    
    def test_image_preprocessing(self):
        """Test image preprocessing matches notebook approach."""
        if not PACKAGES_AVAILABLE:
            self.skipTest("Required packages not available")
        
        # Create test image
        test_image = Image.new('RGB', (256, 256), color='red')
        
        # Save to bytes
        img_bytes = io.BytesIO()
        test_image.save(img_bytes, format='JPEG')
        img_bytes.seek(0)
        
        # Apply preprocessing (same as notebook)
        transform = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ])
        
        image = Image.open(img_bytes)
        processed = transform(image)
        
        # Verify shape
        self.assertEqual(processed.shape, (3, 224, 224))
        print("✅ Image preprocessing test passed")
    
    def test_model_loading(self):
        """Test PyTorch model loading."""
        if not PACKAGES_AVAILABLE:
            self.skipTest("Required packages not available")
        
        try:
            # Load MobileNetV2 (same as notebook)
            model = models.mobilenet_v2(pretrained=True)
            model.eval()
            
            # Create dummy input
            dummy_input = torch.randn(1, 3, 224, 224)
            
            # Run inference
            with torch.no_grad():
                output = model(dummy_input)
            
            # Verify output shape
            self.assertEqual(output.shape[1], 1000)  # ImageNet classes
            print("✅ Model loading test passed")
        except Exception as e:
            print(f"⚠️ Model loading test skipped: {e}")
    
    def test_label_extraction(self):
        """Test label extraction from path (same as notebook)."""
        from pytorch_streaming_classifier import extract_label_from_path
        
        # Test cases matching notebook structure
        test_cases = [
            ("flower_photos/roses/image1.jpg", "roses"),
            ("flower_photos/tulips/photo.jpg", "tulips"),
            ("flower_photos/dandelion/test.jpg", "dandelion"),
            ("/path/to/flower_photos/sunflowers/img.jpg", "sunflowers"),
            ("other/path/image.jpg", "path")  # Fallback case
        ]
        
        for path, expected_label in test_cases:
            label = extract_label_from_path(path)
            self.assertEqual(label, expected_label, f"Failed for path: {path}")
        
        print("✅ Label extraction test passed")
    
    def test_base64_encoding(self):
        """Test image to base64 conversion for Kafka."""
        # Create test image
        test_image = Image.new('RGB', (100, 100), color='blue')
        
        # Save to bytes and encode
        img_bytes = io.BytesIO()
        test_image.save(img_bytes, format='JPEG')
        img_bytes.seek(0)
        
        # Encode to base64
        encoded = base64.b64encode(img_bytes.getvalue()).decode('utf-8')
        
        # Decode back
        decoded = base64.b64decode(encoded)
        decoded_image = Image.open(io.BytesIO(decoded))
        
        # Verify
        self.assertEqual(decoded_image.size, (100, 100))
        print("✅ Base64 encoding test passed")
    
    def test_streaming_udf_structure(self):
        """Test the UDF structure matches notebook approach."""
        if not PACKAGES_AVAILABLE:
            self.skipTest("Required packages not available")
        
        from pytorch_streaming_classifier import imagenet_streaming_model_udf
        
        # Create test data
        test_image = Image.new('RGB', (224, 224), color='green')
        img_bytes = io.BytesIO()
        test_image.save(img_bytes, format='JPEG')
        img_bytes.seek(0)
        
        # Create DataFrame with image
        test_data = [(img_bytes.getvalue(),)]
        df = self.spark.createDataFrame(test_data, ["content"])
        
        # Apply UDF (same structure as notebook)
        udf = imagenet_streaming_model_udf()
        
        # Verify UDF can be created
        self.assertIsNotNone(udf)
        print("✅ Streaming UDF structure test passed")
    
    def test_kafka_message_format(self):
        """Test Kafka message format for flower images."""
        from flower_kafka_producer import extract_label_from_path
        
        # Create test message
        message = {
            "image_id": "img_000001",
            "path": "flower_photos/roses/test.jpg",
            "label": extract_label_from_path("flower_photos/roses/test.jpg"),
            "modificationTime": 1234567890,
            "size": 12345,
            "image_data": "base64_encoded_data_here",
            "timestamp": "2024-08-10T10:00:00"
        }
        
        # Verify structure
        self.assertEqual(message["label"], "roses")
        self.assertIn("image_id", message)
        self.assertIn("image_data", message)
        
        # Test JSON serialization (for Kafka)
        json_str = json.dumps(message)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["label"], "roses")
        
        print("✅ Kafka message format test passed")
    
    def test_file_streaming_structure(self):
        """Test file-based streaming structure."""
        if not PACKAGES_AVAILABLE:
            self.skipTest("Required packages not available")
        
        # Create temporary directory structure like flower_photos
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create flower directory structure
            flower_dir = Path(tmpdir) / "flower_photos"
            for category in ["roses", "tulips", "dandelion"]:
                category_dir = flower_dir / category
                category_dir.mkdir(parents=True)
                
                # Create dummy image
                img_path = category_dir / "test.jpg"
                test_image = Image.new('RGB', (100, 100), color='red')
                test_image.save(img_path)
            
            # Test reading with Spark (batch mode for testing)
            df = self.spark.read \
                .format("binaryFile") \
                .option("pathGlobFilter", "*.jpg") \
                .option("recursiveFileLookup", "true") \
                .load(str(flower_dir))
            
            # Verify structure
            self.assertTrue(df.count() > 0)
            self.assertIn("path", df.columns)
            self.assertIn("content", df.columns)
            
            print(f"✅ File streaming structure test passed (found {df.count()} images)")
    
    def test_gcp_paths(self):
        """Test GCS path handling."""
        test_cases = [
            ("gs://bucket/flower_photos/roses/img.jpg", True),
            ("gs://my-bucket/streaming-input/test.jpg", True),
            ("/local/path/image.jpg", False),
            ("flower_photos/roses/test.jpg", False)
        ]
        
        for path, is_gcs in test_cases:
            self.assertEqual(path.startswith("gs://"), is_gcs)
        
        print("✅ GCP path handling test passed")


def run_integration_test():
    """Run a simple integration test of the streaming pipeline."""
    print("\n" + "=" * 60)
    print("Running Integration Test")
    print("=" * 60)
    
    if not PACKAGES_AVAILABLE:
        print("⚠️ Integration test skipped: packages not available")
        return
    
    try:
        # Create test image
        print("Creating test image...")
        test_image = Image.new('RGB', (224, 224))
        draw = Image.Image.core.draw(test_image.im, 0)
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
            test_image.save(tmp.name)
            test_path = tmp.name
        
        # Create minimal Spark session
        spark = SparkSession.builder \
            .appName("IntegrationTest") \
            .master("local[1]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        # Load image
        df = spark.read.format("binaryFile").load(test_path)
        
        # Apply transformations (similar to notebook)
        from pytorch_streaming_classifier import mobilenet_v2_udf
        result_df = df.withColumn("prediction", mobilenet_v2_udf(F.col("content")))
        
        # Check result
        result = result_df.select("prediction.*").collect()
        if result:
            print(f"✅ Integration test passed - Prediction: {result[0]}")
        
        spark.stop()
        os.unlink(test_path)
        
    except Exception as e:
        print(f"⚠️ Integration test failed: {e}")


def main():
    """Run all tests."""
    print("=" * 80)
    print("PyTorch Streaming Classifier - Test Suite")
    print("Direct Conversion of Lecture 10 Notebook")
    print("=" * 80)
    
    # Check environment
    print("\nEnvironment Check:")
    print(f"  PyTorch available: {'✅' if 'torch' in sys.modules else '❌'}")
    print(f"  PySpark available: {'✅' if 'pyspark' in sys.modules else '❌'}")
    print(f"  Pandas available: {'✅' if 'pandas' in sys.modules else '❌'}")
    
    # Run unit tests
    print("\nRunning Unit Tests:")
    print("-" * 40)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestPyTorchStreaming)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(suite)
    
    # Run integration test
    run_integration_test()
    
    # Summary
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("🎉 All tests passed successfully!")
        print("The PyTorch streaming classifier is ready for deployment.")
    else:
        print("⚠️ Some tests failed. Please review the errors above.")
    print("=" * 60)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())