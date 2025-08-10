#!/usr/bin/env python3
"""
Real-Time Image Stream Simulator
Course: Introduction to Big Data - Week 9
Author: Abhyudaya B Tharakan 22f3001492

This simulator generates synthetic images and streams them to Kafka topics
or uploads them to GCS to simulate real-time image classification scenarios.

Features:
- Generate synthetic images with different patterns and objects
- Stream base64-encoded images to Kafka topics
- Upload images to GCS buckets with notifications
- Configurable streaming rates and image properties
- Multiple image generation strategies
"""

import os
import io
import json
import time
import uuid
import base64
import argparse
import threading
from typing import List, Dict, Any
from datetime import datetime
import numpy as np
from PIL import Image, ImageDraw, ImageFont

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("WARNING: kafka-python not installed, Kafka streaming disabled")

try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    print("WARNING: google-cloud-storage not installed, GCS upload disabled")


class ImageGenerator:
    """Generate synthetic images for streaming simulation."""
    
    def __init__(self, width: int = 224, height: int = 224):
        self.width = width
        self.height = height
        self.colors = [
            (255, 0, 0),    # Red
            (0, 255, 0),    # Green
            (0, 0, 255),    # Blue
            (255, 255, 0),  # Yellow
            (255, 0, 255),  # Magenta
            (0, 255, 255),  # Cyan
            (128, 128, 128), # Gray
            (255, 165, 0),  # Orange
        ]
        
        # Try to load a font for text
        try:
            self.font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
        except:
            try:
                self.font = ImageFont.load_default()
            except:
                self.font = None
    
    def generate_geometric_pattern(self, pattern_type: str = "circles") -> Image.Image:
        """Generate image with geometric patterns."""
        img = Image.new('RGB', (self.width, self.height), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        if pattern_type == "circles":
            for i in range(5):
                x = np.random.randint(0, self.width - 50)
                y = np.random.randint(0, self.height - 50)
                radius = np.random.randint(10, 40)
                color = self.colors[np.random.randint(0, len(self.colors))]
                draw.ellipse([x, y, x + radius, y + radius], fill=color)
        
        elif pattern_type == "rectangles":
            for i in range(4):
                x1 = np.random.randint(0, self.width // 2)
                y1 = np.random.randint(0, self.height // 2)
                x2 = x1 + np.random.randint(30, 80)
                y2 = y1 + np.random.randint(30, 80)
                color = self.colors[np.random.randint(0, len(self.colors))]
                draw.rectangle([x1, y1, x2, y2], fill=color)
        
        elif pattern_type == "lines":
            for i in range(8):
                x1 = np.random.randint(0, self.width)
                y1 = np.random.randint(0, self.height)
                x2 = np.random.randint(0, self.width)
                y2 = np.random.randint(0, self.height)
                color = self.colors[np.random.randint(0, len(self.colors))]
                draw.line([(x1, y1), (x2, y2)], fill=color, width=3)
        
        return img
    
    def generate_noise_pattern(self) -> Image.Image:
        """Generate image with random noise patterns."""
        # Generate random noise
        noise = np.random.randint(0, 256, (self.height, self.width, 3), dtype=np.uint8)
        img = Image.fromarray(noise)
        
        # Add some structured elements
        draw = ImageDraw.Draw(img)
        for i in range(3):
            x = np.random.randint(50, self.width - 50)
            y = np.random.randint(50, self.height - 50)
            radius = np.random.randint(20, 50)
            color = self.colors[np.random.randint(0, len(self.colors))]
            draw.ellipse([x - radius, y - radius, x + radius, y + radius], fill=color)
        
        return img
    
    def generate_text_image(self, text: str = None) -> Image.Image:
        """Generate image with text content."""
        img = Image.new('RGB', (self.width, self.height), 
                       self.colors[np.random.randint(0, len(self.colors))])
        draw = ImageDraw.Draw(img)
        
        if text is None:
            text = f"IMG_{np.random.randint(1000, 9999)}"
        
        if self.font:
            # Calculate text size and position
            bbox = draw.textbbox((0, 0), text, font=self.font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            x = (self.width - text_width) // 2
            y = (self.height - text_height) // 2
            draw.text((x, y), text, fill=(255, 255, 255), font=self.font)
        else:
            # Fallback without font
            draw.text((self.width // 4, self.height // 2), text, fill=(255, 255, 255))
        
        return img
    
    def generate_gradient_image(self) -> Image.Image:
        """Generate image with gradient patterns."""
        img = Image.new('RGB', (self.width, self.height))
        draw = ImageDraw.Draw(img)
        
        # Create gradient
        for y in range(self.height):
            r = int(255 * y / self.height)
            g = int(255 * (1 - y / self.height))
            b = 128
            color = (r, g, b)
            draw.line([(0, y), (self.width, y)], fill=color)
        
        # Add some shapes on top
        for i in range(3):
            x = np.random.randint(20, self.width - 20)
            y = np.random.randint(20, self.height - 20)
            size = np.random.randint(10, 30)
            color = self.colors[np.random.randint(0, len(self.colors))]
            draw.rectangle([x, y, x + size, y + size], fill=color)
        
        return img
    
    def generate_random_image(self) -> Image.Image:
        """Generate a random image using one of the available patterns."""
        patterns = [
            lambda: self.generate_geometric_pattern("circles"),
            lambda: self.generate_geometric_pattern("rectangles"),
            lambda: self.generate_geometric_pattern("lines"),
            self.generate_noise_pattern,
            lambda: self.generate_text_image(),
            self.generate_gradient_image
        ]
        
        selected_pattern = np.random.choice(patterns)
        return selected_pattern()


class KafkaImageStreamer:
    """Stream images to Kafka topics."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python package required for Kafka streaming")
        
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.image_generator = ImageGenerator()
        print(f"Kafka producer initialized for topic: {topic}")
    
    def image_to_base64(self, image: Image.Image) -> str:
        """Convert PIL image to base64 string."""
        buffer = io.BytesIO()
        image.save(buffer, format='JPEG', quality=85)
        buffer.seek(0)
        return base64.b64encode(buffer.getvalue()).decode('utf-8')
    
    def stream_images(self, rate_per_second: float = 1.0, duration_seconds: int = 60):
        """Stream images to Kafka at specified rate."""
        interval = 1.0 / rate_per_second
        start_time = time.time()
        image_count = 0
        
        print(f"Starting to stream images at {rate_per_second} images/second for {duration_seconds} seconds")
        
        try:
            while (time.time() - start_time) < duration_seconds:
                # Generate image
                image = self.image_generator.generate_random_image()
                image_base64 = self.image_to_base64(image)
                
                # Create message
                message = {
                    "image_id": str(uuid.uuid4()),
                    "image_data": image_base64,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": {
                        "source": "simulator",
                        "image_size": f"{image.width}x{image.height}",
                        "format": "JPEG",
                        "sequence_number": str(image_count)
                    }
                }
                
                # Send to Kafka
                self.producer.send(
                    self.topic,
                    key=message["image_id"],
                    value=message
                )
                
                image_count += 1
                if image_count % 10 == 0:
                    print(f"Streamed {image_count} images...")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print("\nStopping image streaming...")
        
        finally:
            self.producer.flush()
            self.producer.close()
            print(f"Streaming completed. Total images sent: {image_count}")


class GCSImageUploader:
    """Upload images to GCS bucket for notification-based streaming."""
    
    def __init__(self, bucket_name: str, prefix: str = "streaming-images/"):
        if not GCS_AVAILABLE:
            raise ImportError("google-cloud-storage package required for GCS upload")
        
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.image_generator = ImageGenerator()
        print(f"GCS uploader initialized for bucket: {bucket_name}")
    
    def upload_images(self, rate_per_second: float = 0.5, duration_seconds: int = 60):
        """Upload images to GCS at specified rate."""
        interval = 1.0 / rate_per_second
        start_time = time.time()
        image_count = 0
        
        print(f"Starting to upload images at {rate_per_second} images/second for {duration_seconds} seconds")
        
        try:
            while (time.time() - start_time) < duration_seconds:
                # Generate image
                image = self.image_generator.generate_random_image()
                
                # Create unique object name
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                object_name = f"{self.prefix}img_{timestamp}_{image_count:04d}.jpg"
                
                # Convert image to bytes
                buffer = io.BytesIO()
                image.save(buffer, format='JPEG', quality=85)
                buffer.seek(0)
                
                # Upload to GCS
                blob = self.bucket.blob(object_name)
                blob.upload_from_file(buffer, content_type='image/jpeg')
                
                # Set metadata
                blob.metadata = {
                    "source": "simulator",
                    "image_size": f"{image.width}x{image.height}",
                    "sequence_number": str(image_count)
                }
                blob.patch()
                
                image_count += 1
                if image_count % 5 == 0:
                    print(f"Uploaded {image_count} images...")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print("\nStopping image upload...")
        
        print(f"Upload completed. Total images uploaded: {image_count}")


def main():
    parser = argparse.ArgumentParser(description="Real-Time Image Stream Simulator")
    
    parser.add_argument("--mode", choices=["kafka", "gcs", "both"], default="kafka",
                       help="Streaming mode")
    parser.add_argument("--rate", type=float, default=1.0,
                       help="Images per second")
    parser.add_argument("--duration", type=int, default=60,
                       help="Streaming duration in seconds")
    
    # Kafka options
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="image-stream",
                       help="Kafka topic for streaming")
    
    # GCS options
    parser.add_argument("--gcs-bucket", help="GCS bucket for uploads")
    parser.add_argument("--gcs-prefix", default="streaming-images/",
                       help="GCS object prefix")
    
    # Image options
    parser.add_argument("--image-width", type=int, default=224,
                       help="Generated image width")
    parser.add_argument("--image-height", type=int, default=224,
                       help="Generated image height")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Real-Time Image Stream Simulator")
    print("=" * 60)
    print(f"Mode: {args.mode}")
    print(f"Rate: {args.rate} images/second")
    print(f"Duration: {args.duration} seconds")
    print(f"Image size: {args.image_width}x{args.image_height}")
    
    threads = []
    
    try:
        if args.mode in ["kafka", "both"]:
            if not KAFKA_AVAILABLE:
                print("ERROR: kafka-python package not available for Kafka streaming")
            else:
                kafka_streamer = KafkaImageStreamer(args.kafka_servers, args.kafka_topic)
                kafka_thread = threading.Thread(
                    target=kafka_streamer.stream_images,
                    args=(args.rate, args.duration)
                )
                kafka_thread.start()
                threads.append(kafka_thread)
        
        if args.mode in ["gcs", "both"]:
            if not GCS_AVAILABLE:
                print("ERROR: google-cloud-storage package not available for GCS upload")
            elif not args.gcs_bucket:
                print("ERROR: --gcs-bucket required for GCS mode")
            else:
                gcs_uploader = GCSImageUploader(args.gcs_bucket, args.gcs_prefix)
                gcs_thread = threading.Thread(
                    target=gcs_uploader.upload_images,
                    args=(args.rate * 0.5, args.duration)  # Slower rate for GCS
                )
                gcs_thread.start()
                threads.append(gcs_thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        print("\nSimulation completed successfully!")
    
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user")
    except Exception as e:
        print(f"\nError during simulation: {e}")


if __name__ == "__main__":
    main()