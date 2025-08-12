#!/usr/bin/env python3
"""
Kafka Producer for Flower Images - Maintains structure from Lecture 10
Sends flower images to Kafka topic with the same label extraction logic
"""

import os
import io
import json
import time
import base64
import argparse
from typing import List
from pathlib import Path
from datetime import datetime

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Warning: kafka-python not installed")

from PIL import Image


def extract_label_from_path(file_path: str) -> str:
    """
    Extract label from file path (same logic as notebook).
    Expects structure like: flower_photos/roses/image.jpg
    """
    parts = Path(file_path).parts
    # Look for flower_photos in path and get next directory as label
    try:
        flower_idx = parts.index("flower_photos")
        if flower_idx < len(parts) - 2:
            return parts[flower_idx + 1]
    except ValueError:
        pass
    
    # Fallback: use parent directory name
    return Path(file_path).parent.name


def scan_flower_directory(base_path: str) -> List[dict]:
    """
    Scan flower_photos directory structure (same as notebook's binaryFile).
    Returns list of image metadata.
    """
    images = []
    base_path = Path(base_path)
    
    # Recursively find all .jpg files
    for jpg_file in base_path.rglob("*.jpg"):
        rel_path = jpg_file.relative_to(base_path.parent if base_path.name == "flower_photos" else base_path)
        
        images.append({
            "path": str(rel_path),
            "full_path": str(jpg_file),
            "label": extract_label_from_path(str(jpg_file)),
            "modificationTime": os.path.getmtime(jpg_file),
            "size": os.path.getsize(jpg_file)
        })
    
    return images


def image_to_base64(image_path: str) -> str:
    """Convert image file to base64 string."""
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode('utf-8')


def produce_flower_images(
    kafka_servers: str,
    topic: str,
    flower_dir: str,
    rate_per_second: float = 1.0,
    max_images: int = None
):
    """
    Stream flower images to Kafka topic.
    Maintains the same data structure as the original notebook.
    """
    if not KAFKA_AVAILABLE:
        raise ImportError("kafka-python required. Install with: pip install kafka-python")
    
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )
    
    # Scan flower directory
    print(f"Scanning {flower_dir} for images...")
    images = scan_flower_directory(flower_dir)
    print(f"Found {len(images)} images")
    
    if not images:
        print("No images found!")
        return
    
    # Group by label (like the notebook does)
    labels = {}
    for img in images:
        label = img["label"]
        if label not in labels:
            labels[label] = []
        labels[label].append(img)
    
    print(f"Labels found: {list(labels.keys())}")
    for label, imgs in labels.items():
        print(f"  {label}: {len(imgs)} images")
    
    # Stream images
    interval = 1.0 / rate_per_second
    sent_count = 0
    
    print(f"\nStreaming images at {rate_per_second} images/second to topic '{topic}'")
    print("-" * 60)
    
    try:
        for img_metadata in images:
            if max_images and sent_count >= max_images:
                break
            
            # Create message matching notebook structure
            message = {
                "image_id": f"img_{sent_count:06d}",
                "path": img_metadata["path"],
                "label": img_metadata["label"],
                "modificationTime": img_metadata["modificationTime"],
                "size": img_metadata["size"],
                "image_data": image_to_base64(img_metadata["full_path"]),
                "timestamp": datetime.now().isoformat()
            }
            
            # Send to Kafka
            producer.send(
                topic,
                key=message["image_id"],
                value=message
            )
            
            sent_count += 1
            print(f"Sent {sent_count}: {img_metadata['path']} (label: {img_metadata['label']})")
            
            time.sleep(interval)
    
    except KeyboardInterrupt:
        print("\nStopping producer...")
    
    finally:
        producer.flush()
        producer.close()
        print(f"\nTotal images sent: {sent_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Producer for Flower Images (Lecture 10 structure)"
    )
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="flower-images",
                       help="Kafka topic")
    parser.add_argument("--flower-dir", required=True,
                       help="Path to flower_photos directory")
    parser.add_argument("--rate", type=float, default=1.0,
                       help="Images per second")
    parser.add_argument("--max-images", type=int,
                       help="Maximum number of images to send")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Flower Images Kafka Producer")
    print("=" * 60)
    
    produce_flower_images(
        args.kafka_servers,
        args.topic,
        args.flower_dir,
        args.rate,
        args.max_images
    )


if __name__ == "__main__":
    main()