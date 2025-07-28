#!/usr/bin/env python3
"""
Kafka Environment Setup Script for Week 7
Course: Introduction to Big Data - Week 7

This script sets up a local Kafka environment for the streaming assignment.
It can download, configure, and start Kafka and Zookeeper services.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import os
import sys
import time
import shutil
import logging
import argparse
import subprocess
import platform
from pathlib import Path
import urllib.request
import tarfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaEnvironmentSetup:
    """
    Sets up and manages local Kafka environment for development and testing.
    """
    
    def __init__(self, kafka_version='2.13-3.4.0', install_dir=None):
        """
        Initialize Kafka environment setup.
        
        Args:
            kafka_version: Kafka version to download (format: scala-kafka)
            install_dir: Directory to install Kafka (default: ./kafka-env)
        """
        self.kafka_version = kafka_version
        self.scala_version = kafka_version.split('-')[0]
        self.kafka_version_number = kafka_version.split('-')[1]
        
        # Set installation directory
        if install_dir:
            self.install_dir = Path(install_dir)
        else:
            self.install_dir = Path(__file__).parent.parent / "kafka-env"
        
        self.kafka_dir = self.install_dir / f"kafka_{kafka_version}"
        self.kafka_bin = self.kafka_dir / "bin"
        
        # Determine platform-specific scripts
        self.is_windows = platform.system() == "Windows"
        self.script_ext = ".bat" if self.is_windows else ".sh"
        
        logger.info(f"🔧 Kafka Environment Setup initialized")
        logger.info(f"   Version: {self.kafka_version}")
        logger.info(f"   Install directory: {self.install_dir}")
        logger.info(f"   Platform: {platform.system()}")
    
    def download_kafka(self):
        """
        Download Kafka binary distribution.
        """
        try:
            # Kafka download URL
            download_url = (f"https://downloads.apache.org/kafka/{self.kafka_version_number}/"
                          f"kafka_{self.kafka_version}.tgz")
            
            download_file = self.install_dir / f"kafka_{self.kafka_version}.tgz"
            
            logger.info(f"📥 Downloading Kafka {self.kafka_version}...")
            logger.info(f"   URL: {download_url}")
            logger.info(f"   Destination: {download_file}")
            
            # Create install directory
            self.install_dir.mkdir(parents=True, exist_ok=True)
            
            # Download with progress
            def progress_hook(block_num, block_size, total_size):
                downloaded = block_num * block_size
                if total_size > 0:
                    percent = min(100, (downloaded * 100) // total_size)
                    print(f"\r   Progress: {percent}% ({downloaded:,}/{total_size:,} bytes)", 
                          end='', flush=True)
            
            urllib.request.urlretrieve(download_url, download_file, progress_hook)
            print()  # New line after progress
            
            logger.info(f"✅ Download completed: {download_file}")
            return download_file
            
        except Exception as e:
            logger.error(f"❌ Failed to download Kafka: {e}")
            raise
    
    def extract_kafka(self, download_file):
        """
        Extract Kafka archive.
        
        Args:
            download_file: Path to the downloaded archive
        """
        try:
            logger.info(f"📦 Extracting Kafka archive...")
            
            with tarfile.open(download_file, 'r:gz') as tar:
                tar.extractall(self.install_dir)
            
            logger.info(f"✅ Extraction completed to: {self.kafka_dir}")
            
            # Clean up download file
            download_file.unlink()
            logger.info(f"🗑️ Cleaned up download file")
            
        except Exception as e:
            logger.error(f"❌ Failed to extract Kafka: {e}")
            raise
    
    def configure_kafka(self):
        """
        Configure Kafka server properties for local development.
        """
        try:
            logger.info(f"⚙️ Configuring Kafka for local development...")
            
            # Server properties file
            server_props = self.kafka_dir / "config" / "server.properties"
            
            if not server_props.exists():
                logger.error(f"❌ Server properties file not found: {server_props}")
                return
            
            # Read current properties
            with open(server_props, 'r') as f:
                properties = f.read()
            
            # Modify key properties for local development
            modifications = {
                'listeners=PLAINTEXT://:9092': True,
                'advertised.listeners=PLAINTEXT://localhost:9092': True,
                'log.dirs=/tmp/kafka-logs': True,
                'num.partitions=3': True,
                'default.replication.factor=1': True,
                'min.insync.replicas=1': True,
                'auto.create.topics.enable=true': True,
                'delete.topic.enable=true': True,
                'log.retention.hours=24': True,  # Short retention for development
                'log.segment.bytes=268435456': True,  # 256MB segments
                'log.cleanup.policy=delete': True
            }
            
            # Apply modifications
            lines = properties.split('\n')
            modified_lines = []
            modified_keys = set()
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    key = line.split('=')[0]
                    
                    # Check if we need to modify this property
                    found_modification = False
                    for mod_line, _ in modifications.items():
                        if mod_line.startswith(key + '='):
                            modified_lines.append(mod_line)
                            modified_keys.add(mod_line)
                            found_modification = True
                            break
                    
                    if not found_modification:
                        modified_lines.append(line)
                else:
                    modified_lines.append(line)
            
            # Add any missing properties
            for mod_line, _ in modifications.items():
                if mod_line not in modified_keys:
                    modified_lines.append(mod_line)
            
            # Write back modified properties
            with open(server_props, 'w') as f:
                f.write('\n'.join(modified_lines))
            
            logger.info(f"✅ Kafka configuration updated")
            
        except Exception as e:
            logger.error(f"❌ Failed to configure Kafka: {e}")
            raise
    
    def install_kafka(self):
        """
        Complete Kafka installation process.
        """
        try:
            logger.info(f"🚀 Starting Kafka installation...")
            
            # Check if already installed
            if self.kafka_dir.exists():
                logger.warning(f"⚠️ Kafka already installed at: {self.kafka_dir}")
                response = input("Do you want to reinstall? (y/N): ")
                if response.lower() != 'y':
                    logger.info("📋 Using existing installation")
                    return
                else:
                    logger.info("🗑️ Removing existing installation...")
                    shutil.rmtree(self.kafka_dir)
            
            # Download and extract
            download_file = self.download_kafka()
            self.extract_kafka(download_file)
            
            # Configure
            self.configure_kafka()
            
            logger.info(f"✅ Kafka installation completed!")
            logger.info(f"   Installation directory: {self.kafka_dir}")
            logger.info(f"   Binary directory: {self.kafka_bin}")
            
        except Exception as e:
            logger.error(f"❌ Kafka installation failed: {e}")
            raise
    
    def start_zookeeper(self):
        """
        Start Zookeeper service.
        """
        try:
            logger.info(f"🐘 Starting Zookeeper...")
            
            zookeeper_script = self.kafka_bin / f"zookeeper-server-start{self.script_ext}"
            zookeeper_config = self.kafka_dir / "config" / "zookeeper.properties"
            
            if not zookeeper_script.exists():
                logger.error(f"❌ Zookeeper script not found: {zookeeper_script}")
                return None
            
            # Start Zookeeper in background
            cmd = [str(zookeeper_script), str(zookeeper_config)]
            
            logger.info(f"   Command: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait a bit for startup
            time.sleep(5)
            
            if process.poll() is None:
                logger.info(f"✅ Zookeeper started (PID: {process.pid})")
                return process
            else:
                stdout, stderr = process.communicate()
                logger.error(f"❌ Zookeeper failed to start")
                logger.error(f"   stdout: {stdout}")
                logger.error(f"   stderr: {stderr}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to start Zookeeper: {e}")
            return None
    
    def start_kafka_server(self):
        """
        Start Kafka server.
        """
        try:
            logger.info(f"🚀 Starting Kafka server...")
            
            kafka_script = self.kafka_bin / f"kafka-server-start{self.script_ext}"
            kafka_config = self.kafka_dir / "config" / "server.properties"
            
            if not kafka_script.exists():
                logger.error(f"❌ Kafka script not found: {kafka_script}")
                return None
            
            # Start Kafka server in background
            cmd = [str(kafka_script), str(kafka_config)]
            
            logger.info(f"   Command: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait a bit for startup
            time.sleep(10)
            
            if process.poll() is None:
                logger.info(f"✅ Kafka server started (PID: {process.pid})")
                return process
            else:
                stdout, stderr = process.communicate()
                logger.error(f"❌ Kafka server failed to start")
                logger.error(f"   stdout: {stdout}")
                logger.error(f"   stderr: {stderr}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka server: {e}")
            return None
    
    def create_topic(self, topic_name, partitions=3, replication_factor=1):
        """
        Create a Kafka topic.
        
        Args:
            topic_name: Name of the topic to create
            partitions: Number of partitions
            replication_factor: Replication factor
        """
        try:
            logger.info(f"📝 Creating topic: {topic_name}")
            
            kafka_topics_script = self.kafka_bin / f"kafka-topics{self.script_ext}"
            
            cmd = [
                str(kafka_topics_script),
                "--create",
                "--topic", topic_name,
                "--bootstrap-server", "localhost:9092",
                "--partitions", str(partitions),
                "--replication-factor", str(replication_factor)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"✅ Topic '{topic_name}' created successfully")
            else:
                if "already exists" in result.stderr:
                    logger.info(f"ℹ️ Topic '{topic_name}' already exists")
                else:
                    logger.error(f"❌ Failed to create topic '{topic_name}'")
                    logger.error(f"   Error: {result.stderr}")
                    
        except Exception as e:
            logger.error(f"❌ Error creating topic: {e}")
    
    def print_usage_instructions(self):
        """
        Print usage instructions for the Kafka environment.
        """
        print(f"\n{'='*60}")
        print("🎉 KAFKA ENVIRONMENT READY!")
        print(f"{'='*60}")
        print(f"📁 Installation directory: {self.kafka_dir}")
        print(f"🔧 Configuration: {self.kafka_dir}/config/")
        print(f"\n📋 NEXT STEPS:")
        print(f"1. Keep this terminal open (Kafka is running)")
        print(f"2. Open a new terminal for the producer:")
        print(f"   cd week-7")
        print(f"   python producer/kafka_producer.py --data-file data/customer_transactions_1200.csv")
        print(f"\n3. Open another terminal for the consumer:")
        print(f"   cd week-7")
        print(f"   python consumer/spark_streaming_consumer.py")
        print(f"\n🛠️ USEFUL COMMANDS:")
        print(f"List topics:")
        print(f"   {self.kafka_bin}/kafka-topics{self.script_ext} --list --bootstrap-server localhost:9092")
        print(f"\nDescribe topic:")
        print(f"   {self.kafka_bin}/kafka-topics{self.script_ext} --describe --topic customer-transactions --bootstrap-server localhost:9092")
        print(f"\nConsole consumer (for testing):")
        print(f"   {self.kafka_bin}/kafka-console-consumer{self.script_ext} --topic customer-transactions --from-beginning --bootstrap-server localhost:9092")
        print(f"{'='*60}\n")

def main():
    """
    Main function to set up Kafka environment.
    """
    parser = argparse.ArgumentParser(description='Setup Kafka Environment for Week 7')
    parser.add_argument('--kafka-version', default='2.13-3.4.0',
                       help='Kafka version to install (default: 2.13-3.4.0)')
    parser.add_argument('--install-dir',
                       help='Installation directory (default: ./kafka-env)')
    parser.add_argument('--topic-name', default='customer-transactions',
                       help='Topic name to create (default: customer-transactions)')
    parser.add_argument('--partitions', type=int, default=3,
                       help='Number of partitions for the topic (default: 3)')
    parser.add_argument('--skip-start', action='store_true',
                       help='Skip starting Kafka services (install only)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Create setup instance
        setup = KafkaEnvironmentSetup(
            kafka_version=args.kafka_version,
            install_dir=args.install_dir
        )
        
        # Install Kafka
        setup.install_kafka()
        
        if not args.skip_start:
            # Start services
            zk_process = setup.start_zookeeper()
            if zk_process is None:
                logger.error("❌ Failed to start Zookeeper, cannot continue")
                sys.exit(1)
            
            kafka_process = setup.start_kafka_server()
            if kafka_process is None:
                logger.error("❌ Failed to start Kafka server")
                zk_process.terminate()
                sys.exit(1)
            
            # Create topic
            setup.create_topic(
                topic_name=args.topic_name,
                partitions=args.partitions
            )
            
            # Print instructions
            setup.print_usage_instructions()
            
            try:
                logger.info("⏳ Kafka environment is running. Press Ctrl+C to stop...")
                while True:
                    time.sleep(1)
                    
                    # Check if processes are still running
                    if zk_process.poll() is not None:
                        logger.error("❌ Zookeeper process died")
                        break
                    if kafka_process.poll() is not None:
                        logger.error("❌ Kafka process died")
                        break
                        
            except KeyboardInterrupt:
                logger.info("\n🛑 Shutting down Kafka environment...")
                
                logger.info("   Stopping Kafka server...")
                kafka_process.terminate()
                kafka_process.wait(timeout=10)
                
                logger.info("   Stopping Zookeeper...")
                zk_process.terminate()
                zk_process.wait(timeout=10)
                
                logger.info("✅ Kafka environment stopped")
        else:
            logger.info("✅ Kafka installation completed (services not started)")
            logger.info(f"   To start manually, run this script without --skip-start")
        
    except KeyboardInterrupt:
        logger.info("👋 Setup interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Setup failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()