#!/bin/bash
# Manual Kafka setup for Week 7 streaming cluster
set -e

echo "🚀 Starting Kafka setup on cluster..."

# Update system and install Java
echo "📦 Installing Java 11..."
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk wget

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc

# Download and install Kafka
echo "📥 Downloading Kafka 3.9.1..."
cd /opt
sudo wget -q https://dlcdn.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz || {
    echo "Using backup URL..."
    sudo wget -q https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
}

echo "📂 Extracting Kafka..."
sudo tar -xzf kafka_2.13-3.9.1.tgz
sudo mv kafka_2.13-3.9.1 kafka
sudo chown -R $USER:$USER /opt/kafka

# Create directories
echo "📁 Creating Kafka directories..."
mkdir -p /tmp/kafka-logs
mkdir -p /tmp/zookeeper

# Configure Kafka for single node with minimal memory
echo "⚙️ Configuring Kafka..."
cat > /opt/kafka/config/server.properties << EOF
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=/tmp/kafka-logs
num.network.threads=2
num.io.threads=2
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=1
log.segment.bytes=268435456
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
default.replication.factor=1
min.insync.replicas=1
EOF

# Set Kafka heap to minimal for e2-medium
echo 'export KAFKA_HEAP_OPTS="-Xmx256m -Xms256m"' > /opt/kafka/kafka_env.sh

echo "🏁 Kafka setup complete!"
echo "To start Kafka:"
echo "1. cd /opt/kafka"
echo "2. source kafka_env.sh"  
echo "3. bin/zookeeper-server-start.sh -daemon config/zookeeper.properties"
echo "4. sleep 10"
echo "5. bin/kafka-server-start.sh -daemon config/server.properties"
echo "6. bin/kafka-topics.sh --create --topic customer-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"