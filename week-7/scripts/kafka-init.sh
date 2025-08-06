#!/bin/bash
# Kafka initialization script for Dataproc cluster
set -e

# Update system (ignore repository errors)
apt-get update || true

# Install Java 11 (better compatibility)
apt-get install -y openjdk-11-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /etc/environment

# Download and install Kafka 3.9.1 (latest stable)
cd /opt
wget -q https://dlcdn.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz || wget -q https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
tar -xzf kafka_2.13-3.9.1.tgz
mv kafka_2.13-3.9.1 kafka
chown -R yarn:yarn /opt/kafka

# Create Kafka directories
mkdir -p /var/log/kafka
mkdir -p /var/lib/kafka-logs
chown -R yarn:yarn /var/log/kafka
chown -R yarn:yarn /var/lib/kafka-logs

# Configure Kafka
cat > /opt/kafka/config/server.properties << EOF
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$(hostname -I | awk '{print $1}'):9092
log.dirs=/var/lib/kafka-logs
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=24
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
default.replication.factor=1
min.insync.replicas=1
EOF

# Create systemd service for Kafka
cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=yarn
Group=yarn
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
Environment=KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start Kafka service
systemctl daemon-reload
systemctl enable kafka
systemctl start kafka

# Wait for Kafka to start
sleep 30

# Create topic for streaming
/opt/kafka/bin/kafka-topics.sh --create --topic customer-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "Kafka initialization completed successfully"
