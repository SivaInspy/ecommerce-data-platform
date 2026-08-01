"""
Application Configuration

This file stores all configurable values for the Kafka Producer.
"""

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "orders"

# Producer Configuration
MESSAGE_INTERVAL_SECONDS = 2

# Logging
LOG_FILE = "logs/producer.log"
LOG_LEVEL = "INFO"