"""
Spark Application Configuration

Runtime constants for Spark jobs; relative paths resolve from the working directory.
"""

# Spark application name shown in the Spark UI and logs.
APP_NAME = "RealTimeECommerce"

# Host-side Kafka listener; use a container-reachable address when Spark runs in Docker.
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Kafka topic containing JSON order messages.
KAFKA_TOPIC = "orders"

# Checkpoint locations retain query offsets/state and should be reused on restart.
BRONZE_CHECKPOINT_LOCATION = "checkpoint_bronze"

SILVER_CHECKPOINT_LOCATION = "checkpoint_silver"

# Data paths are relative to the Spark working directory.
BRONZE_PATH = "bronze"

SILVER_PATH = "silver/data"

QUARANTINE_PATH = "quarantine/data"

LOG_LEVEL = "ERROR"
