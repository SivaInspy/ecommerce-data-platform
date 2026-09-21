"""
Spark Application Configuration
"""

APP_NAME = "RealTimeECommerce"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPIC = "orders"

BRONZE_CHECKPOINT_LOCATION = "checkpoint_bronze"

SILVER_CHECKPOINT_LOCATION = "checkpoint_silver"

BRONZE_PATH = "bronze"

SILVER_PATH = "silver"

QUARANTINE_PATH = "quarantine"

LOG_LEVEL = "ERROR"