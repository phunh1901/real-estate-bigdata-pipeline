"""
Cấu hình Kafka
"""
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "real-estate-documents")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "real-estate-documents-producer")

