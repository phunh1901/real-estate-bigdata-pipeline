"""
Cấu hình cho Spark Consumer
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "real-estate-documents")
KAFKA_STARTING_OFFSET = os.getenv("KAFKA_STARTING_OFFSET", "earliest")

# Spark Configuration
SPARK_APP_NAME = "RealEstateDocumentsConsumer"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Mặc định chạy local và lưu trong project. Docker Compose ghi đè các biến này
# để Spark đọc Kafka nội bộ và ghi dữ liệu/checkpoint vào HDFS.
HDFS_NAMENODE    = os.getenv("HDFS_NAMENODE", "")
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH",     str(PROJECT_ROOT / "output" / "real-estate"))
HDFS_CHECKPOINT_PATH = os.getenv("HDFS_CHECKPOINT_PATH", str(PROJECT_ROOT / "output" / "checkpoint"))
