"""
Cấu hình Kafka
"""

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" # kết nối từ bên ngoài Docker
KAFKA_TOPIC = "real-estate-documents"
KAFKA_CLIENT_ID = "real-estate-documents-producer"
KAFKA_TRANSACTIONAL_ID = "real-estate-producer-1" # Kích hoạt tính năng Exactly-Once Semantic

