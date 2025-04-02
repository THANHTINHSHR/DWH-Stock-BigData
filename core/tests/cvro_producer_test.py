from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Cấu hình Kafka và Schema Registry
config = {
    "bootstrap.servers": "localhost:9092",  # Kafka server
    "schema.registry.url": "http://localhost:8081",  # Schema Registry
}

# Định nghĩa Avro Schema
key_schema_str = """
{
    "type": "string"
}
"""

value_schema_str = """
{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"}
    ]
}
"""

# Parse schema
key_schema = avro.loads(key_schema_str)
value_schema = avro.loads(value_schema_str)

# Khởi tạo AvroProducer
producer = AvroProducer(
    config, default_key_schema=key_schema, default_value_schema=value_schema
)

# Gửi message
try:
    producer.produce(
        topic="test_avro", key="key1", value={"id": 1, "name": "Kafka Avro"}
    )
    producer.flush()
    print("✅ Gửi thành công!")
except Exception as e:
    print(f"❌ Lỗi: {e}")
