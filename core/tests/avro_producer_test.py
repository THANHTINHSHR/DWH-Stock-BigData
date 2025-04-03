from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

SCHEMA_REGISTRY_URL = "http://localhost:8081"  # Địa chỉ Schema Registry
KAFKA_BROKER = "localhost:9092"  # Kafka broker

# Avro Schema
AVRO_SCHEMA = """
{
  "type": "record",
  "name": "TestRecord",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}
"""


class AvroProducer:
    def __init__(self, topic):
        self.topic = topic

        # Khởi tạo Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        # Khởi tạo Avro Serializer
        self.avro_serializer = AvroSerializer(self.schema_registry_client, AVRO_SCHEMA)

        # Khởi tạo Kafka Producer
        self.producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    def produce(self, key, value):
        """
        Gửi dữ liệu Avro đến Kafka
        """
        self.producer.produce(
            topic=self.topic,
            key=StringSerializer()("{}".format(key), None),
            value=self.avro_serializer(value, None),
        )
        self.producer.flush()
        print(f"Produced: {value}")


# Test producer
if __name__ == "__main__":
    producer = AvroProducer("test_topic")
    test_data = {"id": 1, "name": "Kafka Avro"}
    producer.produce("key1", test_data)
