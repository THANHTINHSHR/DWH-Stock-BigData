from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import Schema

import json


class AvroKafkaProducer:

    def __init__(self, bootstrap_servers, schema_registry_url, topic, avro_schema):

        self.topic = topic

        # Initialize Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

        # Create Avro Serializer
        self.avro_serializer = AvroSerializer(self.schema_registry_client, avro_schema)

        # Configure Kafka Producer
        self.producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": self.avro_serializer,
        }

        # Initialize Kafka Producer
        self.producer = SerializingProducer(self.producer_config)

    def delivery_report(self, err, msg):

        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def send_message(self, key, value):

        self.producer.produce(
            topic=self.topic, key=key, value=value, on_delivery=self.delivery_report
        )
        self.producer.flush()


if __name__ == "__main__":
    # Kafka & Schema Registry Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"  # or kafka:9093 if run in Docker
    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    TOPIC_NAME = "test_topic"

    # Define Avro Schema
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

    # Initialize the Producer
    producer = AvroKafkaProducer(
        KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC_NAME, AVRO_SCHEMA
    )

    # Send a test message
    test_data = {"id": 1, "name": "Kafka Avro OOP thannh tinh"}
    print(f"✅ KAFKA_BOOTSTRAP_SERVERS {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"✅ SCHEMA_REGISTRY_URL {SCHEMA_REGISTRY_URL}")
    producer.send_message("test-key", test_data)
    print(f"✅ Sent to Kafka: SUCCESS")
    # Khởi tạo Schema Registry Client
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_schema = Schema(AVRO_SCHEMA, schema_type="AVRO")

    # Đăng ký schema nếu chưa tồn tại
    subject = "test-topic-value"  # Quy ước đặt tên: {topic}-value
    try:
        print(f"✅ Schema avro_schema: is dict: {isinstance(avro_schema, dict)}")  #
        print(f"✅type avro_schema: {type(avro_schema)}")
        schema_id = schema_registry_client.register_schema(subject, avro_schema)
        print(f"✅ Registered schema with ID: {schema_id}")
    except Exception as e:
        print(f"❌ Error registering schema: {e}")
