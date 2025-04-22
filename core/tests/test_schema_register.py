from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import Schema

import json


def load_one_schema(self):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path_scm = os.path.join(base_dir, "trade.avsc")
    print(f"✅ path_scm: {path_scm}")

    with open(path_scm, "r", encoding="utf-8") as f:
        schema = json.load(f)
        schema_str = json.dumps(schema)
    schema_obj = Schema(schema_str, schema_type="AVRO")
    self.schema_registry_client.register_schema("trade", schema_obj)


if __name__ == "__main__":
    # Kafka & Schema Registry Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"  # Hoặc kafka:9093 nếu chạy trong Docker
    SCHEMA_REGISTRY_URL = (
        "http://localhost:8081"  # Kiểm tra Schema Registry trên localhost
    )
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

    # Send a test message
    test_data = {"id": 1, "name": "Kafka Avro OOP thannh tinh"}

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_schema = Schema(AVRO_SCHEMA, schema_type="AVRO")

    subject = "test-topic-value"
    try:
        schema_id = schema_registry_client.register_schema(subject, avro_schema)
        print(f"✅ Registered schema with ID: {schema_id}")
    except Exception as e:
        print(f"❌ Error registering schema: {e}")
