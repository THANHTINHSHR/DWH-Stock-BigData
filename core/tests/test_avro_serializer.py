from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import Schema

import json, os


class AvroKafkaProducer:
    """Kafka Producer using Avro serialization"""

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


class Test1:
    def __init__(self):
        self.schema_registry_client = SchemaRegistryClient(
            {"url": "http://localhost:8081"}  # <-- dùng "url" là đúng trong lib này
        )

        self.load_one_schema()
        super().__init__()
        pass

    def load_one_schema(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path_scm = os.path.join(base_dir, "trade.avro")
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

    # Initialize the Producer
    producer = AvroKafkaProducer(
        KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC_NAME, AVRO_SCHEMA
    )

    # Send a test message
    test_data = {"id": 1, "name": "Kafka Avro OOP thannh tinh"}

    producer.send_message("test-key", test_data)
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_schema = Schema(AVRO_SCHEMA, schema_type="AVRO")

    subject = "test-topic-value"
    try:
        schema_id = schema_registry_client.register_schema(subject, avro_schema)
        print(f"✅ Registered schema with ID: {schema_id}")
    except Exception as e:
        print(f"❌ Error registering schema: {e}")
    test = Test1()
