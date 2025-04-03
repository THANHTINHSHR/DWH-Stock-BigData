from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


class AvroKafkaProducer:
    """Kafka Producer using Avro serialization"""

    def __init__(self, bootstrap_servers, schema_registry_url, topic, avro_schema):
        """
        Initialize the Kafka producer with Avro serialization.

        :param bootstrap_servers: Kafka broker address (e.g., "localhost:9092")
        :param schema_registry_url: Schema Registry URL (e.g., "http://localhost:8081")
        :param topic: Kafka topic name
        :param avro_schema: Avro schema in JSON format (as a string)
        """
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
        """
        Callback function to check if the message was successfully delivered.

        :param err: Error message (if any)
        :param msg: Kafka message metadata
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def send_message(self, key, value):
        """
        Send a message to Kafka topic.

        :param key: Message key (string)
        :param value: Message value (must match Avro schema)
        """
        self.producer.produce(
            topic=self.topic, key=key, value=value, on_delivery=self.delivery_report
        )
        self.producer.flush()


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
    test_data = {"id": 1, "name": "Kafka Avro OOP"}
    print(f"✅ KAFKA_BOOTSTRAP_SERVERS {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"✅ SCHEMA_REGISTRY_URL {SCHEMA_REGISTRY_URL}")
    producer.send_message("test-key", test_data)
