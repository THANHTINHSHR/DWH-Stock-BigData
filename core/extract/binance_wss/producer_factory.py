from confluent_kafka import Producer

from confluent_kafka.schema_registry.avro import AvroSerializer

from core.extract.binance_wss.schema_avro.schema_registry_connector import (
    SchemaRegistryConnector,
)
import os
from dotenv import load_dotenv

load_dotenv()


class ProducerFactory:
    def __init__(self):
        self.registry = SchemaRegistryConnector.get_instance()
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")

    def create_producer(self):
        """Create a Kafka producer."""
        conf = {
            "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            "schema.registry.url": self.SCHEMA_REGISTRY_URL,
        }

    return Producer(conf)

    def get_serializer(self, stream_type: str):
        """Return Avro serializer for given stream_type."""
        schema_name = f"{self.BINANCE_TOPIC}_{stream_type}"
        client = self.registry.get_client()
        return AvroSerializer(client, schema_name)

    def send_data(self, data: dict, stream_type: str):
        """Serialize and send data to Kafka."""
        serializer = self.get_serializer(stream_type)
        serialized_value = serializer(data)
        self.producer.produce(
            topic=f"{self.BINANCE_TOPIC}.{stream_type}",
            key=data.get("symbol", ""),  # ví dụ dùng symbol làm key
            value=serialized_value,
        )


if __name__ == "__main__":
    producer_factory = ProducerFactory()
    producer = producer_factory.create_producer("trade")
    print(producer)
