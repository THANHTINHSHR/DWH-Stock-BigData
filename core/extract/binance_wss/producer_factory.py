from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
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

    def create_producer(self, stream_type: str):
        """Create Producer base on stream_type"""
        schema = self.registry.get_schema_by_name(stream_type)
        schema_str = schema.schema.schema_str
        client = self.registry.get_client()
        serializer = AvroSerializer(
            client,
            schema_str,
        )
        return SerializingProducer(
            {
                "bootstrap.servers": self.BOOTSTRAP_SERVERS,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": serializer,
            }
        )


if __name__ == "__main__":
    producer_factory = ProducerFactory()
    producer = producer_factory.create_producer("trade")
    print(producer)
