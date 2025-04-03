from confluent_kafka import Producer
from core.extract.binance_wss.schema_avro.schema_registry_connector import (
    SchemaRegistryConnector,
)


class SimpleKafkaProducer:
    """
    A simple Kafka Producer using confluent_kafka Producer.
    """

    def __init__(self, bootstrap_servers, topic):
        """
        Initialize the Kafka Producer.

        :param bootstrap_servers: Kafka broker address (e.g., "localhost:9093")
        :param topic: The Kafka topic to produce messages to.
        """
        self.topic = topic
        # Configure Kafka Producer
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.schema_registry = SchemaRegistryConnector()
        self.schema_registry.register_default_schema()

    def delivery_report(self, err, msg):
        """
        Delivery report callback to check if the message was successfully delivered.

        :param err: Error message (if any).
        :param msg: Kafka message metadata.
        """
        if err:
            print(f"❌ Message failed: {err}")
        else:
            print(
                f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def produce_message(self, key, value):
        """
        Produce a message to the Kafka topic.

        :param key: The message key (string).
        :param value: The message value (string).
        """
        self.producer.produce(
            topic=self.topic, key=key, value=value, callback=self.delivery_report
        )
        self.producer.flush()  # Ensure all messages are sent
        print(f"✅ Produced: {value}")


if __name__ == "__main__":
    # Kafka & topic configuration
    bootstrap_servers = "localhost:9093"
    topic = "test_topickafka"

    # Initialize the producer
    producer = SimpleKafkaProducer(bootstrap_servers, topic)

    # Send a test message
    producer.produce_message("key1", "Hello Kafka!")
