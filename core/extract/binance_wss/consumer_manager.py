from confluent_kafka import Consumer, KafkaException, KafkaError
from core.extract.binance_wss.consumer_factory import ConsumerFactory
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from core.extract.binance_wss.schema_avro.schema_registry_connector import (
    SchemaRegistryConnector,
)

import asyncio, os, boto3, json

from dotenv import load_dotenv

load_dotenv()


class ConsumerManager:
    def __init__(self):
        # Kafka
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        # AWS
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.s3 = boto3.client("s3")
        # Consumer
        self.consumers = {}
        self.get_all_consumers()
        # Register Avro deserializer
        self.registry = SchemaRegistryConnector.get_instance()
        self.deserializers = {}
        self.deserialize()

    def get_all_consumers(self):
        """Get all consumers"""
        for stream_type in self.STREAM_TYPES:
            if stream_type not in self.consumers:
                self.consumers[stream_type] = ConsumerFactory().create_consumer(
                    stream_type
                )
        return self.consumers

    async def fetch_stream(self, stream_type, consumer):
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    deserializer = self.deserializers[stream_type]
                    ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                    data = deserializer(msg.value(), ctx)
                    # Get coin symbol from the message
                    coin = data.get("s")
                    key = f"{stream_type}/{coin}/{msg.timestamp()[1]}.json"
                    self.upload_to_s3(data, key)

        except KeyboardInterrupt:
            print(f"⚠️ [{stream_type}] Stopping consumer...")

        finally:
            consumer.close()

    def deserialize(self):
        """Deserialize Avro data"""
        for stream_type in self.STREAM_TYPES:
            schema = self.registry.get_schema_by_name(f"{stream_type}")
            schema_str = schema.schema.schema_str
            deserializer = AvroDeserializer(self.registry.get_client(), schema_str)
            self.deserializers[stream_type] = deserializer
        pass

    def upload_to_s3(self, data, key):
        """Upload data to S3"""
        try:
            # Convert the data to JSON string
            json_data = json.dumps(data)

            # Upload the JSON string to S3
            self.s3.put_object(
                Bucket=self.BUCKET_NAME,
                Key=key,
                Body=json_data,
            )
            print(f"✅ Uploaded {key} to S3 bucket {self.BUCKET_NAME}")

        except Exception as e:
            print(f"❌ Failed to upload {key} to S3: {e}")

    async def start_listen(self):
        tasks = []
        for key, consumer in self.consumers.items():
            tasks.append(self.fetch_stream(key, consumer))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    cm = ConsumerManager()
    asyncio.run(cm.start_listen())
