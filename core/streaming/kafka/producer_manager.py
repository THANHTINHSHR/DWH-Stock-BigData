from core.extract.binance_wss.topic_creator import TopicCreator
from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from core.extract.binance_wss.schema_avro.schema_registry_connector import (
    SchemaRegistryConnector,
)
import io, os, requests, asyncio, json, websockets
from dotenv import load_dotenv

load_dotenv()


class ProducerManager:
    def __init__(self):

        # Load environment variables
        self.WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT"))
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
        self.registry = SchemaRegistryConnector.get_instance()
        # List of producers and serializers base on coin
        self.producers = {}
        self.serializers = {}
        self.load_top_coins_list()
        print(f"📌 STREAM_TYPES: {self.STREAM_TYPES}")
        print(f"TOPCOIN: {TopicCreator.TOPCOIN}")

    def load_top_coins_list(self):
        for symbol in TopicCreator.TOPCOIN:
            self.get_producer(symbol)
            print("✅ Producer created")
        for stream_type in self.STREAM_TYPES:
            self.get_serializer(stream_type)
            print("✅ Serializer created")

    def get_producer(self, symbol):
        conf = {
            "bootstrap.servers": self.BOOTSTRAP_SERVERS,
        }
        if symbol not in self.producers:
            self.producers[symbol] = Producer(conf)
        return self.producers[symbol]

    def get_serializer(self, stream_type: str):
        # if dont have - creat one
        if stream_type not in self.serializers:
            schema_obj = self.registry.get_schema_by_name(stream_type)
            schema = schema_obj.schema.schema_str
            client = self.registry.get_client()
            self.serializers[stream_type] = AvroSerializer(client, schema)
        return self.serializers[stream_type]

    def send_message(self, producer: Producer, topic, key, message):
        try:
            # Serialize and send message to Kafka
            serializer = self.get_serializer(stream_type=key)
            # Define serializer
            serialized_value = serializer(
                message, SerializationContext(topic, MessageField.VALUE)
            )
            producer.produce(
                topic=topic,
                key=str(key),
                value=serialized_value,
                on_delivery=self.delivery_report,
            )
            print(f"✅ Message produced to topic: {topic}")
        except Exception as e:
            print(f"❌ Error producing message: {e}")

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f"❌ Delivery failed: {err}")
        else:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    async def fetch_stream(self, stream_type, symbol):
        """Open WebSocket connection and handle reconnection"""
        url = f"{self.WSS_ENDPOINT}/{symbol}@{stream_type}"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    print(f"📡 Establish wss streaming")
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)
                        product = self.get_producer(symbol=symbol)
                        # Sending message to Kafka with schema
                        self.send_message(
                            product,
                            f"{self.BINANCE_TOPIC}_{symbol}",
                            stream_type,
                            data,
                        )

            except Exception as e:
                print(f"🔄 Reconnecting WebSocket {url} due to error: {e}")
                await asyncio.sleep(3)

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently"""
        tasks = []
        for symbol in TopicCreator.TOPCOIN:
            for stream_type in self.STREAM_TYPES:
                print(f"📡 starting WSS")
                tasks.append(self.fetch_stream(stream_type, symbol))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    tc = TopicCreator()
    producer_manager = ProducerManager()
    # Start the publish process
    asyncio.run(producer_manager.start_publish())
