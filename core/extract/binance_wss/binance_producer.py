import asyncio
import websockets
import json
import sys

print(sys.path)
from dotenv import load_dotenv
import os
import requests
from confluent_kafka import SerializingProducer
from aws_glue_schema_registry.avro_encoder import AvroEncoder
from aws_glue_schema_registry.config import (
    AWSSchemaRegistryClient,
    SchemaRegistryConfig,
)

load_dotenv()


class BinanceProducer:
    def __init__(self):
        # Load environment variables
        self.WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")  # WebSocket endpoint
        self.URL_TOP = os.getenv("URL_TOP")  # API URL to get top coins
        self.LIMIT = int(os.getenv("LIMIT"))  # Number of top coins to process
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")  # Kafka topic
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(
            ","
        )  # List of stream types (e.g., trade, ticker)
        self.BOOTSTRAP_SERVERS = os.getenv(
            "BOOTSTRAP_SERVERS"
        )  # Kafka bootstrap servers
        self.AWS_DEFAULT_REGION = os.getenv(
            "AWS_DEFAULT_REGION"
        )  # AWS region for Glue Schema Registry

        # AWS Glue Schema Registry setup
        schema_registry_conf = SchemaRegistryConfig(region_name=self.AWS_DEFAULT_REGION)
        self.schema_registry_client = AWSSchemaRegistryClient(schema_registry_conf)

        # Kafka Producer
        self.producer = SerializingProducer(
            {
                "bootstrap.servers": self.BOOTSTRAP_SERVERS,
                "key.serializer": str.encode,
            }
        )

    def get_top_coins(self):
        """Fetch the top coins based on trading volume"""
        response = requests.get(self.URL_TOP)
        data = response.json()
        top_coins = sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)[
            : self.LIMIT
        ]
        return [coin["symbol"].lower() for coin in top_coins]

    async def fetch_stream(self, stream_type, symbol):
        """Open WebSocket connection to receive real-time data for a specific stream type and symbol"""
        url = f"{self.WSS_ENDPOINT}/{symbol}@{stream_type}"
        async with websockets.connect(url) as ws:
            while True:
                message = await ws.recv()
                data = json.loads(message)
                topic = self.BINANCE_TOPIC
                schema_name = f"{stream_type}"
                self.send_message(topic, symbol, data, schema_name)

    def send_message(self, topic, key, message, schema_name):
        """Serialize the message using Avro and send it to Kafka"""
        avro_encoder = AvroEncoder(self.schema_registry_client, schema_name=schema_name)
        encoded_value = avro_encoder.encode(message)
        self.producer.produce(topic=topic, key=key, value=encoded_value)
        self.producer.flush()
        print(f"✅ Sent {schema_name} to {topic}")

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently for different symbols and stream types"""
        symbols = self.get_top_coins()
        tasks = []

        for symbol in symbols:
            for stream_type in self.STREAM_TYPES:
                tasks.append(self.fetch_stream(stream_type, symbol))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    producer = BinanceProducer()
    asyncio.run(producer.start_publish())
