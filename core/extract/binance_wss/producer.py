import asyncio
import websockets
import json
from dotenv import load_dotenv
import os
import requests
from abc import ABC, abstractmethod
from kafka import KafkaProducer

load_dotenv()


class Producer(ABC):
    def __init__(self):
        self.WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT"))
        self.STREAM_TYPES = os.getenv("STREAM_TYPES")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.producer = KafkaProducer(bootstrap_servers=self.BOOTSTRAP_SERVERS)

    def send_message(self, topic: str, message: str):
        print(f"✅ Sent message to Kafka! topic: {topic}")
        self.producer.send(topic, message)
        print(f"✅ Plushed message to Kafka! topic: {topic}")
        self.producer.flush()
        pass

    # Get top coins by volume
    def get_top_coins(self):
        response = requests.get(self.URL_TOP)
        data = response.json()
        top_coins = sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)[
            : self.LIMIT
        ]
        return [coin["symbol"].lower() for coin in top_coins]

    @abstractmethod
    async def start_publish(self):
        pass

    async def close(self):
        websockets.close()
        pass
