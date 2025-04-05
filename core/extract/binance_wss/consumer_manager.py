from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import os
import boto3
import uuid
import threading
from dotenv import load_dotenv

load_dotenv()


class ConsumerManager:
    def __init__(self):
        self.consumers = {}
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.GROUP_ID_PREFIX = os.getenv("GROUP_ID_PREFIX", "binance-group")

        # Define S3 client
        self.s3 = boto3.client("s3")

    def get_binance_topics(self):
        """Get all topics that start with 'binance-'"""
        consumer = KafkaConsumer(bootstrap_servers=self.BOOTSTRAP_SERVERS)
        topics = consumer.topics()
        return [topic for topic in topics if topic.startswith("binance-")]


if __name__ == "__main__":
    cm = ConsumerManager()
    cm.register_topics()
    cm.start_consumers()
