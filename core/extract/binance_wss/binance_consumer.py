from confluent_kafka import Consumer, KafkaException
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
        admin_client = Consumer(
            {
                "bootstrap.servers": self.BOOTSTRAP_SERVERS,
                "group.id": "admin-group",
                "enable.auto.commit": False,
            }
        )
        metadata = admin_client.list_topics()
        topics = metadata.topics.keys()
        return [topic for topic in topics if topic.startswith("binance-")]

    def register_topics(self):
        """Create consumers for all binance topics"""
        topics = self.get_binance_topics()

        for topic in topics:
            stream_type = topic.split("-")[-1]  # Extract stream type from topic name
            group_id = f"{self.GROUP_ID_PREFIX}-{stream_type}"  # Create group ID based on stream type

            if topic not in self.consumers:
                consumer = Consumer(
                    {
                        "bootstrap.servers": self.BOOTSTRAP_SERVERS,
                        "group.id": group_id,
                        "auto.offset.reset": "earliest",
                        "enable.auto.commit": True,
                    }
                )
                consumer.subscribe([topic])
                self.consumers[topic] = consumer
                print(f"✅ Registered Consumer for {topic} (Group: {group_id})")

    def push_to_s3(self, topic):
        """Listen to Kafka topic and push messages to S3"""
        if topic not in self.consumers:
            print(f"❌ Consumer for {topic} is not registered.")
            return

        consumer = self.consumers[topic]
        print(f"📥 Listening to {topic}...")

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"❌ Kafka Error: {msg.error()}")
                continue

            try:
                json_data = json.loads(msg.value().decode("utf-8"))
                stream_type = json_data.get("stream", "default")

                # Determine S3 key based on stream type
                s3_key = f"{stream_type}/{uuid.uuid4()}.json"

                self.s3.put_object(
                    Bucket=self.BUCKET_NAME,
                    Key=s3_key,
                    Body=json.dumps(json_data),
                    ContentType="application/json",
                )

                print(f"✅ Pushed to S3: {s3_key}")

            except Exception as e:
                print(f"❌ Error pushing to S3: {e}")

    def start_consumers(self):
        """Run all consumers in separate threads"""
        print(f"✅ Starting consumers...")
        for topic in self.consumers:
            thread = threading.Thread(target=self.push_to_s3, args=(topic,))
            thread.start()


if __name__ == "__main__":
    cm = ConsumerManager()
    cm.register_topics()
    cm.start_consumers()
