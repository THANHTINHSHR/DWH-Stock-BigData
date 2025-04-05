from confluent_kafka.admin import AdminClient, NewTopic
import os
from dotenv import load_dotenv

load_dotenv()


class TopicCreator:
    def __init__(self):
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.admin_client = AdminClient({"bootstrap.servers": self.BOOTSTRAP_SERVERS})
        self.NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", 1))
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")

    def create_topic(self):
        try:
            for stream_type in self.STREAM_TYPES:
                topic_name = f"{self.BINANCE_TOPIC}_{stream_type}"
                new_topic = NewTopic(
                    topic_name, num_partitions=self.NUM_PARTITIONS, replication_factor=1
                )
                self.admin_client.create_topics([new_topic])
                print(f"✅ Topic '{topic_name}' created successfully")
        except Exception as e:
            print(f"❌ Failed to create topic: {e}")


if __name__ == "__main__":
    topic_creator = TopicCreator()
    topic_creator.create_topic()
