from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

import os, requests, json
from dotenv import load_dotenv

load_dotenv()


class TopicCreator:
    # Class attribute
    TOPCOIN = []

    def __init__(self):
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.admin_client = AdminClient({"bootstrap.servers": self.BOOTSTRAP_SERVERS})
        self.NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", 1))
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT", 10))
        # init value for TOPCOIN
        self.get_top_coins()
        self.create_topic()

    @classmethod
    def get_TOPCOIN(cls):
        """Class method to access TOPCOIN."""
        return cls.TOPCOIN

    def get_top_coins(self):
        response = requests.get(self.URL_TOP)
        data = response.json()
        # Filter the coins that end with "USDT"
        filtered_coins = [coin for coin in data if coin["symbol"].endswith("USDT")]
        # Sort the filtered coins by quoteVolume and take the top
        top_coins = sorted(
            filtered_coins, key=lambda x: float(x["quoteVolume"]), reverse=True
        )[: self.LIMIT]
        TopicCreator.TOPCOIN = [coin["symbol"].lower() for coin in top_coins]

    def create_topic(self):
        num_streams = len(self.STREAM_TYPES)
        num_partitions = num_streams * len(TopicCreator.TOPCOIN)
        print(f"📌 Num_partitions: {num_partitions}")
        for type in self.STREAM_TYPES:
            topic_name = f"{self.BINANCE_TOPIC}_{type}"  # ex : binance_btcusdt
            new_topic = NewTopic(
                topic_name, num_partitions=num_partitions, replication_factor=1
            )
            # Wait for the topic creation to complete
            futures = self.admin_client.create_topics([new_topic])
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"✅ Topic '{topic}' created successfully")
                except Exception as e:
                    print(f"❌ Failed to create topic '{topic}': {e}")


if __name__ == "__main__":
    topic_creator = TopicCreator()
    print(f"TOPCOIN: {TopicCreator.TOPCOIN}")
    topic_creator.create_topic()
