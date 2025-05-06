from confluent_kafka.admin import AdminClient, NewTopic

# from confluent_kafka import Producer # Unused import

import os, requests, json, logging
from dotenv import load_dotenv

load_dotenv()


class TopicCreator:
    # Class attribute
    TOPCOIN = []

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.admin_client = AdminClient({"bootstrap.servers": self.BOOTSTRAP_SERVERS})
        self.NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", 1))
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT", 10))
        # init value for TOPCOIN
        self.get_top_coins()
        if TopicCreator.TOPCOIN:  # Only create topics if TOPCOIN is populated
            self.create_topic()
        else:
            self.logger.warning("TOPCOIN list is empty. Skipping topic creation.")

    @classmethod
    def get_TOPCOIN(cls):
        """Class method to access TOPCOIN."""
        return cls.TOPCOIN

    def get_top_coins(self):
        try:
            response = requests.get(self.URL_TOP)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            # Filter the coins that end with "USDT"
            filtered_coins = [
                coin
                for coin in data
                if isinstance(coin, dict)
                and "symbol" in coin
                and coin["symbol"].endswith("USDT")
            ]
            # Sort the filtered coins by quoteVolume and take the top
            top_coins = sorted(
                filtered_coins,
                key=lambda x: float(x.get("quoteVolume", 0)),
                reverse=True,
            )[: self.LIMIT]
            TopicCreator.TOPCOIN = [coin["symbol"].lower() for coin in top_coins]
            self.logger.info(
                f"Successfully fetched and updated TOPCOIN list: {TopicCreator.TOPCOIN}"
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get top coins from {self.URL_TOP}: {e}")
            TopicCreator.TOPCOIN = []  # Ensure TOPCOIN is empty on failure
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Failed to decode JSON response from {self.URL_TOP}: {e}"
            )
            TopicCreator.TOPCOIN = []

    def create_topic(self):
        num_streams = len(self.STREAM_TYPES)
        num_partitions = num_streams * len(TopicCreator.TOPCOIN)
        if num_partitions == 0:
            self.logger.warning(
                "Number of partitions would be 0 (no streams or no top coins). Skipping topic creation."
            )
            return
        self.logger.info(
            f"📌 Calculated number of partitions for new topics: {num_partitions}"
        )
        for stream_type_item in self.STREAM_TYPES:
            topic_name = (
                f"{self.BINANCE_TOPIC}_{stream_type_item}"  # ex : binance_btcusdt
            )
            new_topic = NewTopic(
                topic_name, num_partitions=num_partitions, replication_factor=1
            )
            futures = self.admin_client.create_topics([new_topic])
            for topic, future in futures.items():
                try:
                    future.result()
                    self.logger.info(
                        f"✅ Topic '{topic}' created successfully or already exists."
                    )
                except Exception as e:
                    self.logger.error(f"❌ Failed to create topic '{topic}': {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    topic_creator = TopicCreator()
    logging.info(f"TOPCOIN after initialization: {TopicCreator.TOPCOIN}")
    # topic_creator.create_topic() # This is already called in __init__ if TOPCOIN is populated
