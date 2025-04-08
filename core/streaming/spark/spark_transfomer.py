from core.streaming.spark.spark_session_singleton import SparkSessionSingleton
from core.extract.binance_wss.topic_creator import TopicCreator

import os
from dotenv import load_dotenv

load_dotenv()


class SparkTransformer:
    def __init__(self):
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.spark = SparkSessionSingleton.get_spark_session()
        self.streams = []
        # DEBUG - stream counter
        self.counter = 0
        # Load Streams
        self.get_df_streams()

    def read_kafka_topic(self, symbol, stream_type):
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
            .option("startingOffsets", "latest")
            .option("subscribe", f"{self.BINANCE_TOPIC}_{symbol}")
            .option("groupId", f"{stream_type}")
            .load()
        )
        return df

    def get_df_streams(self):
        TOPCOIN = TopicCreator.TOPCOIN
        for symbol in TOPCOIN:
            for stream_type in self.STREAM_TYPES:
                df = self.read_kafka_topic(symbol, stream_type)
                self.streams.append(df)
                # Debug
                self.counter += 1

    def show_df_streams(self):
        print(f"✅ Number of streams created: {len(self.streams)}")

        self.streams[0].writeStream.outputMode("append").format(
            "console"
        ).start().awaitTermination()


if __name__ == "__main__":
    tc = TopicCreator()
    st = SparkTransformer()
    st.show_df_streams()
