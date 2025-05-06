from core.streaming.kafka.producer_manager import ProducerManager
from core.streaming.influxDB.influxDB_creator import InfluxDBConnector
from core.streaming.grafana.grafana_creator import GrafanaCreator
from core.streaming.athena.athena_creator import AthenaCreator
from core.streaming.superset.superset_creator import SupersetCreator
from core.streaming.kafka.topic_creator import TopicCreator
from core.streaming.spark.trade_pipeline import TradePipeline
from core.streaming.spark.ticker_pipeline import TickerPipeline
from core.streaming.spark.book_ticker_pipeline import BookTickerPipeline
from concurrent.futures import ThreadPoolExecutor
import asyncio
import logging


class RunStreaming:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.topic_creator = TopicCreator()
        self.producer = ProducerManager()
        self.influxDB = InfluxDBConnector()
        self.grafana = GrafanaCreator()
        self.athena = AthenaCreator()
        self.superset = SupersetCreator()
        self.trade_pipeline = TradePipeline()
        self.ticker_pipeline = TickerPipeline()
        self.book_ticker_pipeline = BookTickerPipeline()

    @staticmethod
    def run_async_producer(producer):
        asyncio.run(producer.start_publish())

    def run(self):
        # Create topic in Kafka
        self.topic_creator.create_topic()

        # Create InfluxDB buckets
        self.influxDB.create_buckets()

        # Create Grafana dashboards
        self.grafana.run_grafana()

        # Create Athena DB and tables
        self.athena.run_athena()

        # Create Superset Dataset and chart
        self.superset.run_superset()

        # Run producer + pipelines concurrently
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(self.run_async_producer, self.producer)
            executor.submit(self.trade_pipeline.run_streams)
            executor.submit(self.ticker_pipeline.run_streams)
            executor.submit(self.book_ticker_pipeline.run_streams)


if __name__ == "__main__":
    run = RunStreaming()
    run.run()
