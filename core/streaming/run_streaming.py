from core.streaming.kafka.producer_manager import ProducerManager
from core.streaming.influxDB.influxDB_creator import InfluxDBConnector
from core.streaming.grafana.grafana_creator import GrafanaCreator
from core.streaming.athena.athena_creator import AthenaCreator
from core.streaming.superset.superset_creator import SupersetCreator
from core.streaming.kafka.topic_creator import TopicCreator
from core.streaming.spark.trade_pipeline import TradePipeline
from core.streaming.spark.ticker_pipeline import TickerPipeline
from core.streaming.spark.book_ticker_pipeline import BookTickerPipeline
from concurrent.futures import ThreadPoolExecutor, wait
import asyncio
import logging


# Main class to orchestrate the entire streaming process.
class RunStreaming:
    def __init__(self):
        logging.basicConfig(
            # Configure basic logging for the application.
            level=logging.INFO,
            format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.topic_creator = TopicCreator()
        self.producer = ProducerManager()
        self.influxDB = InfluxDBConnector()
        # Initialize various components needed for the streaming pipeline.
        self.grafana = GrafanaCreator()
        self.athena = AthenaCreator()
        self.superset = SupersetCreator()

        # Initialize Spark streaming pipelines
        self.trade_pipeline = TradePipeline()
        self.ticker_pipeline = TickerPipeline()
        self.book_ticker_pipeline = BookTickerPipeline()

    @staticmethod
    def run_async_producer(producer):
        # Static method to run the Kafka producer's asynchronous publishing process.
        asyncio.run(producer.start_publish())

    def run(self):
        # Main method to start all parts of the streaming application.

        # Create topic in Kafka
        self.topic_creator.create_topic()

        # Create InfluxDB buckets
        self.influxDB.create_buckets()

        # Create Grafana dashboards
        self.grafana.run_grafana()
        # Create Athena DB and tables after Spark might have written some data.
        self.athena.run_athena()

        # Create Superset Dataset and chart.
        self.superset.run_superset()

        # Run producer + Spark pipelines concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit Kafka producer and Spark streaming pipelines to run in parallel.
            producer_future = executor.submit(
                self.run_async_producer, self.producer)
            trade_pipeline_future = executor.submit(
                self.trade_pipeline.run_streams)
            ticker_pipeline_future = executor.submit(
                self.ticker_pipeline.run_streams)
            book_ticker_pipeline_future = executor.submit(
                self.book_ticker_pipeline.run_streams
            )


if __name__ == "__main__":
    logging.basicConfig(
        # Configure basic logging for the application.
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Suppress specific Python loggers if their warnings are not desired
    logging.getLogger("DBTicker").setLevel(logging.ERROR)
    logging.getLogger("DBBookTicker").setLevel(logging.ERROR)

    try:
        logging.info("📡🟢📡Starting Spark streaming pipeline📡🟢📡...")
        run = RunStreaming()
        run.run()
    except Exception as e:
        logging.error(f"❌ Error when running Spark pipeline{e}")
    finally:
        logging.info("✅✅Spark streaming pipeline finished running✅✅")
