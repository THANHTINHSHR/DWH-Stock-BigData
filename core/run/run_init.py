from core.streaming.kafka.producer_manager import ProducerManager
from core.streaming.influxDB.influxDB_creator import InfluxDBConnector
from core.streaming.grafana.grafana_creator import GrafanaCreator
from core.streaming.athena.athena_creator import AthenaCreator
from core.streaming.superset.superset_creator import SupersetCreator
from core.streaming.kafka.topic_creator import TopicCreator

import logging


# Main class to orchestrate the entire streaming process.
class RunInit:
    def __init__(self):
        logging.basicConfig(
            # Configure basic logging for the application.
            level=logging.INFO,
            format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.topic_creator = TopicCreator()
        self.influxDB = InfluxDBConnector()
        # Initialize various components needed for the streaming pipeline.
        self.grafana = GrafanaCreator()
        self.athena = AthenaCreator()
        self.superset = SupersetCreator()

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


if __name__ == "__main__":
    logging.basicConfig(
        # Configure basic logging for the application.
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        logging.info("📡🟢📡Starting Init Project streaming pipeline📡🟢📡...")
        run = RunInit()
        run.run()
    except Exception as e:
        logging.error(f"❌ Error when init Project : {e}")
    finally:
        logging.info("✅✅ Finish Init Project ✅✅")
