from core.streaming.kafka.producer_manager import ProducerManager
from core.streaming.spark.pipeline_base import PipelineBase
from concurrent.futures import ThreadPoolExecutor, wait
import asyncio
import logging
from abc import ABC, abstractmethod

# Main class to orchestrate the entire streaming process.


class RunBase(ABC):
    def __init__(self, stream_type: str, pipline: PipelineBase):
        self.stream_type = stream_type
        self.pipeline = pipline
        logging.basicConfig(
            # Configure basic logging for the application.
            level=logging.INFO,
            format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.producer = ProducerManager()

    @staticmethod
    def run_async_producer(producer: ProducerManager, stream_type: str):
        # Static method to run the Kafka producer's asynchronous publishing process.
        asyncio.run(producer.start_publish(stream_type))

    def run(self):
        # Run producer + Spark pipelines concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit Kafka producer and Spark streaming pipelines to run in parallel.
            producer_future = executor.submit(
                self.run_async_producer, self.producer, self.stream_type)
            pipeline_future = executor.submit(
                self.pipeline.run_streams)
            wait([producer_future, pipeline_future])
