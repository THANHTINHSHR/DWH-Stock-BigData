from core.streaming.kafka.producer_manager import ProducerManager
from core.streaming.spark.book_ticker_pipeline import BookTickerPipeline
from core.run.run_base import RunBase
import logging

# Main class to orchestrate the entire streaming process.


class RunBookTickerPipline(RunBase):
    def __init__(self):
        stream_type = "bookTicker"
        pipline = BookTickerPipeline()
        super().__init__(stream_type, pipline)
        logging.basicConfig(
            # Configure basic logging for the application.
            level=logging.INFO,
            format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.producer = ProducerManager()


if __name__ == "__main__":
    logging.basicConfig(
        # Configure basic logging for the application.
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        logging.info("📡🟢📡Starting BookT Ticker streaming pipeline📡🟢📡...")
        run = RunBookTickerPipline()
        run.run()
    except Exception as e:
        logging.error(
            f"❌ Error when running BookT Ticker streaming pipeline: {e}")
    finally:
        logging.info("✅✅ BookT Ticker streaming pipeline Finish✅✅")
