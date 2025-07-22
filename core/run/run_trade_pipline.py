from core.streaming.kafka.producer_manager import ProducerManager
from core.streaming.spark.trade_pipeline import TradePipeline
from core.run.run_base import RunBase
import asyncio
import logging

# Main class to orchestrate the entire streaming process.


class RunTradePipline(RunBase):
    def __init__(self):
        stream_type = "trade"
        pipline = TradePipeline()
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
        logging.info("📡🟢📡Starting Trade streaming pipeline📡🟢📡...")
        run = RunTradePipline()
        run.run()
    except Exception as e:
        logging.error(f"❌ Error when running Trade streaming pipeline: {e}")
    finally:
        logging.info("✅✅ Trade streaming pipeline Finish✅✅")
