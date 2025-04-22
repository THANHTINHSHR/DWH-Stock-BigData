from core.streaming.superset.db_trade import DBTrade

from dotenv import load_dotenv
import os, requests, logging

load_dotenv()


class SupersetCreator:
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(SupersetCreator, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        if not hasattr(self, "session"):
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.info("✅ SupersetCreator initialized")
            self.SUPERSET_URL = os.getenv("SUPERSET_URL")
            self.SUPERSET_SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")

            self.session = requests.Session()
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {self.SUPERSET_SECRET_KEY}",
                    "Content-Type": "application/json",
                }
            )

            self.db_trade = DBTrade(self.SUPERSET_URL, self.session)

    def create_dashboards(self):
        self.db_trade.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    superset_creator = SupersetCreator()
    superset_creator.create_dashboards()
