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
            self.SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME")
            self.SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD")

            self.session = requests.Session()

            self.db_trade = DBTrade(self.SUPERSET_URL, self.session)

    def login(self):
        login_url = f"{self.SUPERSET_URL}/api/v1/security/login"
        login_payload = {
            "username": self.SUPERSET_USERNAME,
            "password": self.SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True,
        }

        login_response = self.session.post(login_url, json=login_payload)

        if login_response.status_code == 200:
            login_data = login_response.json()
            access_token = login_data.get("access_token")

            if access_token:
                self.logger.info("✅ Login Success")
                return access_token
            else:
                self.logger.error("❌ Failed to retrieve access token")
                return None
        else:
            self.logger.error(f"❌ Login failed: {login_response.text}")
            return None

    def create_dashboards(self):
        access_token = self.login()
        self.db_trade.run(access_token)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    superset_creator = SupersetCreator()

    superset_creator.create_dashboards()
