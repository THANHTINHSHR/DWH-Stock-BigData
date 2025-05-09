from core.streaming.grafana.db_realtime_trades import DBRealtimeTrades
from core.streaming.grafana.db_realtime_tickers import DBRealtimeTickers
from core.streaming.grafana.db_realtime_bookticker import DBRealtimeBookTicker
import requests
from dotenv import load_dotenv
import os
import logging

load_dotenv()


class GrafanaCreator:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(GrafanaCreator, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "GRAFANA_URL"):
            self.GRAFANA_URL = os.getenv("GRAFANA_URL")
            self.GRAFANA_KEY = os.getenv("GRAFANA_KEY")
            self.GRAFANA_DB_URL = os.getenv("GRAFANA_DB_URL")
            self.INFLUX_ORG = os.getenv("INFLUXDB_ORG")
            self.INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
            self.logger = logging.getLogger(self.__class__.__name__)

            self.connect_data_source()
            self.db_realtime_trades = DBRealtimeTrades()
            self.db_realtime_tickers = DBRealtimeTickers()
            self.db_realtime_bookticker = DBRealtimeBookTicker()

    def connect_data_source(self):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.GRAFANA_KEY}",
        }

        data = {
            "name": "Binance_InfluxDB",
            "type": "influxdb",
            "access": "proxy",
            "url": self.GRAFANA_DB_URL,
            "basicAuth": False,
            "jsonData": {
                "version": "Flux",
                "httpMode": "POST",
                "timeInterval": "10s",
                "organization": self.INFLUX_ORG,
                "defaultBucket": "trades",
                "tlsSkipVerify": True,
            },
            "secureJsonData": {"token": self.INFLUXDB_TOKEN},
        }

        self.logger.info(f"📌 Grafana try to connect datasource")
        res = requests.post(
            f"{self.GRAFANA_URL}/api/datasources", headers=headers, json=data
        )
        if res.status_code != 200:
            self.logger.error(
                f"❌ Failed to connect datasource: {res.status_code} - {res.text}"
            )
        else:
            self.logger.info(f"✅ Connected datasource")

    def create_dashboard(self, dashboard_data):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.GRAFANA_KEY}",
        }

        self.logger.info("📌 Grafana try to create dashboard")
        res = requests.post(
            f"{self.GRAFANA_URL}/api/dashboards/db",
            headers=headers,
            json=dashboard_data,
        )

        if res.status_code != 200:
            self.logger.error(
                f"❌ Failed to create dashboard: {res.status_code} - {res.text}"
            )
        else:
            self.logger.info("✅ Created dashboard successfully")

    def run_grafana(self):
        self.create_dashboard(self.db_realtime_trades.dashboard)
        self.create_dashboard(self.db_realtime_tickers.dashboard)
        self.create_dashboard(self.db_realtime_bookticker.dashboard)


if __name__ == "__main__":
    grafana = GrafanaCreator()
    grafana.run_grafana()
