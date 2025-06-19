from core.streaming.informerAI.outer.grafana_ticker_predict import GrafanaTickerPredict
import os
import logging
import requests

from dotenv import load_dotenv
load_dotenv()


class GrafanaCreatorPredict:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(GrafanaCreatorPredict, cls).__new__(
                cls, *args, **kwargs)
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
            self.db_ticker_predict = GrafanaTickerPredict()

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
                f"⚠️  Failed to connect datasource: {res.status_code} - {res.text}"
            )
        else:
            self.logger.info(f"✅ Connected datasource")

    def create_dashboard(self, dashboard_data):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.GRAFANA_KEY}",
        }
        # Get dashbaord from input
        actual_dashboard_definition = dashboard_data.get("dashboard", {})
        dashboard_title = actual_dashboard_definition.get(
            "title", "Untitled Dashboard")
        self.logger.info(f"🔍 Searching conflict dashboard ....")
        search_params = {
            "query": dashboard_title,
            "type": "dash-db",
            "title": dashboard_title,
        }
        try:
            search_res = requests.get(
                f"{self.GRAFANA_URL}/api/search",
                headers=headers,
                params=search_params,
                timeout=10,
            )
            search_res.raise_for_status()
            results = search_res.json()
            for dash in results:
                if dash.get("title") == dashboard_title:
                    self.logger.info(
                        f"📌 Found existing dashboard '{dashboard_title} . ⚠️ STOP CREATE ⚠️ ."
                    )
                    return
        except Exception as e:
            self.logger.error(f"❌ Error searching for existing dashboard: {e}")
            return
        payload_to_send = dashboard_data.copy()
        payload_to_send["dashboard"] = actual_dashboard_definition.copy()

        res = requests.post(
            f"{self.GRAFANA_URL}/api/dashboards/db",
            headers=headers,
            json=payload_to_send,
        )
        if res.status_code == 200:
            self.logger.info(
                f"✅ Dashboard '{dashboard_title}' created successfully.")
        else:
            self.logger.error(
                f"❌ Failed to create dashboard '{dashboard_title}': {res.status_code} - {res.text}"
            )

    def run_grafana(self):
        self.create_dashboard(self.db_ticker_predict.dashboard)
        pass
