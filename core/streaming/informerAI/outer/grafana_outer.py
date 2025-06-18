
import logging
import os
import requests
from dotenv import load_dotenv
load_dotenv()


class GrafanaOuter():
    def __init__(self) -> None:
        self.GRAFANA_URL = os.getenv("GRAFANA_URL")
        self.GRAFANA_KEY = os.getenv("GRAFANA_KEY")
        self.GRAFANA_DB_URL = os.getenv("GRAFANA_DB_URL")
        self.INFLUX_ORG = os.getenv("INFLUXDB_ORG")
        self.INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
        self.logger = logging.getLogger(self.__class__.__name__)

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

    def create_dashboard_data(self):
        dashboard = {
            "dashboard": {
                "title": "Realtime Trade Binance",
                "schemaVersion": 30,
                "version": 1,
                "refresh": "10s",
                "panels": [
                    self.create_predict_panel(),
                ],
            },
            "overwrite": True,
        }

    def create_predict_panel(self):
        return {
            "type": "timeseries",
            "title": "Predict Panel",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
    from(bucket: "trade")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) =>
        r._measurement == "trade" and
        r._field == "price"
    )
    |> group(columns: ["symbol"])
    |> aggregateWindow(every: 5s, fn: mean)
    |> yield(name: "mean")
    """,
                    "refId": "A",
                    "interval": "5s",
                }
            ],
            "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8},
        }
