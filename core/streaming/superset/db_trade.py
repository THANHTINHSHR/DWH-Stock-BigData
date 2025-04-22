from dotenv import load_dotenv
import os
import requests
import json
import logging
import uuid

load_dotenv()


class DBTrade:
    def __init__(self, SUPERSET_URL, session):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("✅ Superset DBTrade initialized")
        self.SUPERSET_URL = SUPERSET_URL
        self.session = session
        self.title = "trade"  # table name in Superset

    def get_datasource_id(self):
        url = f"{self.SUPERSET_URL}/api/v1/dataset/"
        params = {
            "q": json.dumps(
                {"filters": [{"col": "table_name", "opr": "eq", "value": self.title}]}
            )
        }
        res = self.session.get(url, params=params)
        if res.status_code == 200:
            result = res.json()
            if result["count"] > 0:
                datasource_id = result["result"][0]["id"]
                self.logger.info(
                    f"✅ Found datasource_id for '{self.title}': {datasource_id}"
                )
                return datasource_id
            else:
                self.logger.warning(f"⚠️ Dataset '{self.title}' not found.")
        else:
            self.logger.error(f"❌ Failed to fetch datasource_id: {res.text}")
        return None

    def create_chart(self):
        url = f"{self.SUPERSET_URL}/api/v1/chart/"
        payload = {
            "slice_name": "Trade Overview by Symbol",
            "viz_type": "bar",
            "datasource_id": self.get_datasource_id(),
            "datasource_type": "table",
            "params": json.dumps(
                {
                    "metrics": [
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "quantity"},
                            "aggregate": "SUM",
                            "label": "Total Volume",
                        },
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "trade_id"},
                            "aggregate": "COUNT",
                            "label": "Trade Count",
                        },
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "price"},
                            "aggregate": "AVG",
                            "label": "Avg Price",
                        },
                    ],
                    "groupby": ["symbol"],
                    "time_range": "Last 7 days",
                    "legend_position": "top",
                    "color_scheme": "bnbColors",
                }
            ),
        }
        res = self.session.post(url, json=payload)
        if res.status_code == 201:
            self.logger.info("✅ Chart created")
            return res.json()["id"]
        self.logger.error(f"❌ Failed to create chart: {res.text}")
        return None

    def link_chart_to_dashboard(self, chart_id, dashboard_id):
        position_json = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {
                "type": "GRID",
                "id": "GRID_ID",
                "children": ["ROW_ID"],
                "parents": ["ROOT_ID"],
            },
            "ROW_ID": {
                "type": "ROW",
                "id": "ROW_ID",
                "children": ["CHART-1"],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "TRANSPARENT"},
            },
            "CHART-1": {
                "type": "CHART",
                "id": "CHART-1",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", "ROW_ID"],
                "meta": {
                    "chartId": chart_id,
                    "uuid": str(uuid.uuid4()),
                    "width": 4,
                    "height": 4,
                    "sliceName": "Trade Overview by Symbol",
                },
            },
        }

        url = f"{self.SUPERSET_URL}/api/v1/dashboard/{dashboard_id}"
        payload = {"position_json": json.dumps(position_json)}
        res = self.session.put(url, json=payload)
        if res.status_code == 200:
            self.logger.info("✅ Chart linked to dashboard")
        else:
            self.logger.error(f"❌ Failed to link chart: {res.text}")

    def create_dashboard(self):
        url = f"{self.SUPERSET_URL}/api/v1/dashboard/"
        payload = {
            "dashboard_title": self.title,
            "json_metadata": "{}",
            "published": True,
            "position_json": "{}",
        }
        res = self.session.post(url, json=payload)
        self.logger.info(f"✅ dashboard status code {res.status_code}")
        if res.status_code == 201:
            self.logger.info(f"✅ Created dashboard {self.title}")
            dashboard_id = res.json().get("id")
            if dashboard_id:
                self.logger.info(f"✅ Dashboard ID: {dashboard_id}")
                return dashboard_id
            else:
                self.logger.error("❌ No dashboard ID returned.")
        else:
            print(res.text)
            self.logger.error(f"❌ Failed to create dashboard: {res.text}")
        return None

    def run(self):
        dashboard_id = self.create_dashboard()
        if not dashboard_id:
            return

        chart_id = self.create_chart()
        if not chart_id:
            return

        self.link_chart_to_dashboard(chart_id, dashboard_id)
