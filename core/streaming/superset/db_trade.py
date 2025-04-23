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

    def get_datasource_id(self, access_token):
        url = f"{self.SUPERSET_URL}/api/v1/dataset/"
        params = {
            "q": json.dumps(
                {"filters": [{"col": "table_name", "opr": "eq", "value": self.title}]}
            )
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        res = self.session.get(url, params=params, headers=headers)
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

    def create_chart(self, access_token):
        url = f"{self.SUPERSET_URL}/api/v1/chart/"
        payload = {
            "slice_name": "Trade Volume by Symbol",
            "viz_type": "bar",
            "datasource_id": self.get_datasource_id(access_token),
            "datasource_type": "table",
            "params": json.dumps(
                {
                    "metrics": [
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "quantity"},
                            "aggregate": "SUM",
                            "label": "Total Volume",
                        }
                    ],
                    "groupby": ["symbol"],
                    "row_limit": 100,  # Giới hạn dữ liệu
                    "time_range": "No filter",
                    "show_legend": True,
                    "color_scheme": "bnbColors",
                    "order_desc": True,
                    "adhoc_filters": [],
                }
            ),
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        res = self.session.post(url, json=payload, headers=headers)
        if res.status_code == 201:
            self.logger.info("✅ Chart created")
            return res.json()["id"]
        self.logger.error(f"❌ Failed to create chart: {res.text}")
        return None

    def link_chart_to_dashboard(
        self, chart_id: int, dashboard_id: int, access_token: str
    ):
        self.logger.info(
            f"📌 Chart id {chart_id} try to link to dashboard id {dashboard_id}"
        )

        url = f"{self.SUPERSET_URL}/api/v1/dashboard/{dashboard_id}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        # Lấy thông tin dashboard hiện tại
        res = self.session.get(url, headers=headers)
        if res.status_code != 200:
            self.logger.error(f"❌ Failed to fetch dashboard: {res.text}")
            return

        dashboard = res.json()["result"]
        position_json = json.loads(dashboard["position_json"])
        max_y = max(
            [
                v.get("meta", {}).get("chartId", 0)
                for v in position_json.values()
                if v["type"] == "CHART"
            ],
            default=0,
        )

        # Tạo node mới cho chart
        chart_key = f"CHART-{chart_id}"
        position_json[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "meta": {
                "chartId": chart_id,
                "width": 4,
                "height": 4,
            },
            "children": [],
        }

        # Gắn vào container GRID_ID
        for key, value in position_json.items():
            if value.get("type") == "GRID":
                value.setdefault("children", []).append(chart_key)
                break

        # Gửi update
        payload = {
            "dashboard_title": dashboard["dashboard_title"],
            "position_json": json.dumps(position_json),
        }

        res = self.session.put(url, json=payload, headers=headers)
        if res.status_code == 200:
            self.logger.info(f"✅ Chart {chart_id} linked to Dashboard {dashboard_id}")
        else:
            self.logger.error(f"❌ Error when linking chart: {res.text}")

    def create_dashboard(self, access_token):

        url = f"{self.SUPERSET_URL}/api/v1/dashboard/"
        payload = {
            "dashboard_title": self.title,
            "json_metadata": "{}",
            "published": True,
            "position_json": "{}",
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        res = self.session.post(url, json=payload, headers=headers)
        self.logger.info(f"✅ dashboard status code :{res.status_code}")
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

    def run(self, access_token):
        dashboard_id = self.create_dashboard(access_token)
        if not dashboard_id:
            return

        chart_id = self.create_chart(access_token)
        if not chart_id:
            return

        self.link_chart_to_dashboard(chart_id, dashboard_id, access_token)
