from dotenv import load_dotenv
import json
import logging

load_dotenv()


class DBTrade:
    def __init__(self, SUPERSET_URL, session):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.SUPERSET_URL = SUPERSET_URL
        self.session = session
        self.title = "trade"

    def create_combined_pie_chart(self, access_token, dataset_id):
        datasource_id = dataset_id

        payload = {
            "slice_name": "Tổng GTGD theo Symbol + is_market_maker",
            "viz_type": "pie",
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "params": json.dumps(
                {
                    "adhoc_filters": [],  # Filters will be injected from dashboard
                    "color_scheme": "bnbColors",
                    "groupby": ["is_market_maker"],  # Group only by is_market_maker
                    "granularity_sqla": "trade_time",
                    "time_grain_sqla": "P1D",
                    "time_range": "No filter",
                    "metric": {
                        "expressionType": "SQL",
                        "sqlExpression": "SUM(price * quantity)",
                        "label": "Tổng GTGD",
                        "optionName": "metric_1",
                    },
                    "row_limit": 10000,
                    "number_format": "SMART_NUMBER",
                    "show_labels": True,
                    "show_legend": True,
                    "donut": False,
                    "viz_type": "pie",
                }
            ),
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        res = self.session.post(
            f"{self.SUPERSET_URL}/api/v1/chart/", json=payload, headers=headers
        )

        if res.status_code == 201:
            chart_id = res.json().get("id")
            print(f"✅ Chart created. ID: {chart_id}")
            return chart_id
        else:
            print(f"❌ Error {res.status_code}: {res.text}")
            return None

    def create_price_timeseries_chart(self, access_token, dataset_id):
        datasource_id = dataset_id

        payload = {
            "slice_name": "Price Over Time per Symbol",
            "viz_type": "line",
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "params": json.dumps(
                {
                    "metrics": [
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "price"},
                            "aggregate": "AVG",
                            "label": "Average Price",
                        }
                    ],
                    "groupby": ["symbol"],
                    "granularity_sqla": "trade_time",
                    "time_range": "Last 1 days",
                    "viz_type": "line",
                    "is_timeseries": True,
                    "order_desc": False,
                    "adhoc_filters": [],
                    "row_limit": 10000,
                }
            ),
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        res = self.session.post(
            f"{self.SUPERSET_URL}/api/v1/chart/", json=payload, headers=headers
        )

        if res.status_code == 201:
            chart_id = res.json().get("id")
            self.logger.info(f"✅ Chart created. ID: {chart_id}")
            return chart_id
        else:
            self.logger.error(f"❌ Error {res.status_code}: {res.text}")
            return None

    def get_chart_meta(self, access_token, chart_id):
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        res = self.session.get(
            f"{self.SUPERSET_URL}/api/v1/chart/{chart_id}", headers=headers
        )
        if res.status_code == 200:
            return res.json().get("result")
        else:
            self.logger.warning(f"⚠️ Cannot fetch chart meta for id {chart_id}")
            return None

    def create_trade_dashboard(self, access_token):
        headers = {"Authorization": f"Bearer {access_token}"}
        dashboard_payload = {
            "dashboard_title": "Trade Dashboard",
            "position_json": "{}",
        }

        response = self.session.post(
            f"{self.SUPERSET_URL}/api/v1/dashboard/",
            json=dashboard_payload,
            headers=headers,
        )

        if response.status_code == 201:
            self.logger.info(f"✅ Create dashboard success!")
            db_id = response.json()["id"]
            return db_id
        else:
            self.logger.error(f"❌ Creat dashboard failed : {response.text}")
            return None

    def link_chart_to_dashboard(self, access_token, dashboard_id, chart_id):
        headers = {"Authorization": f"Bearer {access_token}"}

        url_chart = f"{self.SUPERSET_URL}/api/v1/chart/{chart_id}"
        response_chart = self.session.get(url_chart, headers=headers)

        if response_chart.status_code != 200:
            self.logger.error(
                f"❌ fail when get chart {chart_id}: {response_chart.text}"
            )
            return

        chart_data = response_chart.json().get("result", {})
        slice_name = chart_data.get("sliceName", f"Chart {chart_id}")

        url_get = f"{self.SUPERSET_URL}/api/v1/dashboard/{dashboard_id}"
        response_get = self.session.get(url_get, headers=headers)

        if response_get.status_code != 200:
            self.logger.info("❌ Fail whe get dashboard info:", response_get.text)
            return

        dashboard_data = response_get.json().get("result", {})
        position_json_str = dashboard_data.get("position_json", "{}")
        position_json = json.loads(position_json_str) if position_json_str else {}

        if "ROOT_ID" not in position_json:
            position_json["ROOT_ID"] = {
                "children": ["GRID_ID"],
                "id": "ROOT_ID",
                "type": "ROOT",
            }

        if "GRID_ID" not in position_json:
            position_json["GRID_ID"] = {
                "children": [],
                "id": "GRID_ID",
                "parents": ["ROOT_ID"],
                "type": "GRID",
            }

        if "ROW_ID" not in position_json:
            position_json["ROW_ID"] = {
                "children": [],
                "id": "ROW_ID",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID"],
                "type": "ROW",
            }
            position_json["GRID_ID"]["children"].append("ROW_ID")
        chart_key = f"CHART-{chart_id}"
        position_json[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "meta": {
                "chartId": chart_id,
                "height": 50,
                "sliceName": slice_name,
                "uuid": str(chart_id),
                "width": 4,
            },
            "parents": ["ROOT_ID", "GRID_ID", "ROW_ID"],
        }
        position_json["ROW_ID"]["children"].append(chart_key)
        url_update = f"{self.SUPERSET_URL}/api/v1/dashboard/{dashboard_id}"
        payload = {
            "dashboard_title": dashboard_data.get(
                "dashboard_title", f"Dashboard {dashboard_id}"
            ),
            "position_json": json.dumps(position_json),
        }
        response_update = self.session.put(url_update, json=payload, headers=headers)

        if response_update.status_code == 200:
            self.logger.info("✅ Update dashboard successed!")
        else:
            self.logger.info("❌ update dashboard failed:", response_update.text)

    def get_dashboard_position_json(self, access_token, dashboard_id):
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{self.SUPERSET_URL}/api/v1/dashboard/{dashboard_id}"
        response = self.session.get(url, headers=headers)
        if response.status_code == 200:
            dashboard_data = response.json().get("result", {})
            position_json = dashboard_data.get("position_json", "{}")
            self.logger.info(
                f"✅ dashboard position_json  {dashboard_id}: {position_json}"
            )
            return position_json
        else:
            self.logger.info(
                f"❌ Fail dashboard position_json {dashboard_id}: {response.text}"
            )
            return None

    def run(self, access_token, dataset_id):
        chart_1 = self.create_price_timeseries_chart(access_token, dataset_id)
        chart_2 = self.create_combined_pie_chart(access_token, dataset_id)
        # db_id = self.create_trade_dashboard(access_token)
        # self.link_chart_to_dashboard(access_token, db_id, chart_1)
        # self.link_chart_to_dashboard(access_token, db_id, chart_2)
