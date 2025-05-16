from dotenv import load_dotenv
import json
import logging
import time  # Import the time module
import urllib.parse

load_dotenv()


class DBTrade:
    def __init__(self, SUPERSET_URL, session):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.SUPERSET_URL = SUPERSET_URL
        self.session = session
        self.title = "trade"

    def chart_exists(self, chart_name, access_token):
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        # Build JSON filter
        filter_dict = {
            "filters": [{"col": "slice_name", "opr": "eq", "value": chart_name}]
        }
        encoded_q = urllib.parse.quote(json.dumps(filter_dict))  # URL encode

        res = self.session.get(
            f"{self.SUPERSET_URL}/api/v1/chart/?q={encoded_q}",
            headers=headers,
        )
        charts = res.json().get("result", [])
        return charts[0]["id"] if charts else None

    def create_combined_pie_chart(self, access_token, dataset_id):
        chart_name = "Total Trade Value by Symbol + Is Market Maker"
        chart_exits = self.chart_exists(chart_name, access_token)
        if chart_exits is None:

            datasource_id = dataset_id

            payload = {
                "slice_name": chart_name,
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
                self.logger.info(f"✅ Chart created. ID: {chart_id}")
                return chart_id
            else:
                print(f"❌ Error {res.status_code}: {res.text}")
                return None
        else:
            self.logger.warning(f"⚠️ Chart ready exists")

    def create_price_timeseries_chart(self, access_token, dataset_id):
        chart_name = "Price Over Time per Symbol"
        chart_exits = self.chart_exists(chart_name, access_token)
        if chart_exits is None:
            datasource_id = dataset_id

            payload = {
                "slice_name": chart_name,
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
                        "time_range": "Last 7 days",
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
        else:
            self.logger.warning(f"⚠️ Chart ready exists : {chart_id}")

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

    def run(self, access_token, dataset_ids):
        trade_dataset_id = dataset_ids.get(self.title)
        self.create_price_timeseries_chart(access_token, trade_dataset_id)
        self.create_combined_pie_chart(access_token, trade_dataset_id)
