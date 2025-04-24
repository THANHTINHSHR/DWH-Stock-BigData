from dotenv import load_dotenv
import json
import logging

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

    def create_combined_pie_chart(self, access_token):
        datasource_id = self.get_datasource_id(access_token)

        payload = {
            "slice_name": "Tổng GTGD theo Symbol + is_market_maker",
            "viz_type": "pie",
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "params": json.dumps(
                {
                    "adhoc_filters": [],
                    "color_scheme": "bnbColors",
                    "groupby": [
                        {
                            "expressionType": "SQL",
                            "sqlExpression": "CONCAT(symbol, '-', CAST(is_market_maker AS VARCHAR))",
                            "label": "symbol_maker",
                        }
                    ],
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

    def create_price_timeseries_chart(self, access_token):
        datasource_id = self.get_datasource_id(access_token)  # ID bảng 'trade'

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
                    "groupby": ["symbol"],  # một đường mỗi symbol
                    "granularity_sqla": "trade_time",  # cột thời gian
                    "time_range": "Last 1 days",  # có thể chỉnh theo ý bạn
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
            print(f"✅ Chart created. ID: {chart_id}")
            return chart_id
        else:
            print(f"❌ Error {res.status_code}: {res.text}")
            return None

    def run(self, access_token):
        self.logger.info(f"✅ datasource id {self.get_datasource_id(access_token)}")
        self.create_price_timeseries_chart(access_token)
        self.create_combined_pie_chart(access_token)


# self.create_price_distribution_box_chart(access_token)
# self.create_total_volume_bar_chart(access_token)
# self.create_trade_count_pie_chart(access_token)
