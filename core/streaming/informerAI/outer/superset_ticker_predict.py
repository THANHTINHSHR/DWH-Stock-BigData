from dotenv import load_dotenv
import json
import logging
import requests
import urllib.parse

load_dotenv()


class SupersetTickerPredict:
    def __init__(self, SUPERSET_URL, session):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.SUPERSET_URL = SUPERSET_URL
        self.session = session  # Session passed from SupersetCreator
        self.title = "ticker"

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

    def create_table_chart(self, access_token: str, dataset_id: int):
        """Creates a Table chart to show raw ticker prediction data."""
        chart_name = "Ticker_predict"
        chart_exists = self.chart_exists(chart_name, access_token)
        if chart_exists is None:

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            # Payload params (from Superset UI export)
            params_dict = {
                "datasource": f"{dataset_id}__table",
                "viz_type": "table",
                "query_mode": "raw",
                "groupby": ["event_time"],
                "time_grain_sqla": "PT1S",
                "temporal_columns_lookup": {"event_time": True},
                "metrics": [],
                "all_columns": [
                    "event_time",
                    "symbol",
                    "last_price",
                    "best_bid_price",
                    "best_ask_price",
                    "trade_count",
                ],
                "percent_metrics": [],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "subject": "event_time",
                        "operator": "TEMPORAL_RANGE",
                        "comparator": "No filter",
                        "expressionType": "SIMPLE",
                    }
                ],
                "order_by_cols": [["event_time", True]],
                "server_pagination": True,
                "server_page_length": 10,
                "table_timestamp_format": "smart_date",
                "allow_render_html": True,
                "show_cell_bars": True,
                "color_pn": True,
                "comparison_color_scheme": "Green",
                "extra_form_data": {},
                "dashboards": [],
            }

            payload = {
                "slice_name": chart_name,
                "viz_type": "table",
                "datasource_id": dataset_id,
                "datasource_type": "table",
                "params": json.dumps(params_dict),
            }

            try:
                response = self.session.post(
                    f"{self.SUPERSET_URL}/api/v1/chart/",
                    headers=headers,
                    json=payload,
                    timeout=30,
                )
                response.raise_for_status()

                if response.status_code == 201:
                    chart_data = response.json()
                    chart_id = chart_data.get("id")
                    self.logger.info(
                        f"✅ Table chart '{chart_name}' created successfully with ID: {chart_id}"
                    )
                    return chart_data.get("result")
                else:
                    self.logger.warning(
                        f"⚠️ Unexpected status {response.status_code} while creating table chart: {response.text}"
                    )
                    return None
            except Exception as e:
                self.logger.error(
                    f"❌ Error occurred while creating table chart: {e}"
                )
                return None
        else:
            self.logger.warning(f"⚠️ Chart already exists")

    def run(self, access_token, dataset_ids):

        ticker_predict_id = dataset_ids.get("ticker_predict")
        self.create_table_chart(access_token, ticker_predict_id)
