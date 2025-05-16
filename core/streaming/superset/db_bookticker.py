import json
import logging
import urllib.parse


class DBBookTicker:
    def __init__(self, SUPERSET_URL, session):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.SUPERSET_URL = SUPERSET_URL
        self.session = session  # Session passed from SupersetCreator
        self.title = "bookticker"

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

    def create_spread_timeseries_chart(self, access_token: str, dataset_id: int):
        chart_name = f"{self.title.capitalize()} Average Spread Over Time"
        chart_exits = self.chart_exists(chart_name, access_token)
        if chart_exits is None:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            # Parameters extracted and adapted from the provided JSON for chart ID 5
            params_dict = {
                "viz_type": "echarts_timeseries_line",
                "datasource": f"{dataset_id}__table",  # Use the provided dataset_id
                "x_axis": "event_time",
                "time_grain_sqla": "P1D",  # Daily granularity, adjust if needed
                "metrics": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "AVG(best_ask_price - best_bid_price)",
                        "label": "AVG Spread (Ask - Bid)",  # More descriptive label
                        "optionName": "metric_avg_spread",  # Give it a unique option name
                        "aggregate": None,  # Not needed for SQL expression
                        "column": None,  # Not needed for SQL expression
                        "datasourceWarning": False,
                        "hasCustomLabel": True,  # Set to true since we provided a label
                    }
                ],
                "groupby": ["symbol"],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "subject": "event_time",
                        "operator": "TEMPORAL_RANGE",
                        "comparator": "No filter",  # Default time range, can be changed
                        "expressionType": "SIMPLE",
                        # "filterOptionName" can be auto-generated or omitted
                    }
                ],
                "limit": 10,  # Limit the number of series (symbols) shown initially
                "row_limit": 10000,  # Standard row limit
                "color_scheme": "supersetColors",  # Standard color scheme
                "seriesType": "line",
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "x_axis_time_format": "smart_date",
                "rich_tooltip": True,
                "y_axis_format": "SMART_NUMBER",
                "extra_form_data": {},
            }

            payload = {
                "slice_name": chart_name,  # Chart name
                "viz_type": params_dict["viz_type"],
                "datasource_id": dataset_id,
                "datasource_type": "table",
                "params": json.dumps(params_dict),  # Convert params dict to JSON string
            }

            try:
                response = self.session.post(
                    f"{self.SUPERSET_URL}/api/v1/chart/",
                    headers=headers,
                    json=payload,
                    timeout=30,
                )
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

                if response.status_code == 201:
                    chart_data = response.json()
                    chart_id = chart_data.get("id")
                    self.logger.info(
                        f"✅ Chart '{payload['slice_name']}' created successfully with ID: {chart_id}"
                    )
                    return chart_data.get(
                        "result"
                    )  # Return the result part containing chart info
                else:
                    # This part might not be reached due to raise_for_status, but good practice
                    self.logger.warning(
                        f"⚠️ Unexpected successful status code {response.status_code} while creating chart: {response.text}"
                    )
                    return None

            except Exception as e:
                # Handle other potential errors (like JSON parsing errors, unexpected issues)
                self.logger.error(
                    f"❌ An unexpected error occurred during chart creation: {e}"
                )
                return None
        else:
            self.logger.warning(f"⚠️ Chart ready exists : {chart_id}")

    def run(self, access_token, dataset_ids):

        book_ticker_dataset_id = dataset_ids.get("bookTicker")
        self.create_spread_timeseries_chart(access_token, book_ticker_dataset_id)
