from dotenv import load_dotenv
import json
import logging
import requests  # Ensure requests is imported

load_dotenv()


class DBTicker:
    def __init__(self, SUPERSET_URL, session):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.SUPERSET_URL = SUPERSET_URL
        self.session = session  # Session passed from SupersetCreator
        self.title = "ticker"

    def create_heatmap_chart(self, access_token: str, dataset_id: int):
        """Creates a Heatmap chart based on the exact configuration from the API."""
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        params_dict = {
            "viz_type": "heatmap_v2",
            "x_axis": "symbol",
            "groupby": "constant",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {
                    "column_name": "price_change_percent",
                },
                "aggregate": "AVG",
                "sqlExpression": None,
                "label": "AVG(price_change_percent)",
            },
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "subject": "close_time",
                    "operator": "TEMPORAL_RANGE",
                    "comparator": "No filter",
                    "expressionType": "SIMPLE",
                }
            ],
            "row_limit": 10000,
            "sort_x_axis": "alpha_asc",
            "sort_y_axis": "alpha_asc",
            "normalize_across": "heatmap",
            "legend_type": "continuous",
            "linear_color_scheme": "superset_seq_1",
            "xscale_interval": -1,
            "yscale_interval": -1,
            "left_margin": "auto",
            "bottom_margin": "auto",
            "value_bounds": [None, None],
            "y_axis_format": "SMART_NUMBER",
            "x_axis_time_format": "smart_date",
            "show_legend": True,
            "show_percentage": True,
            "extra_form_data": {},
        }

        payload = {
            "slice_name": "Heatmap Price Change Percent Done",  # Chart name
            "viz_type": "heatmap_v2",  # Viz type in main payload
            "datasource_id": dataset_id,  # Dataset ID
            "datasource_type": "table",  # Datasource type
            "params": json.dumps(params_dict),  # Standardized JSON params string
        }
        try:
            response = self.session.post(
                f"{self.SUPERSET_URL}/api/v1/chart/",
                headers=headers,
                json=payload,
                timeout=30,  # Add a timeout (e.g., 30 seconds)
            )
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()

            # Process successful response (specifically 201 Created)
            if response.status_code == 201:
                chart_data = response.json()
                chart_id = chart_data.get("id")
                self.logger.info(
                    f"✅ Heatmap chart '{payload['slice_name']}' created successfully with ID: {chart_id}"
                )
                # Return the result part containing chart info
                return chart_data.get("result")
            else:
                # Log unexpected successful status codes (if any occur after raise_for_status)
                self.logger.warning(
                    f"⚠️ Unexpected successful status code {response.status_code} while creating Heatmap chart: {response.text}"
                )
                return None
        except Exception as e:
            # Log the general error
            self.logger.error(
                f"❌ An unexpected error occurred during heatmap chart creation: {e}"
            )
            return None

    def create_scatter_plot_chart(self, access_token: str, dataset_id: int):
        """Creates a Scatter Plot based on the provided JSON params."""

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        params_dict = {
            "datasource": f"{dataset_id}__table",  # Inject correct dataset ID
            "viz_type": "echarts_timeseries_scatter",  # From JSON
            "x_axis": "price_change_percent",  # From JSON (Used as X-axis value)
            "time_grain_sqla": "P1D",  # From JSON
            "x_axis_sort_asc": True,  # From JSON
            "x_axis_sort_series": "name",  # From JSON
            "x_axis_sort_series_ascending": True,  # From JSON
            "metrics": [  # From JSON (This defines the Y-axis value)
                {
                    "expressionType": "SIMPLE",
                    "column": {
                        # Details about the column, ID might differ but name is key
                        "column_name": "price_change_percent",  # !! From JSON - Y-axis is also price_change_percent !!
                        "type": "FLOAT",
                    },
                    "aggregate": "MAX",  # From JSON
                    "sqlExpression": None,
                    "hasCustomLabel": False,
                    "label": "MAX(price_change_percent)",  # From JSON
                    # "optionName" is usually generated, can be omitted
                }
            ],
            "groupby": [
                "symbol"
            ],  # From JSON (This defines the individual points/series)
            "adhoc_filters": [  # From JSON
                {
                    "clause": "WHERE",
                    "subject": "event_time",
                    "operator": "TEMPORAL_RANGE",
                    "comparator": "No filter",
                    "expressionType": "SIMPLE",
                }
            ],
            "order_desc": True,  # From JSON
            "row_limit": 10000,  # From JSON
            "truncate_metric": True,  # From JSON
            "show_empty_columns": True,  # From JSON
            "comparison_type": "values",  # From JSON
            "annotation_layers": [],  # From JSON
            "forecastPeriods": 10,  # From JSON
            "forecastInterval": 0.8,  # From JSON
            "x_axis_title_margin": 15,  # From JSON
            "y_axis_title_margin": 15,  # From JSON
            "y_axis_title_position": "Left",  # From JSON
            "sort_series_type": "sum",  # From JSON
            "color_scheme": "supersetColors",  # From JSON
            "only_total": True,  # From JSON
            "markerSize": 6,  # From JSON
            "show_legend": True,  # From JSON
            "legendType": "scroll",  # From JSON
            "legendOrientation": "top",  # From JSON
            "x_axis_time_format": "smart_date",  # From JSON
            "rich_tooltip": True,  # From JSON
            "showTooltipTotal": True,  # From JSON
            "showTooltipPercentage": True,  # From JSON
            "tooltipTimeFormat": "smart_date",  # From JSON
            "y_axis_format": "SMART_NUMBER",  # From JSON
            "truncateXAxis": True,  # From JSON
            "y_axis_bounds": [None, None],  # From JSON
            "extra_form_data": {},  # From JSON
            # "dashboards": [] # Not needed when creating
        }
        # Construct the payload
        payload = {
            "slice_name": "Scatter Plot Ticker (Auto)",  # Use a distinct name
            "viz_type": params_dict.get(
                "viz_type", "echarts_timeseries_scatter"
            ),  # Use viz_type from params
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params_dict),  # Convert the final dict to JSON string
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
                    f"✅ Scatter chart '{payload['slice_name']}' created successfully with ID: {chart_id}"
                )
                return chart_data.get("result")
            else:
                self.logger.warning(
                    f"⚠️ Unexpected successful status code {response.status_code} while creating Scatter chart: {response.text}"
                )
                return None
        except Exception as e:
            self.logger.error(
                f"❌ An unexpected error occurred during scatter chart creation: {e}"
            )
            return None

    def run(self, access_token, dataset_ids):

        heatmap_dataset_id = dataset_ids.get("heatmap_ticker")
        heatmap_result = self.create_heatmap_chart(access_token, heatmap_dataset_id)
        scatter_plot_ticker_dataset_id = dataset_ids.get("scatter_plot_ticker")
        scatter_plot_result = self.create_scatter_plot_chart(
            access_token, scatter_plot_ticker_dataset_id
        )
