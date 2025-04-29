from core.streaming.superset.db_trade import DBTrade
from core.streaming.superset.db_ticker import DBTicker

from dotenv import load_dotenv
import os, requests, logging, json

load_dotenv()


class SupersetCreator:
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(SupersetCreator, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        if not hasattr(self, "session"):
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.info("✅ SupersetCreator initialized")
            self.SUPERSET_URL = os.getenv("SUPERSET_URL")
            self.SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME")
            self.SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD")
            self.ATHENA_DB = os.getenv("ATHENA_DB")
            self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")

            self.session = requests.Session()

            self.db_trade = DBTrade(self.SUPERSET_URL, self.session)
            self.db_ticker = DBTicker(self.SUPERSET_URL, self.session)

    def login(self):
        login_url = f"{self.SUPERSET_URL}/api/v1/security/login"
        login_payload = {
            "username": self.SUPERSET_USERNAME,
            "password": self.SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True,
        }

        login_response = self.session.post(login_url, json=login_payload)

        if login_response.status_code == 200:
            login_data = login_response.json()
            access_token = login_data.get("access_token")

            if access_token:
                self.logger.info("✅ Login Success")
                return access_token
            else:
                self.logger.error("❌ Failed to retrieve access token")
                return None
        else:
            self.logger.error(f"❌ Login failed: {login_response.text}")
            return None

    def get_database_id(self, access_token, database_name):
        url = f"{self.SUPERSET_URL}/api/v1/database/"
        headers = {"Authorization": f"Bearer {access_token}"}
        res = self.session.get(url, headers=headers)
        if res.status_code == 200:
            for db in res.json().get("result", []):
                if db["database_name"] == database_name:
                    return db["id"]
        return None

    def create_dataset(self, access_token, table_name):
        url = f"{self.SUPERSET_URL}/api/v1/dataset/"
        database_id = self.get_database_id(access_token, "Superset-Athena")
        schema = str(self.ATHENA_DB)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        # Check if dataset already exists by fetching all and filtering manually
        check_res = self.session.get(f"{url}?page_size=1000", headers=headers)
        if check_res.status_code == 200:
            datasets = check_res.json().get("result", [])
            for ds in datasets:
                if (
                    ds.get("table_name") == table_name
                    and ds.get("schema") == schema
                    and ds.get("database", {}).get("id") == database_id
                ):
                    dataset_id = ds["id"]
                    self.logger.info(f"✅ Dataset already exists with ID: {dataset_id}")
                    return dataset_id
        else:
            self.logger.warning(f"⚠️ Failed to fetch dataset list: {check_res.text}")

        # If not found, create a new dataset
        payload = {"database": database_id, "schema": schema, "table_name": table_name}

        res = self.session.post(url, json=payload, headers=headers)
        if res.status_code == 201:
            dataset_id = res.json()["id"]
            self.logger.info(f"✅ Dataset created with ID: {dataset_id}")
            return dataset_id
        else:
            self.logger.error(f"❌ Failed to create dataset: {res.text}")
            return None

    def create_datasets(self, access_token):
        dataset_ids = {}
        for stream_type in self.STREAM_TYPES:
            dataset_ids[stream_type] = self.create_dataset(access_token, stream_type)
        dataset_heathmap_id = self.create_dataset(access_token, "heatmap_ticker")
        dataset_ids["heatmap_ticker"] = dataset_heathmap_id
        dataset_scatter_plot_id = self.create_dataset(
            access_token, "scatter_plot_ticker"
        )
        dataset_ids["scatter_plot_ticker"] = dataset_scatter_plot_id
        return dataset_ids

    def create_chart(self, access_token, dataset_id, chart_name):
        """Tạo Chart từ Dataset"""
        url = f"{self.SUPERSET_URL}/api/v1/chart/"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        payload = {
            "slice_name": chart_name,
            "viz_type": "time_table",
            "datasource_id": dataset_id,
            "datasource_type": "query",
            "params": json.dumps(
                {
                    "granularity_sqla": "event_time",
                    "time_grain_sqla": None,
                    "time_range": "No filter",
                    "metrics": [
                        {
                            "expressionType": "SQL",
                            "sqlExpression": "MAX(price_change_percent)",
                            "label": "latest_price_change_percent",
                            "optionName": "metric_latest_price_change_percent",
                        }
                    ],
                    "groupby": ["symbol"],
                    "adhoc_filters": [],
                    "row_limit": 10000,
                    "color_pn": True,
                    "show_cell_bars": True,
                }
            ),
        }

        res = self.session.post(url, json=payload, headers=headers)

        if res.status_code == 201:
            chart_id = res.json().get("id")
            self.logger.info(f"✅ Chart created: {chart_id}")
            return chart_id
        else:
            self.logger.error(f"❌ Error creating Chart: {res.text}")
            return None

    def add_chart_to_dashboard(self, access_token, dashboard_id, chart_id):
        """Gắn Chart vào Dashboard"""
        url = f"{self.SUPERSET_URL}/api/v1/dashboard/{dashboard_id}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        dashboard_data = self.session.get(url, headers=headers).json().get("result", {})
        positions = dashboard_data.get("position_json", {})
        slices = dashboard_data.get("charts", [])

        # Add chart ID
        slices.append(chart_id)

        payload = {
            "positions": json.dumps(positions),
            "charts": slices,
        }

        res = self.session.put(url, json=payload, headers=headers)

        if res.status_code == 200:
            self.logger.info(f"✅ Chart {chart_id} added to Dashboard {dashboard_id}")
            return True
        else:
            self.logger.error(f"❌ Error adding Chart to Dashboard: {res.text}")
            return False

    def run(self):
        # login
        access_token = self.login()
        # create datasets
        dataset_ids = self.create_datasets(access_token)
        self.db_trade.run(access_token, dataset_ids)
        self.db_ticker.run(access_token, dataset_ids)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    superset_creator = SupersetCreator()

    superset_creator.run()
