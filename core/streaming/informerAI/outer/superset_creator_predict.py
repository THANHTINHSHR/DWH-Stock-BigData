from core.streaming.informerAI.outer.superset_ticker_predict import SupersetTickerPredict


from dotenv import load_dotenv
import os
import requests
import logging
import json

load_dotenv()


class SupersetCreatorPredict:
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(SupersetCreatorPredict, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        if not hasattr(self, "session"):
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.info("✅ Superset Creator Predict initialized")
            self.SUPERSET_URL = os.getenv("SUPERSET_URL")
            self.SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME")
            self.SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD")
            self.S3_STAGING_DIR = os.getenv("S3_STAGING_DIR")
            self.ATHENA_DB = os.getenv("ATHENA_DB")
            self.ROOT_DB = os.getenv("ROOT_DB").split(",")  # type: ignore
            self.BUCKET_NAME = os.getenv("BUCKET_NAME")
            self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
            self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
            self.AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
            self.ATHENA_URI = f"awsathena+rest://{self.AWS_ACCESS_KEY_ID}:{self.AWS_SECRET_ACCESS_KEY}@athena.{self.AWS_REGION}.amazonaws.com/{self.ATHENA_DB}?s3_staging_dir={self.S3_STAGING_DIR}"

            self.session = requests.Session()

            self.db_ticker_predict = SupersetTickerPredict(
                self.SUPERSET_URL, self.session)

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

    def create_database(self, access_token):
        headers = {"Authorization": f"Bearer {access_token}"}
        payload = {
            "database_name": self.ATHENA_DB,
            "sqlalchemy_uri": self.ATHENA_URI,
            "extra": '{"engine_params":{"connect_args":{"aws_region":"ap-southeast-1"}}}',
        }
        db_resp = requests.post(
            f"{self.SUPERSET_URL}/api/v1/database/", json=payload, headers=headers
        )

        if db_resp.status_code == 201:
            self.logger.info("✅ Athena database created successfully.")
        else:
            self.logger.error(f"❌ Failed to create database: {db_resp.text}")

    def get_database_id(self, access_token, database_name):
        url = f"{self.SUPERSET_URL}/api/v1/database/"
        headers = {"Authorization": f"Bearer {access_token}"}

        res = self.session.get(url, headers=headers)
        if res.status_code == 200:
            result = res.json().get("result", [])
            for db in result:
                if db["database_name"].strip().lower() == database_name.strip().lower():
                    return db["id"]
            self.logger.warning(
                "⚠️ Name database incorrect:", [
                    db["database_name"] for db in result]
            )
        else:
            self.logger.error(
                f"❌ Request failed with status {res.status_code}: {res.text}"
            )
        return None

    def create_dataset(self, access_token, table_name):
        url = f"{self.SUPERSET_URL}/api/v1/dataset/"
        schema = str(self.ATHENA_DB)
        database_id = self.get_database_id(access_token, schema)
        self.logger.info(f" ✅ Database ID : {database_id}")
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
                    ds.get("table_name") == f"{table_name}_predict"
                    and ds.get("schema") == schema
                    and ds.get("database", {}).get("id") == database_id
                ):
                    dataset_id = ds["id"]
                    self.logger.info(
                        f"✅ Dataset already exists with ID: {dataset_id}")
                    return dataset_id
        else:
            self.logger.warning(
                f"⚠️ Failed to fetch dataset list: {check_res.text}")

        # If not found, create a new dataset
        payload = {"database": database_id,
                   "schema": schema, "table_name": f"{table_name}_predict"}
        self.logger.info(f"✅ Payload : {json.dumps(payload)}")

        res = self.session.post(url, json=payload, headers=headers)
        if res.status_code == 201:
            dataset_id = res.json()["id"]
            self.logger.info(f"✅ Dataset created with ID: {dataset_id}")
            return dataset_id
        else:
            self.logger.error(f"❌ Failed to create dataset: {res.text}")
            return None

    def create_datasets(self, access_token):
        self.logger.info(f"✅ Creating Dataset")
        dataset_ids = {}
        for type in self.ROOT_DB:
            dataset_ids[type] = self.create_dataset(access_token, type)
        dataset_ticker_predict_id = self.create_dataset(
            access_token, "ticker")
        dataset_ids["ticker_predict"] = dataset_ticker_predict_id

        return dataset_ids

    def run_superset(self):
        self.logger.info(f"✅ Run Superset")
        # login
        access_token = self.login()
        # Create Athena Database
        self.create_database(access_token)
        # create datasets
        dataset_ids = self.create_datasets(access_token)
        self.db_ticker_predict.run(access_token, dataset_ids)
