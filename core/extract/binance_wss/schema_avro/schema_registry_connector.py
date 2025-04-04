import json
import os
import logging
import requests
from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import Schema

load_dotenv()


class SchemaRegistryConnector:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchemaRegistryConnector, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.STREAM_TYPES = os.getenv("STREAM_TYPES", "").split(",")
        self.SCHEMA_REGISTRY_URL = os.getenv(
            "SCHEMA_REGISTRY_URL", "http://localhost:8081"
        )
        self.schema_registry_client = SchemaRegistryClient(
            {"url": self.SCHEMA_REGISTRY_URL}
        )
        self.schema_name_to_id = {}
        self._initialized = True
        self.register_default_schema()

    def load_stream_schema(self):
        stream_schema = {}
        base_dir = os.path.dirname(os.path.abspath(__file__))

        for stream_type in self.STREAM_TYPES:
            path_scm = os.path.join(base_dir, f"{stream_type}.avsc")
            with open(path_scm, "r", encoding="utf-8") as schema_file:
                print(f"✅ type schema_file: {type(schema_file)}")
                schema_json = schema_file.read()  # Đọc file trước
                schema_dict = json.loads(schema_json)  # Convert string JSON thành dict

                print(f"✅ type schema_json: {type(schema_dict)}")
                stream_schema[stream_type] = schema_dict
                print(f"✅ Loaded schema for stream type: {stream_type}")

        return stream_schema

    def register_default_schema(self):
        """Register all default schemas"""
        stream_schema = self.load_stream_schema()
        for stream_type, schema_avro in stream_schema.items():
            self.register_schema(stream_type, schema_avro)

    def register_schema(self, schema_name, schema_avro):
        try:
            # Trực tiếp sử dụng AVRO_SCHEMA dưới dạng chuỗi JSON hợp lệ
            AVRO_SCHEMA = """
            {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
            }
            """
            schema_obj = Schema(AVRO_SCHEMA, schema_type="AVRO")
            schema_id = self.schema_registry_client.register_schema(
                "test_topic-value", schema_obj
            )

            print(f"✅ Schema '{schema_name}' registered with ID: {schema_id}")
            self.schema_name_to_id[schema_name] = schema_id
            return schema_id
        except Exception as e:
            print(f"❌ Error registering schema '{schema_name}': {e}")

    def get_schema_id(self, schema_name):
        """Get schema ID from cache or Schema Registry"""
        if schema_name in self.schema_name_to_id:
            return self.schema_name_to_id[schema_name]

        try:
            version = self.schema_registry_client.get_version(schema_name, 1)
            schema_id = version.schema_id
            self.schema_name_to_id[schema_name] = schema_id
            return schema_id
        except Exception as e:
            logging.exception(f"❌ Error getting schema ID for '{schema_name}': {e}")
            return None

    def get_schema_by_id(self, schema_id):
        """Get schema details by ID"""
        schema_url = f"{self.SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
        response = requests.get(schema_url)
        return response.json().get("schema")

    def get_schema_by_name(self, schema_name):
        """Get schema details by name"""
        schema_id = self.get_schema_id(schema_name)
        return self.get_schema_by_id(schema_id) if schema_id else None


# Example usage
if __name__ == "__main__":
    schema_registry = SchemaRegistryConnector()
    # schema_registry.register_default_schema()
