import json
import os
import logging
import requests
from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import Schema

load_dotenv()


class DefaultSchema:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DefaultSchema, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
        self.schema_registry_client = SchemaRegistryClient(
            {"url": self.SCHEMA_REGISTRY_URL}
        )
        # self.load_stream_schema()
        self.load_one_schema()

    def load_stream_schema(self):
        stream_schema = []
        base_dir = os.path.dirname(os.path.abspath(__file__))
        for stream_type in self.STREAM_TYPES:
            path_scm = os.path.join(base_dir, f"{stream_type}.avsc")
            with open(path_scm, "r", encoding="utf-8") as schema_file:
                schema_dict = json.loads(schema_file.read())
                schema_dict = json.dumps(schema_dict)
                stream_schema.append(schema_dict)
        # Register all default schemas
        for schema in stream_schema:
            # Register schema with Schema Registry
            schema_avro = Schema(schema, schema_type="AVRO")
            self.schema_registry_client.register_schema(
                "test-topic-schema1", schema_avro
            )
        print("✅ Loaded all schemas")

    def load_one_schema(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path_scm = os.path.join(base_dir, "trade.avro")
        print(f"✅ path_scm: {path_scm}")

        with open(path_scm, "r", encoding="utf-8") as f:
            schema = json.load(f)
            schema_str = json.dumps(schema)
        schema_obj = Schema(schema_str, schema_type="AVRO")
        self.schema_registry_client.register_schema("trade", schema_obj)


if __name__ == "__main__":
    df = DefaultSchema()
