from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import Schema

import json, os
from dotenv import load_dotenv

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
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL").split(",")
        print(f"✅ SCHEMA_REGISTRY_URL: {self.SCHEMA_REGISTRY_URL}")
        self.client = SchemaRegistryClient({"url": self.SCHEMA_REGISTRY_URL[0]})
        self.load_stream_schema()
        self._initialized = True

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_client(self):
        return self._instance.client

    def load_stream_schema(self):
        stream_schema = {}
        base_dir = os.path.dirname(os.path.abspath(__file__))

        for stream_type in self.STREAM_TYPES:
            path_scm = os.path.join(base_dir, f"{stream_type}.avro")
            try:
                with open(path_scm, "r", encoding="utf-8") as schema_file:
                    schema_str = schema_file.read()
                    stream_schema[stream_type] = schema_str
            except FileNotFoundError:
                print(f"⚠️ File not found for stream type: {stream_type} at {path_scm}")
            except Exception as e:
                print(f"⚠️ Error reading schema file for {stream_type}: {e}")

        for schema_type, schema in stream_schema.items():
            try:
                print(f"✅ Registering schema: {schema_type}")
                schema_avro = Schema(schema, schema_type="AVRO")
                subject_name = f"binance_{schema_type}"

                self.get_client().register_schema(subject_name, schema_avro)
            except Exception as e:
                print(f"⚠️ Failed to register schema {schema_type}: {e}")

        print("✅ Finished loading schemas")

    # Get the schema by stream type

    def get_schema_by_name(self, stream_type):
        schema = self.client.get_latest_version(f"{self.BINANCE_TOPIC}_{stream_type}")
        if schema is not None:
            return schema
        else:
            raise ValueError(f"Schema {stream_type} not found in the registry.")


if __name__ == "__main__":

    df = SchemaRegistryConnector()
    # df.load_one_schema()
