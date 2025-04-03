from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
import os, logging, requests
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
            with open(path_scm, "r") as schema_file:
                schema_json = schema_file.read()
                stream_schema[stream_type] = schema_json

        return stream_schema

    def register_default_schema(self):
        stream_schema = self.load_stream_schema()
        for stream_type, schema_arvo in stream_schema.items():
            self.register_schema(stream_type, schema_arvo)

    def register_schema(self, schema_name, schema_arvo):
        try:
            schema_obj = Schema(schema_arvo, schema_type="AVRO")
            # Register schema
            schema_id = self.schema_registry_client.register_schema(
                schema_name, schema_obj
            )
            print(f"✅ Schema '{schema_name}' registered with ID: {schema_id}")
            self.schema_name_to_id[schema_name] = schema_id
            return schema_id
        except Exception as e:
            print(f"❌ Error registering schema '{schema_name}': {e}")

    def get_schema_id(self, schema_name):
        # Get schema ID from cache or Schema Registry
        if schema_name in self.schema_name_to_id:
            return self.schema_name_to_id[schema_name]

        try:
            version = self.schema_registry_client.get_version(schema_name, 1)
            print(f"✅ Got schema ID for '{schema_name}': {version.schema_id}")
            schema_id = version.schema_id
            self.schema_name_to_id[schema_name] = schema_id
            return schema_id
        except Exception as e:
            print(f"❌ Error getting schema ID for '{schema_name}': {e}")
            return None

    def get_schema_by_id(self, schema_id):
        schema_url = f"{self.SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
        response = requests.get(schema_url)
        schema = response.json()["schema"]
        return schema

    def get_schema_by_name(self, schema_name):
        schema_id = self.get_schema_id(schema_name)
        if not schema_id:
            return None
        return self.get_schema_by_id(schema_id)


# Example usage
if __name__ == "__main__":
    schema_registry = SchemaRegistryConnector()
    schema_registry.register_default_schema()
