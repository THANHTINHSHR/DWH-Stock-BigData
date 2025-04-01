from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
import os, logging
from dotenv import load_dotenv

load_dotenv()


class SchemaRegistry:
    def __init__(self):
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.SCHEMA_REGISTRY_URL = os.getenv(
            "SCHEMA_REGISTRY_URL", "http://localhost:8081"
        )
        self.schema_registry_client = SchemaRegistryClient(
            {"url": self.SCHEMA_REGISTRY_URL}
        )

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
            schema_id = self.schema_registry_client.register_schema(
                schema_name, schema_obj
            )
            print(f"✅ Schema '{schema_name}' registered with ID: {schema_id}")
            return schema_id
        except Exception as e:
            print(f"❌ Error registering schema '{schema_name}': {e}")


# Example usage
if __name__ == "__main__":
    schema_registry = SchemaRegistry()
    schema_registry.register_default_schema()
