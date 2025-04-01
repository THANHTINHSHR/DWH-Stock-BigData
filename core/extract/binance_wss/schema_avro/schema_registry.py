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
        try:
            # Thử kết nối tới Schema Registry
            schemas = self.schema_registry_client.get_subjects()
            print("✅ Connected to Schema Registry. Subjects:", schemas)
        except Exception as e:
            print(f"❌ Failed to connect to Schema Registry: {e}")

    def load_stream_schema(self):
        stream_schema = {}
        base_dir = os.path.dirname(os.path.abspath(__file__))

        for stream_type in self.STREAM_TYPES:
            path_scm = os.path.join(base_dir, f"{stream_type}.avsc")
            logging.info(f" ✅  Loading schema from: {path_scm}")

            with open(path_scm, "r") as schema_file:
                schema_json = schema_file.read()  # Đọc nội dung file giữ nguyên JSON
                stream_schema[stream_type] = schema_json  # Lưu JSON string

        return stream_schema

    def register_default_schema(self):
        stream_schema = self.load_stream_schema()
        for stream_type, schema_arvo in stream_schema.items():
            self.register_schema(stream_type, schema_arvo)

    def register_schema(self, schema_name, schema_arvo):
        try:
            schema_obj = Schema(schema_arvo, schema_type="AVRO")
            print(f"✅ Schema Name :'{schema_name}' loaded from file")
            print(f"✅ Schema Object :'{schema_obj}' loaded from file")
            # Giữ nguyên JSON string
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
