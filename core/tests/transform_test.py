import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from core.transform.schema_detector import SchemaDetector
import asyncio


class TransformTest:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("TransformTest")
            .config("spark.sql.caseSensitive", "true")
            .getOrCreate()
        )


if __name__ == "__main__":
    test = TransformTest()
    sd = SchemaDetector()
    json_raw = asyncio.run(sd.read_s3())
    asyncio.run(sd.detect_schema(json_raw))
