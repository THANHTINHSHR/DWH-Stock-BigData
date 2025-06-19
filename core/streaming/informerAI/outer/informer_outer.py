# autopep8: off
from typing_extensions import Self
import findspark  # type: ignore
findspark.init()
from core.streaming.informerAI.outer.athena_creator_predict import AthenaCreatorPredict  # type: ignore
from core.streaming.informerAI.outer.superset_creator_predict import SupersetCreatorPredict  # type: ignore
from core.streaming.informerAI.outer.influxDB_creator_predict import InfluxDBConnectorPredict  # type: ignore
from core.streaming.informerAI.outer.grafana_creator_predict import GrafanaCreatorPredict  # type: ignore

from pyspark.sql import DataFrame  # type: ignore

import logging
# autopep8: on


class InformerOuter():
    _instance = None
    _initialized = False

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super(InformerOuter, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if InformerOuter._initialized:
            return
        # Class container :
        self.athena_creator_predict = AthenaCreatorPredict()
        self.superset_creator_predict = SupersetCreatorPredict()
        self.influxDB_creator_predict = InfluxDBConnectorPredict()
        self.grafana_creator_predict = GrafanaCreatorPredict()

        # Log
        self.logger = logging.getLogger(self.__class__.__name__)

        InformerOuter._initialized = True

    def run_creator(self):
        self.athena_creator_predict.run_athena()
        self.superset_creator_predict.run_superset()
        self.influxDB_creator_predict.create_buckets()
        self.grafana_creator_predict.run_grafana

    def send_predict_data(self, type: str, sparkDF: DataFrame):
        self.influxDB_creator_predict.send_bulk_data(type, sparkDF)
