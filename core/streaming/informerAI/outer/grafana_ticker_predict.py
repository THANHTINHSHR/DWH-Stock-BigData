class GrafanaTickerPredict:

    def __init__(self):
        self.type = "ticker"
        self.dashboard = {
            "dashboard": {
                "title": "Ticker_predict Dashboard",
                "schemaVersion": 30,
                "version": 1,
                "refresh": "10s",
                "panels": [
                    self.create_ticker_predict_price_panel(),
                    self.create_ticker_predict_best_bid_ask_panel(),
                    self.create_ticker_predict_trade_count_panel(),

                ],
            },
            "overwrite": True,
        }

    def create_ticker_predict_price_panel(self):
        return {
            "type": "timeseries",
            "title": "Last Price Predict",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "ticker_predict")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "ticker")
  |> filter(fn: (r) => r["_field"] == "last_price")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "mean")
""",
                    "refId": "A",
                    "interval": "5s",
                }
            ],
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": 8},
        }

    def create_ticker_predict_best_bid_ask_panel(self):
        return {
            "type": "timeseries",
            "title": "Best bid-ask Predict",
            "datasource": "Binance_InfluxDB",
            "targets": [
               {
                   "query": """
from(bucket: "ticker_predict")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "ticker")
  |> filter(fn: (r) => r["_field"] == "best_bid_price" or r["_field"] == "best_ask_price")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "mean")
""",
                   "refId": "A",
                   "interval": "5s",
               }
            ],

            "gridPos": {"x": 0, "y": 8, "w": 24, "h": 8},
        }

    def create_ticker_predict_trade_count_panel(self):
        return {
            "type": "timeseries",
            "title": "Trade count Predict",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "ticker_predict")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "ticker")
  |> filter(fn: (r) => r["_field"] == "trade_count")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "mean")
""",
                    "refId": "A",
                    "interval": "5s",
                }
            ],

            "gridPos": {"x": 0, "y": 16, "w": 24, "h": 8},
        }
