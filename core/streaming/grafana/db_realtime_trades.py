class DBRealtimeTrades:
    def __init__(self):
        self.type = "trade"
        self.dashboard = {
            "dashboard": {
                "title": "Realtime Trade Binance",
                "schemaVersion": 30,
                "version": 1,
                "refresh": "10s",
                "panels": [
                    self.create_real_time_price_panel(),
                    self.create_transaction_panel(),
                ],
            },
            "overwrite": True,
        }

    def create_real_time_price_panel(self):
        return {
            "type": "timeseries",
            "title": "Average Price",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "trade")
|> range(start: v.timeRangeStart, stop: v.timeRangeStop)
|> filter(fn: (r) =>
    r._measurement == "trade" and
    r._field == "price"
)
|> group(columns: ["symbol"])
|> aggregateWindow(every: 5s, fn: mean)
|> yield(name: "mean")
""",
                    "refId": "A",
                    "interval": "5s",
                }
            ],
            "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8},
        }

    def create_transaction_panel(self):
        return {
            "type": "barchart",
            "title": "Total Transaction Value",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "trade")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "trade")
  |> filter(fn: (r) => r["_field"] == "price" or r["_field"] == "quantity")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({ r with value: r.price * r.quantity }))
  |> group(columns: ["symbol"])
  |> sum(column: "value")
  |> keep(columns: ["_time", "symbol", "value"])
  |> group()
  |> yield(name: "transaction_value_total")
""",
                    "refId": "B",
                    "interval": "5s",
                }
            ],
            "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8},
        }
