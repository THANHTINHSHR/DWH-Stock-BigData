class DBRealtimeTickers:
    def __init__(self):
        self.type = "ticker"
        self.dashboard = {
            "dashboard": {
                "title": "Realtime Ticker Dashboard",
                "schemaVersion": 30,
                "version": 1,
                "refresh": "10s",
                "panels": [
                    self.create_price_timeseries_panel(),
                    # self.create_trade_stat_panel(),
                    # self.create_volume_barchart_panel(),
                ],
            },
            "overwrite": True,
        }

    def create_price_timeseries_panel(self):
        return {
            "type": "timeseries",
            "title": "Last Price Over Time",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "ticker")
|> range(start: v.timeRangeStart, stop: v.timeRangeStop)
|> filter(fn: (r) => r._measurement == "ticker" and r._field == "last_price")
|> group(columns: ["symbol"])
|> aggregateWindow(every: 5s, fn: mean)
|> yield(name: "last_price")
""",
                    "refId": "A",
                    "interval": "5s",
                }
            ],
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": 8},
        }

    def create_trade_stat_panel(self):
        return {
            "type": "stat",
            "title": "Trade Count",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "ticker")
|> range(start: -1m)
|> filter(fn: (r) => r._measurement == "ticker" and r._field == "trade_count")
|> last()
""",
                    "refId": "B",
                }
            ],
            "gridPos": {"x": 0, "y": 8, "w": 6, "h": 4},
        }

    def create_volume_barchart_panel(self):
        return {
            "type": "barchart",
            "title": "Quote Volume by Symbol",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "ticker")
|> range(start: -5m)
|> filter(fn: (r) => r._measurement == "ticker" and r._field == "quote_volume")
|> group(columns: ["symbol"])
|> last()
|> keep(columns: ["_time", "symbol", "_value"])
""",
                    "refId": "C",
                }
            ],
            "gridPos": {"x": 6, "y": 8, "w": 18, "h": 4},
        }
