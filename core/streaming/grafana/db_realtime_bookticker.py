class DBRealtimeBookTicker:
    def __init__(self):
        self.type = "bookTicker"
        self.dashboard = {
            "dashboard": {
                "title": "Realtime BookTicker Dashboard",
                "schemaVersion": 30,
                "version": 1,
                "refresh": "10s",
                "panels": [
                    self.create_bid_ask_timeseries_panel(),
                    self.create_spread_percentage_timeseries_panel(),
                    # self.create_spread_gauge_panel(),
                    # self.create_depth_barchart_panel(),
                ],
            },
            "overwrite": True,
        }

    def create_bid_ask_timeseries_panel(self):

        return {
            "type": "timeseries",
            "title": "Best Bid/Ask Price Over Time",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "bookTicker")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "bookTicker")
  |> filter(fn: (r) => r._field == "best_ask_price" or r._field == "best_bid_price")
  |> group(columns: ["symbol", "_field"]) // Group by symbol and field for correct aggregation
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "aggregated_price") // Yield name updated for clarity

""",
                    "refId": "A",
                }
            ],
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": 8},
        }

    def create_spread_percentage_timeseries_panel(self):
        return {
            "type": "timeseries",
            "title": "Spread Percentage Over Time (%)",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
best_bids_data = from(bucket: "bookTicker")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "bookTicker" and r._field == "best_bid_price")
  |> group(columns: ["symbol"])
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)

best_asks_data = from(bucket: "bookTicker")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "bookTicker" and r._field == "best_ask_price")
  |> group(columns: ["symbol"])
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)

join(
  tables: {bid: best_bids_data, ask: best_asks_data},
  on: ["_time", "symbol"]
)
|> map(fn: (r) => ({
    _time: r._time,
    _value: if exists r._value_bid and r._value_bid != 0.0 and exists r._value_ask then (r._value_ask - r._value_bid) / r._value_bid * 100.0 else float(v: "NaN"),
    symbol: r.symbol,
    _field: "spread_percentage"
}))
|> group(columns: ["symbol", "_field"])
|> yield(name: "percentage_spread_over_time")""",
                    "refId": "B",  # Assuming this is the next active panel
                }
            ],
            "gridPos": {
                "x": 0,
                "y": 8,
                "w": 24,
                "h": 8,
            },  # Positioned below the first panel
        }

    def create_spread_gauge_panel(self):
        return {
            "type": "gauge",
            "title": "Bid-Ask Spread (Latest)",
            "datasource": "Binance_InfluxDB",
            "fieldConfig": {
                "defaults": {
                    "unit": "percent",
                    "min": 0,
                }
            },
            "targets": [
                {
                    "query": """
import "math"

bid = from(bucket: "bookticker")
|> range(start: -1m)
|> filter(fn: (r) => r._measurement == "bookticker" and r._field == "best_bid_price")
|> last()

ask = from(bucket: "bookticker")
|> range(start: -1m)
|> filter(fn: (r) => r._measurement == "bookticker" and r._field == "best_ask_price")
|> last()

join(tables: {bid: bid, ask: ask}, on: ["symbol"])
|> map(fn: (r) => ({
    _time: r._time,
    _value: (r._value_ask - r._value_bid) / r._value_bid * 100.0,
    symbol: r.symbol
}))
""",
                    "refId": "B",
                }
            ],
            "gridPos": {"x": 0, "y": 8, "w": 6, "h": 4},
        }

    def create_depth_barchart_panel(self):
        return {
            "type": "barchart",
            "title": "Bid/Ask Depth by Symbol",
            "datasource": "Binance_InfluxDB",
            "targets": [
                {
                    "query": """
from(bucket: "bookticker")
|> range(start: -5m)
|> filter(fn: (r) => r._measurement == "bookticker" and (r._field == "best_bid_qty" or r._field == "best_ask_qty"))
|> group(columns: ["symbol", "_field"])
|> last()
|> keep(columns: ["symbol", "_field", "_value"])
""",
                    "refId": "C",
                }
            ],
            "gridPos": {"x": 6, "y": 8, "w": 18, "h": 4},
        }
