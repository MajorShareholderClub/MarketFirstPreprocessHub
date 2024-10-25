import json
from datetime import datetime, timezone
from src.common.common_ticker import BaseAsyncTickerProcessor


class BinanceAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("E", None, **data)

    def get_timestamp(self, ticker: dict) -> int:
        """OKX는 timestamp 접근 방식이 다르므로 재정의."""
        return ticker[self.time]

    def get_data(self, item: dict) -> dict:
        return json.loads(item)


class KrakenAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__(None, "data", **data)

    def get_timestamp(self, ticker: str) -> int:
        return int(datetime.now(timezone.utc).timestamp())

    def get_data(self, item: dict) -> dict:
        return json.loads(item)[self.data_collect][0]
