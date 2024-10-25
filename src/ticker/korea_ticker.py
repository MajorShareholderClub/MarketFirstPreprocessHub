from src.common.common_ticker import BaseAsyncTickerProcessor


class UpbithumbAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", None, **data)

    def get_timestamp(self, ticker: dict) -> int:
        return ticker[self.time]

    def get_data(self, item: dict) -> dict:
        return item


class CoinoneAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", "data", **data)

    def get_timestamp(self, ticker: dict) -> int:
        return ticker["data"][self.time]


class KorbitAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", "data", **data)
