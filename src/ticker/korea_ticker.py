from src.common.common_ticker import BaseAsyncTickerProcessor


class UpbithumbAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", **data)


class CoinoneAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", **data)


class KorbitAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", **data)
