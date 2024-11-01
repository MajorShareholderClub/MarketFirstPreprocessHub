import json
from src.common.common_ticker import BaseAsyncTickerProcessor


class GateIoAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("time_ms", **data)


class BybitAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("ts", **data)


class OKXAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("ts", **data)
