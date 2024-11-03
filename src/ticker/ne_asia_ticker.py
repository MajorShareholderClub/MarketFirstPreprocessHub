from src.common.common_ticker import BaseAsyncTickerProcessor
from datetime import datetime, timezone


# 아시아 거래소 티커 프로세서
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


# NE 거래소 티커 프로세서
class BinanceAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("E", **data)

    def get_timestamp(self, ticker: dict) -> int:
        """OKX는 timestamp 접근 방식이 다르므로 재정의."""
        return ticker[self.time]


class KrakenAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__(None, **data)

    def get_timestamp(self, ticker: str) -> int:
        return int(datetime.now(timezone.utc).timestamp())
