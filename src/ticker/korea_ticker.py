from __future__ import annotations
from src.ticker.common_ticker import BaseAsyncTickerProcessor
from enum import Enum


class UpbithumbAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", None, **data)

    def get_timestamp(self, ticker: dict) -> int:
        """OKX는 timestamp 접근 방식이 다르므로 재정의."""
        return ticker[self.time]

    def get_data(self, item: dict) -> dict:
        return item


class CoinonesyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", "data", **data)

    def get_timestamp(self, ticker: dict) -> int:
        return ticker["data"][self.time]


class KorbitsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self, **kafka_meta: dict) -> None:
        data = kafka_meta
        super().__init__("timestamp", "data", **data)


class Exchange(Enum):
    UPBIT = ("upbit", 0)
    BITHUMB = ("bithumb", 2)
    COINONE = ("coinone", 4)
    KORBIT = ("korbit", 6)

    def __init__(self, exchange_name: str, partition: int) -> None:
        self.exchange_name = exchange_name
        self.partition = partition

    @property
    def group_id(self) -> str:
        return f"{self.name}_ticker_group_id"


async def process_ticker_cp(process) -> None:
    kafka_meta = {
        "consumer_topic": "koreaSocketDataInBTC",
        "producer_topic": "TestProcess",
        "c_partition": 6,  # 파티션만 따로 설정,
        "p_partition": 6,
        "group_id": "korbit_ticker_group_id",
        "p_key": "거래소 처리마다 키를 둬야함",
    }

    processor = process(**kafka_meta)

    await processor.initialize()
    try:
        await processor.batch_process_messages()
    finally:
        await processor.cleanup()
