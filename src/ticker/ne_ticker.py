import json
from datetime import datetime, timezone
from src.ticker.common_ticker import BaseAsyncTickerProcessor


class BinanceAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self) -> None:
        data = {
            "consumer_topic": "neSocketDataInBTC",
            "c_partition": 0,
            "group_id": "ticker",
            "producer_topic": "TestProcess",
            "p_partition": 0,
            "p_key": "TEst",
        }
        super().__init__("E", None, **data)

    def get_timestamp(self, ticker: dict) -> int:
        """OKX는 timestamp 접근 방식이 다르므로 재정의."""
        return ticker[self.time]

    def get_data(self, item: dict) -> dict:
        return json.loads(item)


class KrakensyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self) -> None:
        data = {
            "consumer_topic": "neSocketDataInBTC",
            "c_partition": 2,
            "group_id": "ticker",
            "producer_topic": "TestProcess",
            "p_partition": 0,
            "p_key": "TEst",
        }
        super().__init__(None, "data", **data)

    def get_timestamp(self, ticker: str) -> int:
        return int(datetime.now(timezone.utc).timestamp())

    def get_data(self, item: dict) -> dict:
        return json.loads(item)[self.data_collect][0]


async def process_ticker_cp(process) -> None:
    processor = process()

    await processor.initialize()
    try:
        await processor.batch_process_messages()
    finally:
        await processor.cleanup()
