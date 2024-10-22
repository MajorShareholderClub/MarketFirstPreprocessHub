from src.ticker.common_ticker import BaseAsyncTickerProcessor


class GateIoAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self) -> None:
        data = {
            "consumer_topic": "asiaSocketDataInBTC",
            "c_partition": 4,
            "group_id": "ticker",
            "producer_topic": "TestProcess",
            "p_partition": 0,
            "p_key": "TEst",
        }
        super().__init__("time_ms", "result", **data)


class BybitAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self) -> None:
        data = {
            "consumer_topic": "asiaSocketDataInBTC",
            "c_partition": 2,
            "group_id": "ticker",
            "producer_topic": "TestProcess",
            "p_partition": 0,
            "p_key": "TEst",
        }
        super().__init__("ts", "data", **data)


# OKXAsyncTickerProcessor: OKX에 맞는 데이터 접근 방식을 정의
class OKXAsyncTickerProcessor(BaseAsyncTickerProcessor):
    def __init__(self) -> None:
        data = {
            "consumer_topic": "asiaSocketDataInBTC",
            "c_partition": 0,
            "group_id": "ticker",
            "producer_topic": "TestProcess",
            "p_partition": 0,
            "p_key": "TEst",
        }
        super().__init__("ts", "data", **data)

    def get_timestamp(self, ticker: dict) -> int:
        """OKX는 timestamp 접근 방식이 다르므로 재정의."""
        return ticker["data"][0][self.time]  # 기본 self.time 키를 사용해 접근

    def get_data(self, item: dict) -> dict:
        """OKX는 데이터 접근 방식이 다르므로 재정의."""
        return item["data"][0]  # OKX는 ["data"][0]에서 데이터를 접근


async def process_ticker_cp(process) -> None:
    processor = process()

    await processor.initialize()
    try:
        await processor.batch_process_messages()
    finally:
        await processor.cleanup()
