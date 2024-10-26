import json
from src.data_format import MarketData, CoinMarketCollection

from setting.yml_load import TickerProcessorConfig
from mq.exception import handle_processing_errors


class BaseAsyncTickerProcessor:
    """비동기 티커 데이터를 처리하는 기본 클래스."""

    def __init__(self, *args) -> None:
        self.time = args[0]
        self.data_collect = args[1]

    @handle_processing_errors
    def data_task_a_crack_ticker(self, ticker: dict) -> CoinMarketCollection:
        market = ticker["market"]
        symbol = ticker["symbol"]
        params = TickerProcessorConfig(market=market).ticker_parameter

        # 첫 번째 데이터의 timestamp를 가져오는 방식
        if isinstance(ticker["data"][0], str):
            first_data = json.loads(ticker["data"][0])
        else:
            first_data = ticker["data"][0]

        timestamp = self.get_timestamp(first_data)

        # 모든 데이터 합산
        data: list[MarketData] = [
            list(
                MarketData.from_api(
                    api=self.get_data(item), data=params, exchange=market
                )
                .model_dump()
                .values()
            )[0]
            for item in ticker["data"]
        ]

        return CoinMarketCollection(
            market=market,
            coin_symbol=symbol,
            timestamp=timestamp,
            data=data,
        ).model_dump()

    def get_timestamp(self, ticker: dict) -> int:
        """거래소에 따른 timestamp 처리. 하위 클래스에서 재정의 가능."""
        return ticker[self.time]  # 기본적으로 self.time 키를 사용

    def get_data(self, item: dict) -> dict:
        """데이터 접근 방식을 처리. 하위 클래스에서 재정의 가능."""
        return json.loads(item)[self.data_collect]
