import json
from type_model.ticker_model import MarketData, CoinMarketCollection
from src.common.common_consumer import CommonConsumerSettingProcessor

from setting.yml_load import TickerProcessorConfig
from mq.exception import handle_processing_errors


class BaseAsyncTickerProcessor(CommonConsumerSettingProcessor):
    """비동기 티커 데이터를 처리하는 기본 클래스."""

    def __init__(self, time: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.time = time

    @handle_processing_errors
    def data_task_a_crack_ticker(self, ticker: dict) -> CoinMarketCollection:
        region: str = ticker["region"]
        market: str = ticker["market"]
        symbol: str = ticker["symbol"]
        params: dict = TickerProcessorConfig(market=market).ticker_parameter

        for item in ticker["data"]:
            parsed_data = json.loads(item)  # 한 번만 파싱
            return CoinMarketCollection(
                region=region,
                market=market,
                coin_symbol=symbol,
                timestamp=self.get_timestamp(parsed_data),
                data=[
                    MarketData.from_api(
                        api=parsed_data,
                        data=params,
                        exchange=market,
                    ).model_dump()["data"]
                ],
            ).model_dump()

    def get_timestamp(self, ticker: dict) -> int:
        """거래소에 따른 timestamp 처리. 하위 클래스에서 재정의 가능."""
        return ticker[self.time]  # 기본적으로 self.time 키를 사용
