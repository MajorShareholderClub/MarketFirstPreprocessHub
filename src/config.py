from dataclasses import dataclass
from typing import TypedDict, Required, Union

from mq.kafka_config import (
    KafkaConfig,
    Region,
    ExchangeInfo,
    TickerConfigExchange,
    OrderbookConfigExchange,
)
from src.ticker.ne_ticker import BinanceAsyncTickerProcessor, KrakenAsyncTickerProcessor
from src.ticker.korea_ticker import (
    UpbithumbAsyncTickerProcessor,
    CoinoneAsyncTickerProcessor,
    KorbitAsyncTickerProcessor,
)
from src.ticker.asia_ticker import (
    BybitAsyncTickerProcessor,
    GateIoAsyncTickerProcessor,
    OKXAsyncTickerProcessor,
)
from src.orderbook.ne_orderbook import (
    BinanceAsyncOrderbookProcessor,
    KrakenAsyncOrderbookProcessor,
)
from src.orderbook.korea_orderbook import (
    UpBithumbAsyncOrderbookProcessor,
    CoinoneKorbitAsyncOrderbookProcessor,
)
from src.orderbook.asia_orderbook import (
    BybitAsyncOrderbookProcessor,
    GateIOAsyncOrderbookProcessor,
    OKXAsyncOrderbookProcessor,
)

TickerClass = (
    UpbithumbAsyncTickerProcessor
    | CoinoneAsyncTickerProcessor
    | KorbitAsyncTickerProcessor
    | BybitAsyncTickerProcessor
    | GateIoAsyncTickerProcessor
    | OKXAsyncTickerProcessor
    | BinanceAsyncTickerProcessor
    | KrakenAsyncTickerProcessor
)
OrderbookClass = (
    UpBithumbAsyncOrderbookProcessor
    | CoinoneKorbitAsyncOrderbookProcessor
    | BinanceAsyncOrderbookProcessor
    | KrakenAsyncOrderbookProcessor
    | BybitAsyncOrderbookProcessor
    | GateIOAsyncOrderbookProcessor
    | OKXAsyncOrderbookProcessor
)


class TickerOrderConfig(TypedDict):
    kafka_config: Required[KafkaConfig]
    class_address: Required[Union[type[TickerClass], type[OrderbookClass]]]


@dataclass
class ProcessorMapping:
    """프로세서 클래스 매핑"""

    ticker: type[TickerClass]
    orderbook: type[OrderbookClass]


class ExchangeProcessors:
    """거래소별 프로세서 매핑"""

    PROCESSORS: dict[str, ProcessorMapping] = {
        # Korean exchanges
        "UPBIT": ProcessorMapping(
            ticker=UpbithumbAsyncTickerProcessor,
            orderbook=UpBithumbAsyncOrderbookProcessor,
        ),
        "BITHUMB": ProcessorMapping(
            ticker=UpbithumbAsyncTickerProcessor,
            orderbook=UpBithumbAsyncOrderbookProcessor,
        ),
        "KORBIT": ProcessorMapping(
            ticker=KorbitAsyncTickerProcessor,
            orderbook=CoinoneKorbitAsyncOrderbookProcessor,
        ),
        "COINONE": ProcessorMapping(
            ticker=CoinoneAsyncTickerProcessor,
            orderbook=CoinoneKorbitAsyncOrderbookProcessor,
        ),
        # Asian exchanges
        "OKX": ProcessorMapping(
            ticker=OKXAsyncTickerProcessor, orderbook=OKXAsyncOrderbookProcessor
        ),
        "GATEIO": ProcessorMapping(
            ticker=GateIoAsyncTickerProcessor, orderbook=GateIOAsyncOrderbookProcessor
        ),
        "BYBIT": ProcessorMapping(
            ticker=BybitAsyncTickerProcessor, orderbook=BybitAsyncOrderbookProcessor
        ),
        # NE exchanges
        "BINANCE": ProcessorMapping(
            ticker=BinanceAsyncTickerProcessor, orderbook=BinanceAsyncOrderbookProcessor
        ),
        "KRAKEN": ProcessorMapping(
            ticker=KrakenAsyncTickerProcessor, orderbook=KrakenAsyncOrderbookProcessor
        ),
    }

    @classmethod
    def get_processor(cls, exchange_name: str) -> ProcessorMapping:
        return cls.PROCESSORS[exchange_name.upper()]


# fmt: off
def create_exchange_configs(is_ticker: bool = True) -> dict[Region, dict[str, TickerOrderConfig]]:
    """거래소 설정 생성"""
    config_class = TickerConfigExchange if is_ticker else OrderbookConfigExchange
    result: dict[Region, dict[str, TickerOrderConfig]] = {}

    for region in Region:
        region_configs: dict[str, TickerOrderConfig] = {}

        for exchange_name in ExchangeInfo.EXCHANGE_CONFIGS:
            config = config_class.get_config(exchange_name)
            if config.region == region:
                processor = ExchangeProcessors.get_processor(exchange_name)
                processor_class = processor.ticker if is_ticker else processor.orderbook

                region_configs[exchange_name.lower()] = TickerOrderConfig(
                    kafka_config=config.kafka_metadata_config(),
                    class_address=processor_class,
                )

        if region_configs:
            result[region] = region_configs

    return result
