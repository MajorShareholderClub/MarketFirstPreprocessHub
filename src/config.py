from dataclasses import dataclass

import msgspec
from functools import lru_cache

from mq.kafka_config import (
    Region,
    ExchangeInfo,
    TickerConfigExchange,
    OrderbookConfigExchange,
    KafkaMetadataConfig,
)
from src.ticker.korea_ticker import (
    UpbithumbAsyncTickerProcessor,
    CoinoneAsyncTickerProcessor,
    KorbitAsyncTickerProcessor,
)
from src.ticker.ne_asia_ticker import (
    BybitAsyncTickerProcessor,
    GateIoAsyncTickerProcessor,
    OKXAsyncTickerProcessor,
    BinanceAsyncTickerProcessor,
    KrakenAsyncTickerProcessor,
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

# 티커 클래스
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

# 오더북 클래스
OrderbookClass = (
    UpBithumbAsyncOrderbookProcessor
    | CoinoneKorbitAsyncOrderbookProcessor
    | BinanceAsyncOrderbookProcessor
    | KrakenAsyncOrderbookProcessor
    | BybitAsyncOrderbookProcessor
    | GateIOAsyncOrderbookProcessor
    | OKXAsyncOrderbookProcessor
)


class TickerOrderConfig(msgspec.Struct, dict=True, weakref=True):
    kafka_config: KafkaMetadataConfig
    class_address: type[TickerClass] | type[OrderbookClass]

    def to_dict(self) -> dict:
        return {f: getattr(self, f) for f in self.__struct_fields__}


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
            ticker=OKXAsyncTickerProcessor,
            orderbook=OKXAsyncOrderbookProcessor,
        ),
        "GATEIO": ProcessorMapping(
            ticker=GateIoAsyncTickerProcessor,
            orderbook=GateIOAsyncOrderbookProcessor,
        ),
        "BYBIT": ProcessorMapping(
            ticker=BybitAsyncTickerProcessor,
            orderbook=BybitAsyncOrderbookProcessor,
        ),
        # NE exchanges
        "BINANCE": ProcessorMapping(
            ticker=BinanceAsyncTickerProcessor,
            orderbook=BinanceAsyncOrderbookProcessor,
        ),
        "KRAKEN": ProcessorMapping(
            ticker=KrakenAsyncTickerProcessor,
            orderbook=KrakenAsyncOrderbookProcessor,
        ),
    }

    @classmethod
    @lru_cache(maxsize=2)
    def get_processor(cls, exchange_name: str) -> ProcessorMapping:
        return cls.PROCESSORS[exchange_name.upper()]


# fmt: off
@lru_cache(maxsize=2)
def create_exchange_configs(is_ticker: bool = True) -> dict[Region, dict[str, TickerOrderConfig]]:
    """거래소 설정을 생성하고 반환하는 함수
    
    Args:
        is_ticker (bool): 티커 설정 여부. True면 티커, False면 오더북 설정 생성
        
    Returns:
        dict[Region, dict[str, TickerOrderConfig]]: 지역별 거래소 설정 정보
    >>> {
            Region: {
                exchange_name: {
                    kafka_config: 카프카 설정 정보
                    class_address: 프로세서 클래스 주소
                }
            }
        }
    """
    config_class = TickerConfigExchange if is_ticker else OrderbookConfigExchange
    result: dict[Region, dict[str, TickerOrderConfig]] = {}

    # 지역별 거래소 설정 생성
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
                ).to_dict()

        if region_configs:
            result[region] = region_configs

    return result
