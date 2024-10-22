from typing import Awaitable
from dataclasses import dataclass, asdict
from enum import Enum

from setting.yml_load import TickerProcessorConfig
from src.ticker.ne_ticker import BinanceAsyncTickerProcessor, KrakensyncTickerProcessor
from src.ticker.korea_ticker import (
    UpbithumbAsyncTickerProcessor,
    CoinonesyncTickerProcessor,
    KorbitsyncTickerProcessor,
)
from src.ticker.asia_ticker import (
    BybitAsyncTickerProcessor,
    GateIoAsyncTickerProcessor,
    OKXAsyncTickerProcessor,
)


TickerbookProcessor = type[Awaitable[None]]


@dataclass(frozen=True)
class KafkaConfig:
    consumer_topic: str
    c_partition: int
    p_partition: int
    group_id: str
    producer_topic: str
    p_key: str


class Region(Enum):
    """거래소 지역 구분"""

    KOREA = "Korea"
    ASIA = "Asia"
    NE = "NE"


class KafkaConfigExchange(Enum):
    # 각 거래소에 대해 지역, 이름 및 파티션 설정
    UPBIT = (Region.KOREA, "upbit", 0, 0)
    BITHUMB = (Region.KOREA, "bithumb", 2, 2)
    COINONE = (Region.KOREA, "coinone", 4, 4)
    KORBIT = (Region.KOREA, "korbit", 6, 6)

    OKX = (Region.ASIA, "okx", 0, 0)
    BYBIT = (Region.ASIA, "bybit", 2, 2)
    GATEIO = (Region.ASIA, "gateio", 4, 4)

    BINANCE = (Region.NE, "binance", 0, 0)
    KRAKEN = (Region.NE, "kraken", 2, 2)

    def __init__(
        self, region: Region, exchange_name: str, c_partition: int, p_partition: int
    ) -> None:
        """각 거래소의 지역, 이름, partition 설정"""
        self.region = region
        self.exchange_name = exchange_name
        self.c_partition = c_partition
        self.p_partition = p_partition
        self.config = TickerProcessorConfig(self.exchange_name)

    @property
    def group_id(self) -> str:
        """Kafka consumer group id"""
        group_id = self.config.group_id
        return f"{group_id}_ticker_group_id"

    @property
    def product_topic_name(self) -> str:
        """Kafka producer topic name, 지역에 따라 다르게 설정"""
        product = self.config.producer_topic
        return f"Region.{self.region.value}:{self.exchange_name.capitalize()}:{product}"

    def kafka_metadata_config(self) -> dict[str, str | int]:
        """Kafka configuration for a specific exchange"""
        return asdict(
            KafkaConfig(
                consumer_topic="asiaSocketDataInBTC",  # 각 거래소마다 다른 토픽 사용 가능
                c_partition=self.c_partition,
                p_partition=self.p_partition,
                producer_topic=self.product_topic_name,
                group_id=self.group_id,
                p_key=f"{self.exchange_name.capitalize()}Ticker{self.exchange_name.upper()}",
            )
        )


# 예시 실행
exchange = KafkaConfigExchange.OKX.kafka_metadata_config()
print(exchange)
