from enum import Enum
from dataclasses import dataclass
from typing import TypedDict


class Region(Enum):
    """거래소 지역 구분"""

    KOREA = "Korea"
    ASIA = "Asia"
    NE = "NE"


RegionTask = tuple[Region, str, int]


class ExchangeConfig(TypedDict):
    UPBIT: RegionTask
    BITHUMB: RegionTask
    COINONE: RegionTask
    KORBIT: RegionTask
    OKX: RegionTask
    BYBIT: RegionTask
    GATEIO: RegionTask
    BINANCE: RegionTask
    KRAKEN: RegionTask


@dataclass(frozen=True)
class KafkaConfig:
    consumer_topic: str
    p_partition: int
    c_partition: int
    group_id: str
    producer_topic: str
    p_key: str


@dataclass(frozen=True)
class BaseConfigDetails:
    """기본 설정을 담는 데이터 클래스"""

    region: Region
    exchange_name: str
    c_partition: int
    p_partition: int

    def group_id(self, type_suffix: str) -> str:
        """Kafka consumer group id"""
        return f"{type_suffix}_group_id_{self.region.value}"

    def product_topic_name(self, type_suffix: str) -> str:
        """Kafka producer topic name"""
        return f"Region{self.region.value}_{type_suffix}Preprocessing"
