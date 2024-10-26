from enum import Enum
from dataclasses import dataclass, asdict
from typing import TypeVar, Generic

T = TypeVar("T", bound="BaseConfigDetails")


@dataclass(frozen=True)
class KafkaConfig:
    exchange: str
    p_partition: int
    producer_topic: str
    p_key: str


class Region(Enum):
    """거래소 지역 구분"""

    KOREA = "Korea"
    ASIA = "Asia"
    NE = "NE"


@dataclass(frozen=True)
class BaseConfigDetails:
    """기본 설정을 담는 데이터 클래스"""

    region: Region
    exchange_name: str
    p_partition: int

    def group_id(self, type_suffix: str) -> str:
        """Kafka consumer group id"""
        return f"{type_suffix}_group_id_{self.region.value}"

    def product_topic_name(self, type_suffix: str) -> str:
        """Kafka producer topic name"""
        return f"Region.{self.region.value}_{type_suffix}Preprocessing"


class TickerConfigDetails(BaseConfigDetails):
    """Ticker 전용 설정을 담는 데이터 클래스"""

    def get_consumer_topic(self) -> str:
        """Kafka consumer topic만 반환"""
        return f"{self.region.name.lower()}SocketDataInBTC-ticker"

    def kafka_metadata_config(self) -> KafkaConfig:
        """Kafka 설정 정보를 반환"""
        return asdict(
            KafkaConfig(
                exchange=self.exchange_name,
                p_partition=self.p_partition,
                producer_topic=self.product_topic_name("Ticker"),
                p_key=f"{self.exchange_name.capitalize()}Ticker",
            )
        )


class OrderbookConfigDetails(BaseConfigDetails):
    """Orderbook 전용 설정을 담는 데이터 클래스"""

    def get_consumer_topic(self) -> str:
        """Kafka consumer topic만 반환"""
        return f"{self.region.name.lower()}SocketDataInBTC-orderbook"

    def kafka_metadata_config(self) -> KafkaConfig:
        """Kafka 설정 정보를 반환"""
        return asdict(
            KafkaConfig(
                exchange=self.exchange_name,
                p_partition=self.p_partition,
                producer_topic=self.product_topic_name("Orderbook"),
                p_key=f"{self.exchange_name.capitalize()}Orderbook",
            )
        )


class ExchangeInfo:
    """거래소 정보를 관리하는 클래스"""

    EXCHANGE_CONFIGS = {
        # Korean exchanges
        "UPBIT": (Region.KOREA, "upbit", 0),
        "BITHUMB": (Region.KOREA, "bithumb", 1),
        "COINONE": (Region.KOREA, "coinone", 2),
        "KORBIT": (Region.KOREA, "korbit", 3),
        # Asian exchanges
        "OKX": (Region.ASIA, "okx", 0),
        "BYBIT": (Region.ASIA, "bybit", 1),
        "GATEIO": (Region.ASIA, "gateio", 2),
        # NE exchanges
        "BINANCE": (Region.NE, "binance", 0),
        "KRAKEN": (Region.NE, "kraken", 1),
    }

    @classmethod
    def create_config(cls, exchange_name: str, config_class: type[T]) -> T:
        """설정 객체 생성"""
        region, name, base_partition = cls.EXCHANGE_CONFIGS[exchange_name.upper()]
        return config_class(
            region=region,
            exchange_name=name,
            p_partition=base_partition,
        )


class ConfigExchangeBase(Generic[T]):
    """거래소 설정 기본 클래스"""

    config_class: type[T]

    @classmethod
    def get_config(cls, exchange_name: str) -> T:
        """특정 거래소의 설정을 반환"""
        return ExchangeInfo.create_config(exchange_name, cls.config_class)

    @classmethod
    def get_all_configs(cls) -> dict[str, T]:
        """모든 거래소의 설정을 반환"""
        return {
            name: cls.get_config(name) for name in ExchangeInfo.EXCHANGE_CONFIGS.keys()
        }


class TickerConfigExchange(ConfigExchangeBase[TickerConfigDetails]):
    """Ticker 전용 거래소 설정"""

    config_class = TickerConfigDetails


class OrderbookConfigExchange(ConfigExchangeBase[OrderbookConfigDetails]):
    """Orderbook 전용 거래소 설정"""

    config_class = OrderbookConfigDetails
