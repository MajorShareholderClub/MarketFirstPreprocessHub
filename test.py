import asyncio
import logging
from typing import Awaitable
from enum import Enum
from dataclasses import dataclass, asdict
from tenacity import retry, stop_after_attempt, wait_exponential

from setting.yml_load import TickerProcessorConfig
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

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("orderbook_processor.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


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
        self,
        region: Region,
        exchange_name: str,
        c_partition: int,
        p_partition: int,
    ) -> None:
        """각 거래소의 지역, 이름, partition 설정"""
        self.region = region
        self.exchange_name = exchange_name
        self.c_partition = c_partition
        self.p_partition = p_partition
        self.config = TickerProcessorConfig(self.exchange_name)

    @property
    def consumer_topic_name(self) -> str:
        pass

    @property
    def group_id(self) -> str:
        """Kafka consumer group id"""
        group_id = self.config.group_id
        return f"{group_id}_ticker_group_id"

    @property
    def product_topic_name(self) -> str:
        """Kafka producer topic name, 지역에 따라 다르게 설정"""
        product = self.config.producer_topic
        return f"Region.{self.region.value}_{product}"

    def kafka_metadata_config(self) -> KafkaConfig:
        """Kafka configuration for a specific exchange"""
        return asdict(
            KafkaConfig(
                consumer_topic=f"{self.region.name.lower()}SocketDataInBTC",
                c_partition=self.c_partition,
                p_partition=self.p_partition,
                producer_topic=self.product_topic_name,
                group_id=self.group_id,
                p_key=f"{self.exchange_name.capitalize()}Ticker{self.exchange_name.upper()}",
            )
        )


class RegionTickerProcessor:
    """주문서 처리 메인 클래스"""

    EXCHANGE_CONFIGS: dict[Region, dict[str, dict[str, KafkaConfig | TickerClass]]] = {
        Region.KOREA: {
            "upbit": {
                "kafka_config": KafkaConfigExchange.UPBIT.kafka_metadata_config(),
                "class_address": UpbithumbAsyncTickerProcessor,
            },
            "bithumb": {
                "kafka_config": KafkaConfigExchange.BITHUMB.kafka_metadata_config(),
                "class_address": UpbithumbAsyncTickerProcessor,
            },
            "korbit": {
                "kafka_config": KafkaConfigExchange.KORBIT.kafka_metadata_config(),
                "class_address": KorbitAsyncTickerProcessor,
            },
            "coinone": {
                "kafka_config": KafkaConfigExchange.COINONE.kafka_metadata_config(),
                "class_address": CoinoneAsyncTickerProcessor,
            },
        },
        Region.ASIA: {
            "okx": {
                "kafka_config": KafkaConfigExchange.OKX.kafka_metadata_config(),
                "class_address": OKXAsyncTickerProcessor,
            },
            "gateio": {
                "kafka_config": KafkaConfigExchange.GATEIO.kafka_metadata_config(),
                "class_address": GateIoAsyncTickerProcessor,
            },
            "bybit": {
                "kafka_config": KafkaConfigExchange.BYBIT.kafka_metadata_config(),
                "class_address": BybitAsyncTickerProcessor,
            },
        },
        Region.NE: {
            "binance": {
                "kafka_config": KafkaConfigExchange.BINANCE.kafka_metadata_config(),
                "class_address": BinanceAsyncTickerProcessor,
            },
            "kraken": {
                "kafka_config": KafkaConfigExchange.KRAKEN.kafka_metadata_config(),
                "class_address": KrakenAsyncTickerProcessor,
            },
        },
    }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda retry_state: logger.error(
            f"{retry_state.attempt_number}번째 시도 실패"
        ),
    )
    async def _create_task(self, config: dict[str, KafkaConfig, TickerClass]) -> None:
        processor = config["class_address"](**config["kafka_config"])
        await processor.initialize()
        try:
            await processor.batch_process_messages(target="ticker")
        finally:
            await processor.cleanup()

    def _create_region_tasks(self, region: Region) -> list[Awaitable[None]]:
        """지역별 모든 태스크 생성"""
        return [
            self._create_task(config)
            for config in self.EXCHANGE_CONFIGS[region].values()
        ]

    async def _handle_exceptions(self, results: list[Exception | None]) -> None:
        """태스크 실행 결과 예외 처리"""
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"처리 중 에러 발생: {str(result)}")

    async def process_ticker(self) -> None:
        all_tasks = []
        for region in Region:
            logger.info(f"{region} 지역 태스크 생성 중")
            all_tasks.extend(self._create_region_tasks(region))

        logger.info(f"총 {len(all_tasks)}개 태스크 처리 시작")
        results = await asyncio.gather(*all_tasks, return_exceptions=False)
        await self._handle_exceptions(results)


async def main() -> None:
    """메인 실행 함수"""
    try:
        processor = RegionTickerProcessor()
        await processor.process_ticker()
    except Exception as e:
        logger.critical(f"어플리케이션 실행 실패: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
