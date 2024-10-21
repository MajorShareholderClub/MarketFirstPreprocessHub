import asyncio
import logging
import yaml
from dataclasses import dataclass
from enum import Enum
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Awaitable

# 거래소 주문서 처리기 임포트
from src.asia.orderbook.okx_orderbook import okx_orderbook_cp
from src.asia.orderbook.bybit_orderbook import bybit_orderbook_cp
from src.asia.orderbook.gateio_orderbook import gateio_orderbook_cp
from src.korea.orderbook.upbithumb_orderbook import upbithumb_orderbook_cp
from src.korea.orderbook.onekorbit_orderbook import onekorbit_orderbook_cp
from src.ne.orderbook.binance_orderbook import binance_orderbook_cp
from src.ne.orderbook.kraken_orderbook import kraken_orderbook_cp

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("orderbook_processor.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# 주문서 처리기 타입 정의
OrderbookProcessor = type[Awaitable[None]]


class Region(str, Enum):
    """거래소 지역 구분"""

    KOREA = "Korea"
    ASIA = "Asia"
    NE = "NE"


@dataclass
class ExchangeConfig:
    """거래소 설정 데이터 클래스"""

    name: str
    c_partition: int
    p_partition: int
    processor: OrderbookProcessor


class OrderbookProcessorConfig:
    """주문서 처리기 설정 관리 클래스"""

    def __init__(self, config_path: str = "setting/config.yaml") -> None:
        self.config_path = config_path
        self._load_config()

    def _load_config(self) -> None:
        """설정 파일 로드"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"설정 파일 로드 실패: {e}")
            self.config = {}

    @property
    def producer_topic(self) -> str:
        """프로듀서 토픽 설정값 반환"""
        return self.config.get("producer_topic", "OrderbookPreprocessing")

    @property
    def group_id(self) -> str:
        """그룹 ID 설정값 반환"""
        return self.config.get("group_id", "orderbook")


class RegionOrderbookProcessor:
    """주문서 처리 메인 클래스"""

    # 거래소 설정
    EXCHANGE_CONFIGS: dict[Region, dict[str, ExchangeConfig]] = {
        Region.KOREA: {
            "upbit": ExchangeConfig("upbit", 1, 1, upbithumb_orderbook_cp),
            "bithumb": ExchangeConfig("bithumb", 3, 3, upbithumb_orderbook_cp),
            "coinone": ExchangeConfig("coinone", 5, 5, onekorbit_orderbook_cp),
            "korbit": ExchangeConfig("korbit", 7, 7, onekorbit_orderbook_cp),
        },
        Region.ASIA: {
            "okx": ExchangeConfig("okx", 1, 1, okx_orderbook_cp),
            "bybit": ExchangeConfig("bybit", 3, 3, bybit_orderbook_cp),
            "gateio": ExchangeConfig("gateio", 5, 5, gateio_orderbook_cp),
        },
        Region.NE: {
            "binance": ExchangeConfig("binance", 1, 1, binance_orderbook_cp),
            "kraken": ExchangeConfig("kraken", 3, 3, kraken_orderbook_cp),
        },
    }

    def __init__(self, symbol: str, config_path: str = "config.yaml") -> None:
        self.config = OrderbookProcessorConfig(config_path)
        self.producer_topic = self.config.producer_topic
        self.group_id = self.config.group_id
        self.symbol = symbol

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda retry_state: logger.error(
            f"{retry_state.attempt_number}번째 시도 실패"
        ),
    )
    async def _create_task(
        self, region: Region, config: ExchangeConfig
    ) -> Awaitable[None]:
        """단일 주문서 처리 태스크 생성 (재시도 로직 포함)"""
        try:
            return await config.processor(
                consumer_topic=f"{region.lower()}SocketDataIn{self.symbol.upper()}",
                c_partition=config.c_partition,
                p_partition=config.p_partition,
                p_key=f"{region.lower()}{config.name}:Orderbook{self.symbol.upper()}",
                producer_topic=f"{region}{self.producer_topic}{self.symbol.upper()}",
                group_id=f"{region}{self.group_id}",
            )
        except Exception as e:
            logger.error(f"{config.name} 처리기 에러: {str(e)}")
            raise

    def _create_region_tasks(self, region: Region) -> list[Awaitable[None]]:
        """지역별 모든 태스크 생성"""
        return [
            self._create_task(region, config)
            for config in self.EXCHANGE_CONFIGS[region].values()
        ]

    async def _handle_exceptions(self, results: list[Exception | None]) -> None:
        """태스크 실행 결과 예외 처리"""
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"처리 중 에러 발생: {str(result)}")

    async def process_orderbooks(self) -> None:
        """모든 지역의 주문서 동시 처리"""
        try:
            all_tasks = []
            for region in Region:
                logger.info(f"{region} 지역 태스크 생성 중")
                all_tasks.extend(self._create_region_tasks(region))

            logger.info(f"총 {len(all_tasks)}개 태스크 처리 시작")
            results = await asyncio.gather(*all_tasks, return_exceptions=True)

            logger.info("처리 완료, 예외 확인 중")
            await self._handle_exceptions(results)
        except Exception as e:
            logger.critical(f"주문서 처리 중 심각한 오류 발생: {str(e)}")
            raise


async def main() -> None:
    """메인 실행 함수"""
    try:
        processor = RegionOrderbookProcessor("BTC")
        await processor.process_orderbooks()
    except Exception as e:
        logger.critical(f"어플리케이션 실행 실패: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
