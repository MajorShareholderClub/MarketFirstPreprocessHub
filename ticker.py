import asyncio
import logging
from typing import Awaitable
from tenacity import retry, stop_after_attempt, wait_exponential

from mq.kafka_config import KafkaConfig, KafkaConfigExchange, Region
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
        tasks = []
        group_id = None
        for exchange_config in self.EXCHANGE_CONFIGS[region].values():
            task = self._create_task(exchange_config)
            tasks.append(task)

            # 로깅을 위해 그룹 ID 저장
            if group_id is None:
                group_id = exchange_config["kafka_config"]["group_id"]

        logger.info(
            f"Created tasks for region {region.name} - "
            f"Consumer Group: {group_id} - "
            f"Number of consumers: {len(tasks)}"
        )
        return tasks

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
