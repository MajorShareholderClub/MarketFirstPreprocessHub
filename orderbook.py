import asyncio
import logging
from typing import Awaitable
from tenacity import retry, stop_after_attempt, wait_exponential

from mq.kafka_config import Region
from src.config import create_exchange_configs

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("orderbook_processor.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# 주문서 처리기 타입 정의
OrderbookProcessor = type[Awaitable[None]]


class RegionOrderbookProcessor:
    """주문서 처리 메인 클래스"""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda retry_state: logger.error(
            f"{retry_state.attempt_number}번째 시도 실패"
        ),
    )
    async def _create_task(self, config) -> None:
        processor = config["class_address"](**config["kafka_config"])
        await processor.initialize()
        try:
            await processor.batch_process_messages(target="orderbook")
        finally:
            await processor.cleanup()

    def _create_region_tasks(self, region: Region) -> list[Awaitable[None]]:
        """지역별 모든 태스크 생성"""
        tasks = []
        group_id = None
        ticker_config = create_exchange_configs(is_ticker=False)
        for exchange_config in ticker_config[region].values():
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
        processor = RegionOrderbookProcessor()
        await processor.process_ticker()
    except Exception as e:
        logger.critical(f"어플리케이션 실행 실패: {str(e)}")
        raise


asyncio.run(main())
