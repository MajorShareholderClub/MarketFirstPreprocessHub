import asyncio
import logging
from typing import Awaitable

from mq.kafka_config import Region
from src.common.admin.logging.logger import AsyncLogger
from src.config import create_exchange_configs
from src.common.admin.logging.logging_text import task_init_logging


class RegionTickerOrderbookProcessor:
    """주문서 처리 메인 클래스"""

    def __init__(self, kafka_consumer_type: str, is_ticker: bool) -> None:
        self.kafka_consumer_type = kafka_consumer_type
        self.is_ticker = is_ticker
        # 로깅 설정
        self.logger = AsyncLogger(name="init", folder="init", file="task-init")
        self.configs = create_exchange_configs(is_ticker=self.is_ticker)

    async def _create_task(self, config: dict) -> None:
        await self.logger.debug(
            f"""
            태스크 생성 정보:
            처리 클래스: {config['class_address'].__name__}
            카프카 설정: {config['kafka_config']}
            """,
        )
        process = config["class_address"](**config["kafka_config"])
        await process.initialize()

        # 초기화 후 상태 확인
        await self.logger.debug(task_init_logging(process))

        try:
            await process.start_processing_with_partition_management(
                target=self.kafka_consumer_type
            )
        finally:
            await process.close()

    def _create_region_tasks(self, region: Region) -> list[Awaitable[None]]:
        """지역별 모든 태스크 생성"""
        tasks = [
            self._create_task(exchange_config)
            for exchange_config in self.configs[region].values()
        ]
        self.logger.logger.log(
            logging.INFO,
            f"""
            Created tasks for region {region.name} -
            Number of consumers: {len(tasks)}
            """,
        )
        return tasks

    async def _handle_exceptions(self, results: list[Exception | None]) -> None:
        """태스크 실행 결과 예외 처리"""
        for result in results:
            if isinstance(result, Exception):
                await self.logger.log_error(f"처리 중 에러 발생:x {str(result)}")

    async def process_ticker(self) -> None:
        all_tasks = []
        for region in Region:
            await self.logger.info(f"{region} 지역 태스크 생성 중")
            all_tasks.extend(self._create_region_tasks(region))

        await self.logger.info(f"총 {len(all_tasks)}개 태스크 처리 시작")
        results = await asyncio.gather(*all_tasks, return_exceptions=False)
        await self._handle_exceptions(results)
