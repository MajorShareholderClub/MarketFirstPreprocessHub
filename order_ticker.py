import asyncio
import logging
from typing import Awaitable
from tenacity import retry, stop_after_attempt, wait_exponential

from mq.kafka_config import Region
from src.config import create_exchange_configs
from src.logger import AsyncLogger
from src.common.common_consumer import CommonConsumerSettingProcessor


class RegionTickerOrderbookProcessor(CommonConsumerSettingProcessor):
    """주문서 처리 메인 클래스"""

    def __init__(self, kafka_consumer_type: str, is_ticker: bool) -> None:
        self.kafka_consumer_type = kafka_consumer_type
        self.is_ticker = is_ticker
        # 로깅 설정
        self.logger = AsyncLogger(
            "init", folder=self.kafka_consumer_type
        ).log_message_sync
        self.process_map = {
            "orderbook": self.calculate_total_bid_ask,
            "ticker": self.data_task_a_crack_ticker,
        }

    # @retry(
    #     stop=stop_after_attempt(3),
    #     wait=wait_exponential(multiplier=1, min=4, max=10),
    #     retry_error_callback=lambda retry_state: logging.getLogger(__name__).error(
    #         f"{retry_state.attempt_number}번째 시도 실패"
    #     ),
    # )
    async def _create_task(self, config) -> None:
        process = config["class_address"](**config["kafka_config"])
        config[f"bound_{self.kafka_consumer_type}"] = self.process_map.get(
            self.kafka_consumer_type
        )
        await process.initialize()
        try:
            await process.batch_process_messages(
                target=self.kafka_consumer_type, config=config
            )
        finally:
            await process.cleanup()

    def _create_region_tasks(self, region: Region) -> list[Awaitable[None]]:
        """지역별 모든 태스크 생성"""
        tasks = []
        group_id = None
        ticker_config = create_exchange_configs(is_ticker=self.is_ticker)
        for exchange_config in ticker_config[region].values():
            task = self._create_task(exchange_config)
            tasks.append(task)

            # 로깅을 위해 그룹 ID 저장
            if group_id is None:
                group_id = exchange_config["kafka_config"]["group_id"]

        message = (
            f"Created tasks for region {region.name} - "
            f"Consumer Group: {group_id} - "
            f"Number of consumers: {len(tasks)}"
        )
        self.logger(logging.INFO, message)
        return tasks

    async def _handle_exceptions(self, results: list[Exception | None]) -> None:
        """태스크 실행 결과 예외 처리"""
        for result in results:
            if isinstance(result, Exception):
                self.logger(logging.ERROR, f"처리 중 에러 발생: {str(result)}")

    async def process_ticker(self) -> None:
        all_tasks = []
        for region in Region:
            self.logger(logging.INFO, f"{region} 지역 태스크 생성 중")
            all_tasks.extend(self._create_region_tasks(region))

        self.logger(logging.INFO, f"총 {len(all_tasks)}개 태스크 처리 시작")
        results = await asyncio.gather(*all_tasks, return_exceptions=False)
        await self._handle_exceptions(results)
