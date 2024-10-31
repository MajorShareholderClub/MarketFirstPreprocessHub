import asyncio
from typing import Optional

from aiokafka import AIOKafkaConsumer, TopicPartition
from src.common.admin.logging.logger import AsyncLogger


class PartitionManager:
    """카프카 파티션 모니터링을 위한 클래스"""

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        topic: str,
        assigned_partition: int | None,
    ) -> None:
        self.consumer = consumer
        self.topic = topic
        self.assigned_partition = assigned_partition
        self.logger = AsyncLogger(
            name="kafka", folder="kafka/partition", file="parttion_manager"
        )
        self._monitor_task: Optional[asyncio.Task] = None

    async def start_monitoring(self) -> None:
        """파티션 모니터링 시작"""
        # 모니터링 태스크 시작
        self._monitor_task = asyncio.create_task(self._monitor_partitions())
        await self.logger.debug(
            f"파티션 {self.assigned_partition}에 대한 모니터링이 시작되었습니다.",
        )

    async def stop_monitoring(self) -> None:
        """파티션 모니터링 중지"""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            await self.logger.debug("파티션 모니터링이 중지되었습니다.")

    async def _monitor_partitions(self) -> None:
        """파티션 할당 상태 모니터링"""
        while True:
            current_assignment = self.consumer.assignment()
            expected_partition = TopicPartition(self.topic, self.assigned_partition)

            # 현재 할당된 파티션이 예상과 다른 경우 경고
            if expected_partition not in current_assignment:
                await self.logger.warning(
                    f"""
                    파티션 할당 불일치 감지:
                    예상 파티션: {self.assigned_partition}
                    현재 할당: {[p.partition for p in current_assignment]}
                    토픽: {self.topic}
                    컨슈머 ID: {self.consumer._client._client_id}
                    """,
                )
            else:
                await self.logger.debug(
                    f"""
                    파티션 상태 정상:
                    할당된 파티션: {self.assigned_partition}
                    토픽: {self.topic}
                    컨슈머 ID: {self.consumer._client._client_id}
                    """,
                )

            await asyncio.sleep(60)  # 1분마다 체크
