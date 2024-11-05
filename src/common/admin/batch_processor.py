from __future__ import annotations
from types import TracebackType

import time
import psutil
import pympler.asizeof

from datetime import datetime
from typing import TypeVar, Callable, Any

from mq.m_producer import AsyncKafkaProducer
from mq.dlt_producer import DLTProducer
from type_model.kafka_model import KafkaConfigProducer
from src.common.admin.logging.logger import AsyncLogger
from src.common.admin.time_stracker import TimeStacker, StackConfig


T = TypeVar("T")
ProcessFunction = Callable[[T], Any]
today: str = datetime.now().strftime("%Y%m%d")


class BatchProcessor:
    """배치 처리 클래스"""

    # fmt: off
    def __init__(self) -> None:
        """배치 처리기 초기화"""
        self.process = psutil.Process()
        self.logger = AsyncLogger(
            name="kafka", folder=f"kafka/batch", file=f"processing_{today}"
        )
        self.time_stacker = TimeStacker(flush_handler=self._handle_flush, dlt_producer=DLTProducer())
        self.producer = AsyncKafkaProducer()

        
    def _check_memory_usage(self) -> float:
        """메모리 사용량 확인"""
        return self.process.memory_info().rss / 1024 / 1024

    async def __aenter__(self) -> BatchProcessor:
        """TimeStacker와 Producer 시작"""
        await self.time_stacker.start()
        await self.producer.init_producer()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        """TimeStacker와 Producer 중지"""
        await self.time_stacker.stop()
        if self.producer and self.producer.producer:
            await self.producer.stop()
        
    async def _batch_send_kafka(
        self, message: list[dict], kafka_config: KafkaConfigProducer
    ) -> None:
        """카프카로 배치 데이터 전송"""
        await self.producer.producer.send_and_wait(
            topic=kafka_config["producer_topic"],
            key=kafka_config["p_key"],
            value=message,
            partition=kafka_config["p_partition"],
        )

    async def _handle_flush(self, items: list[dict], config: StackConfig) -> None:
        """TimeStacker로부터 플러시된 데이터 처리"""
        try:
            start_time: float = time.time()
            size_of_batch: int = pympler.asizeof.asizeof(items)
            
            if len(items) >= 10:
                chunk_size: int = max(1, len(items) // 2, 1)
                for i in range(0, len(items), chunk_size):
                    chunk: list[dict] = items[i : i + chunk_size]
                    await self._batch_send_kafka(chunk, self.current_kafka_config)
                    del chunk
            else:
                await self._batch_send_kafka(items, self.current_kafka_config)


            processing_time: float = time.time() - start_time
            memory_usage: float = self._check_memory_usage()

            await self.logger.info(
                f"""
                배치 전송 완료:
                - 토픽: {config.topic}
                - 파티션: {config.partition}
                - 배치 크기: {len(items)}
                - 내용물 : {items}
                - 배치 용량: {size_of_batch} Bytes
                - 처리 시간: {processing_time:.3f}초
                - 메모리 사용량: {memory_usage:.2f}MB
                """
            )
        finally:
            items.clear()

    async def process_current_batch(
        self, process: ProcessFunction, kafka_config: KafkaConfigProducer
    ) -> None:
        """배치 데이터 처리 및 전송"""
        self.current_kafka_config = kafka_config

        stack_config = StackConfig(
            topic=kafka_config["producer_topic"],
            partition=kafka_config["p_partition"]
        )

        for data in kafka_config["batch"]:
            processed_data = process(data)
            await self.time_stacker.add_item(processed_data, stack_config)
