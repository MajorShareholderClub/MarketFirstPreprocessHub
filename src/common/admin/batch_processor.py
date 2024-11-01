from __future__ import annotations
from types import TracebackType

import time
import psutil
import pympler.asizeof
from typing import TypeVar, Callable, Any

from type_model.kafka_model import KafkaConfigProducer
from src.common.admin.logging.logger import AsyncLogger
from src.common.admin.time_stracker import TimeStacker, StackConfig

from datetime import datetime

today = datetime.now().strftime("%Y%m%d")
T = TypeVar("T")

ProcessFunction = Callable[[T], Any]


class BatchProcessor:
    """배치 처리 클래스"""

    def __init__(self) -> None:
        """배치 처리기 초기화"""
        self.process = psutil.Process()
        self.logger = AsyncLogger(
            name="kafka", folder=f"kafka/batch", file=f"processing_{today}"
        )
        self.time_stacker = TimeStacker(flush_handler=self._handle_flush)
        self.current_kafka_config: KafkaConfigProducer | None = None

    async def __aenter__(self) -> BatchProcessor:
        """TimeStacker 시작"""
        await self.time_stacker.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        """TimeStacker 중지"""
        await self.time_stacker.stop()

    def _check_memory_usage(self) -> float:
        """메모리 사용량 확인"""
        return self.process.memory_info().rss / 1024 / 1024

    async def _batch_send_kafka(
        self, message: list[T], kafka_config: KafkaConfigProducer
    ) -> None:
        """카프카로 배치 데이터 전송"""
        await kafka_config.producer.send_and_wait(
            topic=kafka_config.producer_topic,
            key=kafka_config.p_key,
            value=message,
            partition=kafka_config.p_partition,
        )
        await kafka_config.producer.flush()

    async def _handle_flush(self, items: list[dict], config: StackConfig) -> None:
        """TimeStacker로부터 플러시된 데이터 처리"""
        if not self.current_kafka_config:
            raise RuntimeError("Kafka configuration not set")

        try:
            start_time = time.time()
            size_of_batch = pympler.asizeof.asizeof(items)

            if size_of_batch > self.current_kafka_config.producer._max_request_size:
                chunk_size = max(1, len(items) // 2)  # 최소 1개 보장
                for i in range(0, len(items), chunk_size):
                    chunk = items[i : i + chunk_size]
                    await self._batch_send_kafka(chunk, self.current_kafka_config)
                    del chunk  # 명시적 메모리 해제

            else:
                await self._batch_send_kafka(items, self.current_kafka_config)

            processing_time = time.time() - start_time
            memory_usage = self._check_memory_usage()

            if memory_usage > 200:  # 메모리 경고 추가
                await self.logger.warning(
                    f"높은 메모리 사용량 감지: {memory_usage:.2f}MB"
                )

            await self.logger.info(
                f"""
                배치 전송 완료:
                - 토픽: {config.topic}
                - 파티션: {config.partition}
                - 배치 크기: {len(items)}
                - 메모리 크기: {size_of_batch} Bytes
                - 처리 시간: {processing_time:.3f}초
                - 메모리 사용량: {memory_usage:.2f}MB
                """
            )

        except Exception as e:
            await self.logger.error(f"배치 플러시 처리 실패: {str(e)}")
            raise
        finally:
            del items  # 명시적 메모리 해제

    async def process_current_batch(
        self, process: ProcessFunction, kafka_config: KafkaConfigProducer
    ) -> None:
        """배치 데이터 처리 및 전송"""
        self.current_kafka_config = kafka_config

        # 배치 처리 설정
        stack_config = StackConfig(
            topic=kafka_config.producer_topic,
            partition=kafka_config.p_partition,
            max_size=10,
            timeout_ms=250,
        )

        try:
            for data in kafka_config.batch:
                processed_data = process(data)
                await self.time_stacker.add_item(processed_data, stack_config)

        except Exception as e:
            await self.logger.error(f"배치 처리 실패: {str(e)}")
            raise
