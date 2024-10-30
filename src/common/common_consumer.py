import traceback
import asyncio
import logging
import time
import os
import psutil
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Final, TypeVar, Callable, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from src.logger import AsyncLogger
from src.data_format import CoinMarketCollection

from mq.m_comsumer import AsyncKafkaHandler
from mq.types import ProcessedOrderBook
from mq.exception import (
    handle_kafka_errors,
    KafkaProcessingError,
    ErrorType,
)

T = TypeVar("T")
ProcessFunction = Callable[[T], Any]


# fmt: off
@dataclass
class BatchConfig:
    """배치 처리를 위한 환경 설정 값"""
    size: int = field(default_factory=lambda: int(os.getenv('BATCH_SIZE', '20')))
    timeout: float = field(default_factory=lambda: float(os.getenv('BATCH_TIMEOUT', '10.0')))
    max_memory_mb: int = field(default_factory=lambda: int(os.getenv('MAX_MEMORY_MB', '1000')))
    retry_count: int = field(default_factory=lambda: int(os.getenv('RETRY_COUNT', '3')))
    retry_delay: float = field(default_factory=lambda: float(os.getenv('RETRY_DELAY', '1.0')))


@dataclass
class ProcessingMetrics:
    """메시지 처리 현황 추적을 위한 메트릭스"""
    total_messages: int = 0
    total_batches: int = 0
    total_processing_time: float = 0
    failed_messages: int = 0
    avg_batch_size: float = 0
    avg_processing_time: float = 0


class CommonConsumerSettingProcessor(AsyncKafkaHandler):
    """카프카 컨슈머 설정 및 배치 처리를 위한 기본 클래스"""
    
    def __init__(
        self,
        consumer_topic: str,
        producer_topic: str,
        group_id: str,
        c_partition: int | None = None,
        p_partition: int | None = None,
        p_key: str | None = None,
        batch_config: BatchConfig | None = None,
    ) -> None:
        """카프카 컨슈머/프로듀서 초기화
        
        Args:
            consumer_topic: 구독할 토픽명
            producer_topic: 발행할 토픽명
            group_id: 컨슈머 그룹 ID
            c_partition: 구독할 파티션 번호 (선택)
            p_partition: 발행할 파티션 번호 (선택)
            p_key: 메시지 키 (선택)
            batch_config: 배치 처리 설정 (선택, 미지정시 기본값 사용)
        """
        super().__init__(
            consumer_topic=consumer_topic,
            c_partition=c_partition,
            group_id=group_id,
        )
        self.producer_topic: Final[str] = producer_topic
        self.p_partition: Final[int | None] = p_partition
        self.c_partition: Final[int | None] = c_partition
        self.p_key: Final[str | None] = p_key
        self.batch_config = batch_config or BatchConfig()
        self.logger = AsyncLogger(target="kafka", folder="topic").log_message
        self.process_map = {
            "orderbook": self.calculate_total_bid_ask,
            "ticker": self.data_task_a_crack_ticker,
        }
        self.metrics = ProcessingMetrics()
        self.process = psutil.Process()

    @abstractmethod
    def data_task_a_crack_ticker(self, ticker: dict) -> CoinMarketCollection: ...
    @abstractmethod
    def calculate_total_bid_ask(self, item: dict) -> ProcessedOrderBook: ...


    def _check_memory_usage(self) -> float:
        """현재 프로세스의 메모리 사용량을 MB 단위로 반환"""
        return self.process.memory_info().rss / 1024 / 1024

    async def _update_metrics(self, batch_size: int, processing_time: float) -> None:
        """배치 처리 후 메트릭스 정보 업데이트"""
        self.metrics.total_messages += batch_size
        self.metrics.total_batches += 1
        self.metrics.total_processing_time += processing_time
        self.metrics.avg_batch_size = (
            self.metrics.total_messages / self.metrics.total_batches
        )
        self.metrics.avg_processing_time = (
            self.metrics.total_processing_time / self.metrics.total_batches
        )

        # 메트릭스 로깅
        if self.metrics.total_batches % 10 == 0:  # 10배치마다 로깅
            await self.logger(
                logging.INFO,
                f"""메트릭스 현황:
                총 처리 메시지: {self.metrics.total_messages}
                평균 배치 크기: {self.metrics.avg_batch_size:.2f}
                평균 처리 시간: {self.metrics.avg_processing_time:.2f}초
                실패 메시지 수: {self.metrics.failed_messages}""",
            )

    async def _send_batch_to_kafka(self, producer: AIOKafkaProducer, batch: list) -> None:
        """카프카로 배치 데이터 전송 (실패시 재시도)"""
        if not batch:
            return

        for attempt in range(self.batch_config.retry_count):
            try:
                send_tasks = [
                    producer.send_and_wait(
                        self.producer_topic,
                        value=data,
                        partition=self.p_partition,
                        key=self.p_key,
                    )
                    for data in batch
                ]
                await asyncio.gather(*send_tasks)
                await self.logger(
                    logging.INFO,
                    f"{self.p_partition} 배치 처리 완료: {len(batch)}개 메시지 --> {self.producer_topic} 전송",
                )
                return
            except Exception as e:
                if attempt == self.batch_config.retry_count - 1:
                    raise
                await asyncio.sleep(self.batch_config.retry_delay * (attempt + 1))
                await self.logger(
                    logging.WARNING,
                    f"배치 전송 재시도 {attempt + 1}/{self.batch_config.retry_count}: {str(e)}",
                )

    async def processing_message(
        self,
        process: ProcessFunction,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
    ) -> None:
        """메시지 배치 처리 메인 로직"""
        batch: list[T] = []
        last_process_time = asyncio.get_event_loop().time()

        try:
            async for message in consumer:
                # 메모리 사용량 체크
                if self._check_memory_usage() > self.batch_config.max_memory_mb:
                    await self.logger(
                        logging.WARNING,
                        f"메모리 사용량 초과 ({self._check_memory_usage():.2f}MB). 배치 강제 처리",
                    )
                    if batch:
                        await self._process_current_batch(batch, process, producer)
                        batch = []
                        last_process_time = asyncio.get_event_loop().time()

                batch.append(message.value)
                current_time = asyncio.get_event_loop().time()

                if (
                    len(batch) >= self.batch_config.size
                    or current_time - last_process_time >= self.batch_config.timeout
                ):
                    await self._process_current_batch(batch, process, producer)
                    batch = []
                    last_process_time = current_time

        except Exception as error:
            self.metrics.failed_messages += len(batch)
            await self._handle_processing_error(error, message, process)

    async def _process_current_batch(
        self, batch: list[T], process: ProcessFunction, producer: AIOKafkaProducer
    ) -> None:
        """배치 데이터 처리 및 전송"""
        start_time = time.time()
        try:
            
            processed_batch = [process(data) for data in batch]
            await self._send_batch_to_kafka(producer, processed_batch)
            processing_time = time.time() - start_time
            await self._update_metrics(len(batch), processing_time)
        except Exception as e:
            await self._handle_processing_error(e, batch[-1], process)
            raise

    async def _handle_processing_error(
        self, error: Exception, message: dict, process: ProcessFunction
    ) -> None:
        """에러 발생시 로깅 및 에러 토픽으로 전송"""
        error_message = f"""
            key --> {message.keys}                     
            partition --> {message.partition}
            consumer_id --> {self.consumer._client._client_id}
            partition --> {self.c_partition}
            method --> {process}
            error_trace --> {traceback.format_exc()}
            error_message --> {str(error)}
        """
        await self.logger(logging.ERROR, error_message)

        # 에러 토픽으로 전송
        error_data = {
            "error_message": str(error),
            "error_trace": traceback.format_exc(),
            "failed_message": error_message,
        }
        await self.producer.send_and_wait(
            topic=f"Processed_error",
            value=error_data,
            key=self.p_key,
        )

    @handle_kafka_errors
    async def batch_process_messages(self, target: str) -> None:
        """지정된 타겟 유형에 따른 배치 처리 시작"""
        if not self.consumer or not self.producer:
            KafkaProcessingError(
                ErrorType.INITIALIZATION,
                "Kafka consumer 또는 producer가 초기화되지 않았습니다",
            )

        process_func = self.process_map.get(target)
        if not process_func:
            raise ValueError(f"알 수 없는 target 유형입니다: {target}")

        await self.processing_message(
            process=process_func, consumer=self.consumer, producer=self.producer
        )
