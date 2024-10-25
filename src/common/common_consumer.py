import asyncio
import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import Final, TypeVar, Callable, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from src.logger import AsyncLogger
from mq.m_comsumer import AsyncKafkaHandler
from mq.types import OrderBookData, ProcessedOrderBook
from mq.exception import (
    handle_kafka_errors,
    KafkaProcessingError,
    ErrorType,
)

T = TypeVar("T")
ProcessFunction = Callable[[T], Any]


@dataclass
class BatchConfig:
    """배치 처리 설정"""

    size: int
    timeout: float


class CommonConsumerSettingProcessor(AsyncKafkaHandler):
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
        super().__init__(
            consumer_topic=consumer_topic,
            group_id=group_id,
        )
        self.producer_topic: Final[str] = producer_topic
        self.p_partition: Final[int | None] = p_partition
        self.c_partition: Final[int | None] = c_partition
        self.p_key: Final[str | None] = p_key
        self.batch_config = batch_config or BatchConfig(size=20, timeout=10.0)
        self.logger = AsyncLogger(target="kafka", folder="topic").log_message
        self._process_map = {
            "orderbook": self.calculate_total_bid_ask,
            "ticker": self.data_task_a_crack_ticker,
        }

    @abstractmethod
    def calculate_total_bid_ask(self, orderbook: OrderBookData) -> ProcessedOrderBook:
        """오더북 데이터 처리"""
        pass

    @abstractmethod
    def data_task_a_crack_ticker(self, ticker: dict) -> dict:
        """티커 데이터 처리"""
        pass

    async def _send_batch_to_kafka(
        self, producer: AIOKafkaProducer, batch: list[Any]
    ) -> None:
        """배치 데이터 Kafka 전송"""
        if not batch:
            return

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
            f"{self.p_partition} 배치 처리 완료: {len(batch)}개 메시지 --> {self.producer_topic} 전송합니다",
        )

    async def processing_message(
        self,
        process: ProcessFunction[T],
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
    ) -> None:
        """메시지 배치 처리"""
        batch: list[T] = []
        last_process_time = asyncio.get_event_loop().time()

        async for message in consumer:
            if message.partition == self.c_partition:
                await self.logger(
                    logging.INFO,
                    f"{consumer._client._client_id} 는 --> {self.c_partition}를 소모합니다",
                )
                batch.append(message.value)
                current_time = asyncio.get_event_loop().time()

                if (
                    len(batch) >= self.batch_config.size
                    or current_time - last_process_time >= self.batch_config.timeout
                ):

                    # 데이터 처리
                    processed_batch = [process(data) for data in batch]

                    # Kafka로 전송
                    await self._send_batch_to_kafka(producer, processed_batch)

                    batch.clear()
                    last_process_time = current_time

    @handle_kafka_errors
    async def batch_process_messages(self, target: str) -> None:
        """배치 처리 시작점"""
        if not self.consumer or not self.producer:
            raise KafkaProcessingError(
                ErrorType.INITIALIZATION,
                "Kafka consumer 또는 producer가 초기화되지 않았습니다",
            )

        process_func = self._process_map.get(target)
        if not process_func:
            raise ValueError(f"알 수 없는 target 유형입니다: {target}")

        await self.processing_message(
            process=process_func, consumer=self.consumer, producer=self.producer
        )
