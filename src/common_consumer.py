import asyncio
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, TypeVar, Callable, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from mq.m_comsumer import AsyncKafkaHandler, logger
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

    def process_order_data(
        self, price_volume_data: list[tuple[str, str]]
    ) -> tuple[float, float]:
        """주문 데이터 처리"""
        if not price_volume_data:
            return 0.0, None

        total = 0.0
        prices = []

        for price_str, volume_str in price_volume_data:
            price = float(price_str)
            total += float(volume_str)
            prices.append(price)

        return total, max(prices) if prices else None

    def orderbook_common_processing(self, bid_data, ask_data) -> ProcessedOrderBook:
        """오더북 공통 처리"""
        bid_total, highest_bid = self.process_order_data(bid_data)
        ask_total, _ = self.process_order_data(ask_data)

        lowest_ask = min(float(price) for price, _ in ask_data) if ask_data else None
        spread = (
            (lowest_ask - highest_bid)
            if (highest_bid is not None and lowest_ask is not None)
            else None
        )

        return ProcessedOrderBook(
            highest_bid=highest_bid,
            lowest_ask=lowest_ask,
            spread=spread,
            total_bid_volume=bid_total,
            total_ask_volume=ask_total,
            timestamp=str(datetime.now(timezone.utc)),
        )

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
        logger.info(f"배치 처리 완료: {len(batch)}개 메시지")

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
                logger.info(
                    f"{consumer._client._client_id} 는 --> {self.c_partition}를 소모합니다"
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
