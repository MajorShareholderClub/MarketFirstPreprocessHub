from abc import abstractmethod
import json
import tracemalloc
import asyncio
from typing import Final
from collections import defaultdict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from mq.m_comsumer import AsyncKafkaHandler, logger
from mq.types import OrderBookData, ProcessedOrderBook
from mq.exception import (
    handle_kafka_errors,
    KafkaProcessingError,
    ErrorType,
)


class CommoneConsumerSettingProcesser(AsyncKafkaHandler):
    def __init__(
        self,
        consumer_topic: str,
        producer_topic: str,
        group_id: str,
        c_partition: int | None,
        p_partition: int | None,
        batch_size: int = 10,
        batch_timeout: float = 1.0,
    ) -> None:
        super().__init__(
            consumer_topic=consumer_topic,
            c_partition=c_partition,
            group_id=group_id,
        )
        tracemalloc.start()
        self.producer_topic: Final[str] = producer_topic
        self.batch_size: Final[int] = batch_size
        self.batch_timeout: Final[float] = batch_timeout
        self.p_partition: Final[int | None] = p_partition  # 파티션 저장

    @abstractmethod
    def calculate_total_bid_ask(self, orderbook: OrderBookData) -> ProcessedOrderBook:
        pass

    def orderbook_common_precessing(
        self, bid_data, ask_data, timestamp: str
    ) -> ProcessedOrderBook:
        totals: defaultdict[str, float] = defaultdict(float)
        highest_bid: float | None = None  # 최고 매수 가격
        lowest_ask: float | None = None  # 최저 매도 가격

        # 매수 및 매도 데이터 처리
        # fmt: off
        for price_str, volume_str in bid_data:
            price = float(price_str)
            totals["bid"] += float(volume_str)
            highest_bid = max(price, highest_bid) if highest_bid is not None else price

        for price_str, volume_str in ask_data:
            price = float(price_str)
            totals["ask"] += float(volume_str)
            lowest_ask = min(price, lowest_ask) if lowest_ask is not None else price

        spread: float | None = (
            lowest_ask - highest_bid
            if (highest_bid is not None and lowest_ask is not None)
            else None
        )
  
        return ProcessedOrderBook(
            highest_bid=highest_bid,
            lowest_ask=lowest_ask,
            spread=spread,
            total_bid_volume=totals["bid"],
            total_ask_volume=totals["ask"],
            timestamp=timestamp,
        )

    async def processing_message(
        self, consumer: AIOKafkaConsumer, producer: AIOKafkaProducer
    ) -> None:
        """메시지 프로토콜"""
        batch: list[OrderBookData] = []
        last_process_time = asyncio.get_event_loop().time()

        async for message in consumer:
            batch.append(message.value)

            current_time = asyncio.get_event_loop().time()
            should_process = (
                len(batch) >= self.batch_size
                or current_time - last_process_time >= self.batch_timeout
            )

            if should_process:
                processed_data_list: list[ProcessedOrderBook] = [
                    self.calculate_total_bid_ask(orderbook) for orderbook in batch
                ]

                # 모든 processed_data를 프로듀서에 전송
                for processed_data in processed_data_list:

                    await producer.send_and_wait(
                        self.producer_topic,
                        value=processed_data,
                        partition=self.p_partition,
                    )

                logger.info(f"{len(batch)} 메시지를 처리했습니다")
                batch.clear()
                last_process_time = current_time

    @handle_kafka_errors
    async def batch_process_messages(self) -> None:
        """메시지를 배치로 처리하여 성능 향상."""
        error_type = ErrorType.INITIALIZATION
        match (self.consumer, self.producer):
            case (None, _):
                raise KafkaProcessingError(error_type, "소비자가 초기화되지 않았습니다")
            case (_, None):
                raise KafkaProcessingError(error_type, "생산자가 초기화되지 않았습니다")
            case (consumer, producer):
                await self.processing_message(consumer=consumer, producer=producer)
