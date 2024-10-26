import logging
import redis
from dataclasses import dataclass
from typing import TypeVar, Callable, Any
from src.config import TickerClass, OrderbookClass

from aiokafka import AIOKafkaProducer
from src.logger import AsyncLogger

from mq.m_comsumer import AsyncKafkaHandler
from mq.exception import (
    handle_kafka_errors,
    KafkaProcessingError,
    ErrorType,
)

T = TypeVar("T")
ProcessFunction = Callable[[T], Any]
r = redis.Redis(host="localhost", port=6379, db=0)


@dataclass
class BatchConfig:
    """배치 처리 설정"""

    size: int
    timeout: float


class CommonConsumerSettingProcessor(AsyncKafkaHandler):
    def __init__(self, target: str, **kafka_meta_data) -> None:
        self.target = target
        self.kafka_setting = kafka_meta_data["kafka_meta_data"]
        self.batch_config = BatchConfig(size=20, timeout=10.0)
        self.logger = AsyncLogger(target="kafka", folder="topic").log_message

        # get 해서 가지고 오기
        self.consumer_topics = self.kafka_setting["consumer_topic"]
        super().__init__(
            consumer_topic=self.consumer_topics,
            group_id=f"{target.capitalize()}_preprocessing_group",
        )
        self.process_map = {
            "ticker": self._process_target_ticker,
            "orderbook": self._process_target_order,
        }

    async def _send_batch_to_kafka(
        self,
        producer: AIOKafkaProducer,
        message: dict,
        kafka_config: dict,
    ) -> None:
        """배치 데이터 Kafka 전송"""

        await producer.send_and_wait(
            topic=kafka_config["producer_topic"],
            value=message,
            partition=kafka_config["p_partition"],
            key=kafka_config["p_key"],
        )
        self.logger(logging.INFO, f"{kafka_config["producer_topic"]} 전송합니다")

    def _find_process(self, message: dict) -> dict:
        key = message.key
        decoded_key: str = key.decode() if isinstance(key, bytes) else key
        ex_keys: list[str] = decoded_key.split(":")
        exchange: str = ex_keys[0].strip('"').lower()
        data_type: str = ex_keys[1].strip('"').lower()

        c_type: list[dict] = self.kafka_setting.get(data_type, None)
        for data in c_type:
            if data["kafka_config"].get("exchange") == exchange:
                return data

    def _process_target_ticker(self, class_address: TickerClass, message: dict) -> dict:
        return class_address.data_task_a_crack_ticker(message)

    # fmt: off
    def _process_target_order(self, class_address: OrderbookClass, message: dict) -> dict:
        return class_address.calculate_total_bid_ask(message)

    async def processing_message(self, producer: AIOKafkaProducer) -> None:
        """메시지 배치 처리"""
        # last_process_time = asyncio.get_event_loop().time()

        async for message in self.consumer:
            process_method = self.process_map.get(self.target)
            meta = self._find_process(message=message)
            if meta is None:
                continue

            kafka_config = meta["kafka_config"]
            process_class = meta["class_address"]()
            process = process_method(class_address=process_class, message=message.value)
            await self._send_batch_to_kafka(producer, process, kafka_config)

            # current_time = asyncio.get_event_loop().time()
            # if (
            #     len(batch) >= self.batch_config.size
            #     or (current_time - last_process_time) >= self.batch_config.timeout
            # ):
            #     batch.clear()
            #     last_process_time = current_time

    @handle_kafka_errors
    async def batch_process_messages(self) -> None:
        """배치 처리 시작점"""

        if not self.consumer or not self.producer:
            raise KafkaProcessingError(
                ErrorType.INITIALIZATION,
                "Kafka consumer 또는 producer가 초기화되지 않았습니다",
            )

        await self.processing_message(producer=self.producer)
