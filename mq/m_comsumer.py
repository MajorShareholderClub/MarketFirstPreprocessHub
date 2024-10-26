from __future__ import annotations

from random import randint
import json
import logging
from src.logger import AsyncLogger
from typing import Final, TypedDict, Callable, Any

from decimal import Decimal
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from mq.exception import handle_kafka_errors


def default(obj: Any):
    if isinstance(obj, Decimal):
        return str(obj)


class KafkaConsumerConfig(TypedDict):
    bootstrap_servers: str
    group_id: str
    client_id: str
    auto_offset_reset: str
    enable_auto_commit: bool = True
    value_deserializer: Callable[[Any], Any]


class AsyncKafkaHandler:
    """비동기 Kafka 연결을 처리하는 기본 클래스."""

    def __init__(
        self,
        group_id: str,
        bootstrap_servers: str = "kafka1:19092,kafka2:29092,kafka3:39092",
        consumer_topic: str | None = None,
    ) -> None:
        self.bootstrap_servers: Final[str] = bootstrap_servers
        self.consumer_topic: Final[str | None] = consumer_topic
        self.group_id: Final[str] = group_id
        self.logger = AsyncLogger(
            target="kafka", folder="kafka_handler"
        ).log_message_sync
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

    @handle_kafka_errors
    async def initialize(self) -> None:
        """Kafka 소비자 및 생산자 연결 초기화"""
        group_id_split: list[str] = self.group_id.split("_")
        if self.consumer_topic:
            config = KafkaConsumerConfig(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                client_id=f"{group_id_split[-1]}-client-{group_id_split[0]}-{randint(1, 100)}",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            self.consumer = AIOKafkaConsumer(**config)
            await self.logger(
                logging.INFO, f"소비자가 초기화되었습니다: {self.consumer_topic}"
            )

            # partition 이 None일 경우 전체를 구독 예외 로직
            self.consumer.subscribe([self.consumer_topic])  # 전체 토픽 구독
            await self.consumer.start()

            # 그룹 메타데이터 확인
            try:
                metadata = self.consumer._group_id
                await self.logger(logging.INFO, f"Consumer group metadata: {metadata}")
            except Exception as e:
                await self.logger(logging.ERROR, f"Failed to get group metadata: {e}")

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda x: json.dumps(x).encode("utf-8"),
            value_serializer=lambda x: json.dumps(x, default=default).encode("utf-8"),
            max_batch_size=1000000,
            max_request_size=1000000,
            enable_idempotence=True,
            retry_backoff_ms=100,
            acks=-1,
        )
        await self.producer.start()
        await self.logger(logging.INFO, "생산자가 초기화되었습니다")

    @handle_kafka_errors
    async def cleanup(self) -> None:
        """Kafka 연결 정리"""
        match (self.consumer, self.producer):
            case (consumer, producer) if consumer is not None and producer is not None:
                await consumer.stop()
                await producer.stop()
                await self.logger(logging.INFO, "모든 Kafka 연결이 종료되었습니다")
            case (consumer, _) if consumer is not None:
                await consumer.stop()
                await self.logger(logging.INFO, "소비자 연결이 종료되었습니다")
            case (_, producer) if producer is not None:
                await producer.stop()
                await self.logger(logging.INFO, "생산자 연결이 종료되었습니다")
            case _:
                await self.logger(logging.INFO, "종료할 연결이 없습니다")
