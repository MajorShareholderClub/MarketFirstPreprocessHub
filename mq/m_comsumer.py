from __future__ import annotations

import json
from random import randint

from decimal import Decimal
from typing import Final, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from src.common.admin.logging.logger import AsyncLogger
from type_model.kafka_model import KafkaConsumerConfig
from mq.exception import handle_kafka_errors
from mq.partition_manager import PartitionManager


def default(obj: Any) -> str:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class AsyncKafkaHandler:
    """비동기 Kafka 연결을 처리하는 기본 클래스."""

    def __init__(
        self,
        consumer_topic: str | None = None,
        group_id: str | None = None,
        bootstrap_servers: str = "kafka1:19092,kafka2:29092,kafka3:39092",
        c_partition: int | None = None,
    ) -> None:
        self.bootstrap_servers: Final[str] = bootstrap_servers
        self.consumer_topic: Final[str] = consumer_topic
        self.group_id: Final[str] = group_id if group_id else "default_group"
        self.logger = AsyncLogger(
            name="kafka", folder="kafka/handler", file="consumer_handler"
        )
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        self.c_partition: Final[int] = c_partition
        self.assigned_partition: int | None = None
        self.partition_manager: PartitionManager | None = None

    @handle_kafka_errors
    async def initialize(self) -> None:
        """Kafka 소비자 및 생산자 연결 초기화"""
        await self.logger.debug(
            f"""
            컨슈머 초기화:
            클래스: {self.__class__}
            토픽: {self.consumer_topic}
            파티션: {self.c_partition}
            그룹ID: {self.group_id}
            """,
        )

        if not self.consumer_topic:
            raise ValueError("consumer_topic이 설정되지 않았습니다.")

        group_id_split: list[str] = self.group_id.split("_")
        config = KafkaConsumerConfig(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            client_id=f"{group_id_split[-1]}-client-{group_id_split[0]}-{randint(1, 100)}",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        self.consumer = AIOKafkaConsumer(**config)
        await self.logger.debug(f"소비자가 초기화되었습니다: {self.consumer_topic}")

        if self.c_partition is not None:
            self.consumer.assign(
                [TopicPartition(self.consumer_topic, self.c_partition)]
            )
            self.assigned_partition = self.c_partition
            await self.logger.debug(
                f"파티션 {self.c_partition}이 수동으로 할당되었습니다."
            )

        await self.consumer.start()
        assigned_partitions = self.consumer.assignment()
        await self.logger.debug(f"실제 할당된 파티션: {assigned_partitions}")

        # PartitionManager 초기화 및 시작
        self.partition_manager = PartitionManager(
            consumer=self.consumer,
            topic=self.consumer_topic,
            assigned_partition=self.assigned_partition,
        )
        await self.partition_manager.start_monitoring()

        # Producer 초기화
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
        await self.logger.debug("생산자가 초기화되었습니다")

    async def close(self) -> None:
        """리소스 정리"""
        if self.partition_manager:
            await self.partition_manager.stop_monitoring()

        if self.consumer:
            await self.consumer.stop()

        if self.producer:
            await self.producer.stop()

        await self.logger.debug("Kafka 연결이 종료되었습니다.")
