from __future__ import annotations

import json
import logging
from typing import Final, TypedDict, Callable, Any

from decimal import Decimal
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from mq.exception import handle_kafka_errors

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def default(obj: Any):
    if isinstance(obj, Decimal):
        return str(obj)


class KafkaConsumerConfig(TypedDict):
    bootstrap_servers: str
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool = True
    value_deserializer: Callable[[Any], bytes]


class AsyncKafkaHandler:
    """비동기 Kafka 연결을 처리하는 기본 클래스."""

    def __init__(
        self,
        group_id: str,
        c_partition: int | None = None,
        bootstrap_servers: str = "kafka1:19092,kafka2:29092,kafka3:39092",
        consumer_topic: str | None = None,
    ) -> None:
        self.bootstrap_servers: Final[str] = bootstrap_servers
        self.consumer_topic: Final[str | None] = consumer_topic
        self.group_id: Final[str] = group_id
        self.c_partition: Final[int | None] = c_partition  # 파티션 저장

        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

    @handle_kafka_errors
    async def initialize(self) -> None:
        """Kafka 소비자 및 생산자 연결 초기화"""
        if self.consumer_topic:
            config = KafkaConsumerConfig(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            self.consumer = AIOKafkaConsumer(**config)
            await self.consumer.start()
            logger.info(f"소비자가 초기화되었습니다: {self.consumer_topic}")

            # 특정 파티션만 할당
            if self.c_partition is not None:
                partition = TopicPartition(self.consumer_topic, self.c_partition)
                self.consumer.assign([partition])  # 특정 파티션 할당
                logger.info(
                    f"{self.consumer_topic}의 파티션 {self.c_partition}을 소비합니다."
                )
            else:
                # partition 이 None일 경우 전체를 구독 예외 로직
                await self.consumer.subscribe([self.consumer_topic])  # 전체 토픽 구독
                await self.consumer.start()
                logger.info(f"소비자가 초기화되었습니다: {self.consumer_topic}")

            # 그룹 메타데이터 확인
            try:
                metadata = self.consumer._group_id
                logger.info(f"Consumer group metadata: {metadata}")
            except Exception as e:
                logger.error(f"Failed to get group metadata: {e}")

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
        logger.info("생산자가 초기화되었습니다")

    @handle_kafka_errors
    async def cleanup(self) -> None:
        """Kafka 연결 정리"""
        match (self.consumer, self.producer):
            case (consumer, producer) if consumer is not None and producer is not None:
                await consumer.stop()
                await producer.stop()
                logger.info("모든 Kafka 연결이 종료되었습니다")
            case (consumer, _) if consumer is not None:
                await consumer.stop()
                logger.info("소비자 연결이 종료되었습니다")
            case (_, producer) if producer is not None:
                await producer.stop()
                logger.info("생산자 연결이 종료되었습니다")
            case _:
                logger.info("종료할 연결이 없습니다")
