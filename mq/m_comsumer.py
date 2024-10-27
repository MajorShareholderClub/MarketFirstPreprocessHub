from __future__ import annotations

import asyncio
from random import randint
import json
import logging
from src.logger import AsyncLogger
from typing import Final, TypedDict, Callable, Any

from decimal import Decimal
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from kafka.errors import KafkaError

from mq.exception import handle_kafka_errors


def default(obj: Any):
    if isinstance(obj, Decimal):
        return str(obj)


class KafkaConsumerConfig(TypedDict):
    bootstrap_servers: str
    group_id: str
    client_id: str
    auto_offset_reset: str
    enable_auto_commit: bool
    value_deserializer: Callable[[Any], Any]


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
        self.consumer_topic: Final[str | None] = consumer_topic
        self.group_id: Final[str] = group_id
        self.logger = AsyncLogger(
            target="kafka", folder="kafka_handler"
        ).log_message_sync
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        self.c_partition: Final[int | None] = c_partition
        self.assigned_partition: int | None = None

    @handle_kafka_errors
    async def initialize(self) -> None:
        """Kafka 소비자 및 생산자 연결 초기화"""
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
        await self.logger(logging.INFO, f"소비자가 초기화되었습니다: {self.consumer_topic}")
        
        if self.c_partition is not None:
            self.consumer.assign([TopicPartition(self.consumer_topic, self.c_partition)])
            self.assigned_partition = self.c_partition
            await self.logger(logging.INFO, f"파티션 {self.c_partition}이 수동으로 할당되었습니다.")
        else:
            await self.consumer.subscribe(*[self.consumer_topic])
            await self.logger(logging.INFO, "전체 토픽을 구독했습니다.")

        await self.consumer.start()

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

    async def manual_partition_management(self) -> None:
        """수동 파티션 관리 및 리밸런싱 대응"""
        check_interval = 60  # 1분마다 확인
        max_retries = 5  # 최대 재시도 횟수
        retry_count = 0

        while True:
            try:
                # 파티션 할당 확인
                assignment = await self.consumer.getmany(timeout_ms=1000)
                if not assignment:
                    await self.logger(logging.WARNING, "할당된 파티션이 없습니다. 재할당을 시도합니다.")
                    if self.c_partition is not None:
                        self.consumer.assign([TopicPartition(self.consumer_topic, self.c_partition)])
                        self.assigned_partition = self.c_partition
                        await self.logger(logging.INFO, f"파티션 {self.c_partition}이 재할당되었습니다.")
                    else:
                        await self.consumer.subscribe(*[self.consumer_topic])
                        await self.logger(logging.INFO, "전체 토픽을 다시 구독했습니다.")
                
                retry_count += 1
                if retry_count >= max_retries:
                    await self.logger(logging.ERROR, f"최대 재시도 횟수({max_retries})를 초과했습니다.")
                    break
                else:
                    retry_count = 0  # 성공 시 재시도 카운트 초기화

                # 메시지 처리
                for tp, messages in assignment.items():
                    for message in messages:
                        # 메시지 처리 로직
                        await self.process_message(message)
                
                # 수동으로 오프셋 커밋
                await self.consumer.commit()
            
            except KafkaError as e:
                await self.logger(logging.ERROR, f"Kafka 오류 발생: {e}")
            
            await asyncio.sleep(check_interval)

    async def process_message(self, message):
        # 이 메서드는 CommonConsumerSettingProcessor에서 오버라이드됩니다.
        pass

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