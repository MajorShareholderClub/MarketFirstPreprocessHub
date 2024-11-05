from __future__ import annotations
from types import TracebackType

from typing import Any
import json
from decimal import Decimal
from aiokafka import AIOKafkaProducer
from src.common.admin.logging.logger import AsyncLogger
from mq.exception import handle_kafka_errors


def default(obj: Any) -> str:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class AsyncKafkaProducer:
    def __init__(self) -> None:
        self.logger = AsyncLogger(
            name="kafka", folder="kafka/handler", file="producer_handler"
        )
        self.producer: AIOKafkaProducer | None = None

    async def __aenter__(self) -> AsyncKafkaProducer:
        await self.init_producer()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """비동기 컨텍스트 매니저 종료"""
        await self.stop()

    @handle_kafka_errors
    async def init_producer(self) -> None:
        """프로듀서 초기화 및 시작"""
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers="kafka1:19092,kafka2:29092,kafka3:39092",
                key_serializer=lambda x: json.dumps(x).encode("utf-8"),
                value_serializer=lambda x: json.dumps(x, default=default).encode(
                    "utf-8"
                ),
                enable_idempotence=True,
                retry_backoff_ms=100,
                acks="all",
            )
            await self.producer.start()

    async def stop(self) -> None:
        """프로듀서 종료"""
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None
