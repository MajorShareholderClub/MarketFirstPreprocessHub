from __future__ import annotations
from types import TracebackType
from datetime import datetime

from type_model.kafka_model import KafkaDeadLetterTopic
from aiokafka import AIOKafkaProducer
import json


class DLTProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
            retries=3,
            linger_ms=100,
        )
        self._is_running = False

    async def __aenter__(self) -> DLTProducer:
        """DLTProducer 시작"""
        await self.producer.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        """DLTProducer 중지"""
        await self.producer.stop()

    def get_dlt_topic(self, original_topic: str) -> str:
        """원본 토픽에 대한 DLT 토픽 이름 생성"""
        return f"{original_topic}-DLT-Meaage"

    async def send_to_dlt(
        self,
        original_topic: str,
        original_partition: int,
        error_message: str,
        error_traceback: str,
        payload: dict | list,
    ) -> None:
        """실패한 메시지를 DLT로 전송"""
        dlt_message = KafkaDeadLetterTopic(
            key=None,
            original_topic=original_topic,
            original_partition=original_partition,
            error_message=error_message,
            error_traceback=error_traceback,
            timestamp=datetime.now().isoformat(),
            message=payload,
        ).to_dict()

        dlt_topic = self.get_dlt_topic(original_topic)

        try:
            await self.producer.send_and_wait(topic=dlt_topic, value=dlt_message)
        except Exception as e:
            # DLT 전송 실패 시 로깅만 하고 예외는 발생시키지 않음
            print(f"DLT 전송 실패: {str(e)}")
