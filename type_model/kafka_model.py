import msgspec
from typing import Callable
from aiokafka import AIOKafkaProducer


class DictonaryBuiltins(msgspec.Struct, dict=True, weakref=True):
    """딕셔너리로 변환"""

    def to_dict(self) -> dict:
        return {f: getattr(self, f) for f in self.__struct_fields__}


class KafkaConfigProducer(DictonaryBuiltins):
    """카프카 설정 값"""

    producer_topic: str
    p_partition: int
    p_key: str
    topic: str
    batch: list[dict]


class KafkaDeadLetterTopic(DictonaryBuiltins):
    """DLT 카프카 설정 값"""

    key: str | None
    original_topic: str
    original_partition: int
    error_message: str
    error_traceback: str
    timestamp: str
    message: dict | list


class KafkaConsumerConfig(DictonaryBuiltins):
    bootstrap_servers: str
    group_id: str
    client_id: str
    auto_offset_reset: str
    enable_auto_commit: bool
    value_deserializer: Callable


class KafkaMetadataConfig(DictonaryBuiltins):
    """Kafka 설정 정보"""

    consumer_topic: str
    p_partition: int
    c_partition: int
    group_id: str
    producer_topic: str
    p_key: str
    transaction_id: str | None = None


class BatchConfig(DictonaryBuiltins):
    """배치 처리를 위한 환경 설정 값"""

    size: int = 20
    timeout: float = 10.0
    max_memory_mb: int = 1000
    retry_count: int = 3
    retry_delay: float = 1.0
