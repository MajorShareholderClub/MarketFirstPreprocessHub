from aiokafka import AIOKafkaProducer
from dataclasses import dataclass, field
from typing import TypedDict, Callable, Any, Required
import os


@dataclass
class KafkaConfigProducer:
    """카프카 설정 값"""

    producer: AIOKafkaProducer
    producer_topic: str
    p_partition: int
    p_key: str
    topic: str
    batch: list[dict]


class KafkaConsumerConfig(TypedDict):
    bootstrap_servers: Required[str]
    group_id: Required[str]
    client_id: Required[str]
    auto_offset_reset: Required[str]
    enable_auto_commit: Required[bool]
    value_deserializer: Required[Callable[[Any], Any]]


@dataclass(frozen=True)
class KafkaConfig:
    consumer_topic: str
    p_partition: int
    c_partition: int
    group_id: str
    producer_topic: str
    p_key: str


@dataclass
class BatchConfig:
    """배치 처리를 위한 환경 설정 값"""

    size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "20")))
    timeout: float = field(
        default_factory=lambda: float(os.getenv("BATCH_TIMEOUT", "10.0"))
    )
    max_memory_mb: int = field(
        default_factory=lambda: int(os.getenv("MAX_MEMORY_MB", "1000"))
    )
    retry_count: int = field(default_factory=lambda: int(os.getenv("RETRY_COUNT", "3")))
    retry_delay: float = field(
        default_factory=lambda: float(os.getenv("RETRY_DELAY", "1.0"))
    )
