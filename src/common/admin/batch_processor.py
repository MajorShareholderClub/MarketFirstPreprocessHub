import os
import time
import psutil
import logging
from typing import TypeVar, Callable, Any
from dataclasses import dataclass, field
from src.common.admin.logging.logger import AsyncLogger
from aiokafka import AIOKafkaProducer
from src.common.admin.metric_manager import MetricsManager

from datetime import datetime

today = datetime.now().strftime("%Y%m%d")
T = TypeVar("T")

# Any -> Ticker, Orderbook
ProcessFunction = Callable[[T], Any]

# fmt: off
@dataclass
class BatchConfig:
    """배치 처리를 위한 환경 설정 값"""
    size: int = field(default_factory=lambda: int(os.getenv('BATCH_SIZE', '20')))
    timeout: float = field(default_factory=lambda: float(os.getenv('BATCH_TIMEOUT', '10.0')))
    max_memory_mb: int = field(default_factory=lambda: int(os.getenv('MAX_MEMORY_MB', '1000')))
    retry_count: int = field(default_factory=lambda: int(os.getenv('RETRY_COUNT', '3')))
    retry_delay: float = field(default_factory=lambda: float(os.getenv('RETRY_DELAY', '1.0')))


@dataclass
class KafkaConfigProducer:
    """카프카 설정 값"""
    producer: AIOKafkaProducer
    producer_topic: str
    p_partition: int
    p_key: str
    topic: str
    batch: list[dict]
    
    
class BatchProcessor:
    """배치 처리 클래스"""
    
    def __init__(self, config: BatchConfig, metrics: MetricsManager) -> None:
        """배치 처리기 초기화"""
        self.config = config
        self.metrics = metrics
        self.logger = AsyncLogger(name="kafka", folder=f"kafka/batch", file=f"processing_{today}")
        self.process = psutil.Process()

    def _check_memory_usage(self) -> None:
        """메모리 사용량 확인"""
        return self.process.memory_info().rss / 1024 / 1024
    
    async def send_batch_to_kafka(self, kafka_config: KafkaConfigProducer) -> None:
        """카프카로 배치 데이터 전송 (실패시 재시도)"""
        kafka_batch = kafka_config.producer.create_batch()
        kafka_batch.append(
                key=kafka_config.p_key,
                value=kafka_config.batch,
                timestamp=None,
            )

        await kafka_config.producer.send_batch(
            kafka_batch, 
            kafka_config.producer_topic, 
            partition=kafka_config.p_partition
        )
        await kafka_config.producer.flush()
        
        await self.logger.info(
            f"""
            파티션 {kafka_config.p_partition} 배치 처리 완료: {len(kafka_config.batch)}개 메시지 
            --> {kafka_config.producer_topic} 전송
            """,
        )


    async def process_current_batch(self, process: ProcessFunction, kafka_config: KafkaConfigProducer) -> None:
        """배치 데이터 처리 및 전송"""
        start_time = time.time()
        try:
            tasks = [process(data) for data in kafka_config.batch]
            kafka_config.batch = tasks

            await self.send_batch_to_kafka(kafka_config)
            processing_time = time.time() - start_time
            await self.metrics.update_metrics(len(kafka_config.batch), processing_time)
            
        except (TypeError, ValueError) as e:
            await self.metrics.record_failure(len(kafka_config.batch))
            raise
        finally:
            # 성공/실패 여부와 관계없이 메트릭 로깅
            await self.metrics.log_metrics()
