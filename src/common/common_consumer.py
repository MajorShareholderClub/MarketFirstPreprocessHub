import traceback
import asyncio
from abc import abstractmethod
from typing import Final, TypeVar

from aiokafka.errors import KafkaError
from src.common.admin.logging.logger import AsyncLogger
from type_model.ticker_model import CoinMarketCollection
from type_model.orderbook_model import ProcessedOrderBook
from type_model.kafka_model import KafkaConfigProducer, BatchConfig
from mq.m_comsumer import AsyncKafkaHandler
from mq.exception import (
    handle_kafka_errors,
    KafkaProcessingError,
    ErrorType,
)
from src.common.admin.batch_processor import BatchProcessor
from src.common.admin.logging.logging_text import consuming_message, consuming_error

T = TypeVar("T")


# fmt: off
class CommonConsumerSettingProcessor(AsyncKafkaHandler):
    """카프카 컨슈머 설정 및 배치 처리를 위한 기본 클래스"""

    def __init__(
        self,
        consumer_topic: str,
        producer_topic: str,
        group_id: str,
        c_partition: int | None = None,
        p_partition: int | None = None,
        p_key: str | None = None,
    ) -> None:
        """카프카 컨슈머/프로듀서 초기화

        Args:
            consumer_topic: 구독할 토픽명
            producer_topic: 발행할 토픽명
            group_id: 컨슈머 그룹 ID
            c_partition: 구독할 파티션 번호 (선택)
            p_partition: 발행할 파티션 번호 (선택)
            p_key: 메시지 키 (선택)
            batch_config: 배치 처리 설정 (선택, 미지정시 기본값 사용)
        """
        super().__init__(
            consumer_topic=consumer_topic,
            c_partition=c_partition,
            group_id=group_id,
        )
        self.logger = AsyncLogger(
            name="kafka", folder="kafka/prepro", file="preprocessing"
        )
        self.producer_topic: Final[str] = producer_topic
        self.p_partition: Final[int | None] = p_partition
        self.c_partition: Final[int | None] = c_partition
        self.p_key: Final[str | None] = p_key
        self.batch_config = BatchConfig()

        self.batch_processor = BatchProcessor()
        self.process_mapping = {
            "orderbook": self.calculate_total_bid_ask,
            "ticker": self.data_task_a_crack_ticker,
        }

    @abstractmethod
    def data_task_a_crack_ticker(self, ticker: dict) -> CoinMarketCollection: ...
    @abstractmethod
    def calculate_total_bid_ask(self, item: dict) -> ProcessedOrderBook: ...

    async def processing_message(self, process_func) -> None:
        """메시지 배치 처리 메인 로직"""
        batch: list[T] = []
        last_process_time = asyncio.get_event_loop().time()
        try:

            async for message in self.consumer:
                # log message
                batch.append(message.value)

                # 배치 사이즈가 충분하거나 시간이 초과되었거나 메모리 사용량이 초과되었을 경우 배치 처리
                current_time = asyncio.get_event_loop().time()
                should_process = (
                    len(batch) >= self.batch_config.size
                    or current_time - last_process_time >= self.batch_config.timeout
                    or self.batch_processor._check_memory_usage()
                    > self.batch_config.max_memory_mb
                )

                if should_process:
                    consuming: str = consuming_message(
                        message=message,
                        c_partition=self.c_partition,
                        process_func=process_func,
                    )
                    await self.logger.debug(consuming)
                    async with self.batch_processor as batch_processor:
                        await batch_processor.process_current_batch(
                            process=process_func,
                            kafka_config=KafkaConfigProducer(
                                producer=self.producer,
                                producer_topic=self.producer_topic,
                                p_partition=self.p_partition,
                                p_key=self.p_key,
                                topic=self.consumer_topic,
                                batch=batch,
                            ),
                        )
                    batch.clear()
                    last_process_time = current_time

        except (TypeError, ValueError, Exception) as error:
            await self._handle_processing_error(error, process_func)

    async def _handle_processing_error(self, error: Exception, process_func) -> None:
        """에러 발생시 로깅 및 에러 토픽으로 전송"""
        # log error
        error_message: str = consuming_error(
            error=error,
            c_partition=self.c_partition,
            process_func=process_func,
            consumer=self.consumer,
        )
        await self.logger.error(error_message)

        # 에러 토픽으로 전송
        await self.producer.send_and_wait(
            topic=f"Processed_error",
            value=error_message,
            key=self.p_key,
        )

    @handle_kafka_errors
    async def start_processing_with_partition_management(self, target: str) -> None:
        """파티션 관리와 함께 배치 처리를 시작하는 통합 메서드"""
        if not self.consumer or not self.producer:
            raise KafkaProcessingError(
                ErrorType.INITIALIZATION,
                "Kafka consumer 또는 producer가 초기화되지 않았습니다",
            )

        process_function = self.process_mapping.get(target)
        if not process_function:
            raise KafkaProcessingError(
                ErrorType.INITIALIZATION,
                f"'{target}' 대상에 대한 처리 함수를 찾을 수 없습니다",
            )

        try:
            # 파티션 관리 시작
            await self.partition_manager.start_monitoring()

            # 메시지 처리 시작
            await self.processing_message(process_func=process_function)

        except KafkaError as e:
            await self.logger.error(f"Kafka 오류 발생: {str(e)}")
            raise
        except Exception as e:
            await self.logger.error(
                f"예상치 못한 오류 발생: {str(e)}\n{traceback.format_exc()}",
            )
            raise
        finally:
            # 파티션 모니터링 중지
            await self.partition_manager.stop_monitoring()
