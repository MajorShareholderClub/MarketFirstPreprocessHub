from dataclasses import dataclass, field

from src.common.admin.logging.logger import AsyncLogger
from src.common.admin.logging.logging_text import metrics_logging
from datetime import datetime

today = datetime.now().strftime("%Y%m%d")


@dataclass
class ProcessingMetrics:
    """메시지 처리 현황 추적을 위한 메트릭스"""

    total_messages: int = 0
    total_batches: int = 0
    total_processing_time: float = 0
    failed_messages: int = 0
    avg_batch_size: float = 0
    avg_processing_time: float = 0


@dataclass
class MetricsManager:
    """메트릭스 관리 클래스"""

    metrics: ProcessingMetrics = field(default_factory=ProcessingMetrics)
    logger: AsyncLogger = field(
        default_factory=lambda: AsyncLogger(
            name="kafka", folder="kafka/metric", file=f"metric_manager_{today}"
        )
    )

    async def update_metrics(self, batch_size: int, processing_time: float) -> None:
        """메트릭스 업데이트"""
        self.metrics.total_messages += batch_size
        self.metrics.total_batches += 1
        self.metrics.total_processing_time += processing_time
        self.metrics.avg_batch_size = (
            self.metrics.total_messages / self.metrics.total_batches
        )
        self.metrics.avg_processing_time = (
            self.metrics.total_processing_time / self.metrics.total_batches
        )

    async def record_failure(self, batch_size: int) -> None:
        """실패한 메시지 수 업데이트"""
        self.metrics.failed_messages += batch_size

    async def log_metrics(self) -> None:
        """매트릭스 로깅"""
        metrics_message: str = metrics_logging(self.metrics)
        await self.logger.debug(metrics_message)
