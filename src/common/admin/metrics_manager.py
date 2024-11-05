from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
from statistics import mean

from src.common.admin.logging.logger import AsyncLogger


@dataclass
class ProcessingMetrics:
    """메시지 처리 현황 추적을 위한 메트릭스"""

    # 토픽과 파티션별로 마지막 로깅 시간 저장
    _last_logged: dict[tuple[str, int], datetime] = field(default_factory=dict)
    # 토픽과 파티션별 메트릭 저장
    _total_messages: dict[tuple[str, int], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    _batch_sizes: dict[tuple[str, int], list] = field(
        default_factory=lambda: defaultdict(list)
    )
    _processing_times: dict[tuple[str, int], list] = field(
        default_factory=lambda: defaultdict(list)
    )
    _total_failures: dict[tuple[str, int], int] = field(
        default_factory=lambda: defaultdict(int)
    )

    def update(
        self,
        batch_size: int,
        processing_time: float,
        current_time: datetime,
        topic: str,
        partition: int,
    ) -> None:
        """메트릭 업데이트"""
        key = (topic, partition)

        # 새로운 분이 시작되면 메트릭 초기화
        if key not in self._last_logged:
            self._last_logged[key] = current_time

        self._total_messages[key] += batch_size
        self._batch_sizes[key].append(batch_size)
        self._processing_times[key].append(processing_time)

    def should_log(self, current_time: datetime, topic: str, partition: int) -> bool:
        """로깅이 필요한지 확인"""
        key = (topic, partition)
        if key not in self._last_logged:
            return False

        time_diff = current_time - self._last_logged[key]
        return time_diff.total_seconds() >= 60

    def get_metrics_summary(self, topic: str, partition: int) -> dict:
        """특정 토픽/파티션의 메트릭 요약"""
        key = (topic, partition)
        if key not in self._total_messages:
            return {}

        total_messages = self._total_messages[key]
        if total_messages == 0:
            return {}

        time_diff = (datetime.now() - self._last_logged[key]).total_seconds()

        return {
            "throughput": total_messages / time_diff if time_diff > 0 else 0,
            "avg_batch_size": (
                mean(self._batch_sizes[key]) if self._batch_sizes[key] else 0
            ),
            "avg_processing_time": (
                mean(self._processing_times[key]) if self._processing_times[key] else 0
            ),
            "failure_rate": (
                self._total_failures[key] / total_messages if total_messages > 0 else 0
            ),
        }

    def reset_metrics(self, current_time: datetime, topic: str, partition: int) -> None:
        """메트릭 초기화"""
        key = (topic, partition)
        self._last_logged[key] = current_time
        self._total_messages[key] = 0
        self._batch_sizes[key] = []
        self._processing_times[key] = []
        self._total_failures[key] = 0

    def record_failure(self, batch_size: int, topic: str, partition: int) -> None:
        """실패 기록"""
        key = (topic, partition)
        self._total_failures[key] += batch_size


@dataclass
class MetricsManager:
    """메트릭스 관리 클래스"""

    metrics: ProcessingMetrics = field(default_factory=ProcessingMetrics)
    logger: AsyncLogger = field(
        default_factory=lambda: AsyncLogger(
            name="metrics",
            folder="kafka/metrics",
            file=f"metrics_{datetime.now():%Y%m%d}",
        )
    )

    async def update_metrics(
        self,
        batch_size: int,
        processing_time: float,
        topic: str,
        partition: int,
        is_success: bool = True,
    ) -> None:
        """메트릭스 업데이트"""
        current_time = datetime.now()
        self.metrics.update(
            batch_size=batch_size,
            processing_time=processing_time,
            current_time=current_time,
            topic=topic,
            partition=partition,
        )

        # 실패한 경우 실패 카운트 증가
        if not is_success:
            self.metrics.record_failure(batch_size, topic, partition)

        # 1분이 지났는지 확인
        if self.metrics.should_log(current_time, topic, partition):
            print(f"메트릭 로깅 시작 - 토픽: {topic}, 파티션: {partition}")
            await self.log_metrics(topic, partition)
            # 로깅 후 메트릭 초기화
            self.metrics.reset_metrics(current_time, topic, partition)

    async def log_metrics(self, topic: str, partition: int) -> None:
        """특정 토픽/파티션의 메트릭 로깅"""
        current_metrics = self.metrics.get_metrics_summary(topic, partition)
        if not current_metrics:
            return

        # 로깅을 위한 데이터 준비
        metrics_data = {
            "topic": topic,
            "partition": partition,
            "timestamp": datetime.now().isoformat(),
            "throughput": round(current_metrics["throughput"], 2),
            "avg_batch_size": round(current_metrics["avg_batch_size"], 1),
            "avg_processing_time": round(current_metrics["avg_processing_time"], 3),
            "failure_rate": round(current_metrics["failure_rate"] * 100, 2),
        }

        # 로깅을 위한 포맷팅
        metrics_message = {
            "title": f"Minute Metrics - Topic: {topic}, Partition: {partition}",
            "time": f"Time: {metrics_data['timestamp']}",
            "throughput": f"Throughput: {metrics_data['throughput']} msgs/sec",
            "avg_batch_size": f"Avg Batch Size: {metrics_data['avg_batch_size']}",
            "avg_processing_time": f"Avg Processing Time: {metrics_data['avg_processing_time']}s",
            "failure_rate": f"Failure Rate: {metrics_data['failure_rate']}%",
        }
        await self.logger.debug({"metrics": metrics_data, "message": metrics_message})
