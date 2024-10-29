from __future__ import annotations

import functools
import json
import logging
from enum import Enum, auto
from typing import Callable, ParamSpec, TypeVar, Awaitable, Optional
from dataclasses import dataclass
from src.logger import AsyncLogger
import time

logger = AsyncLogger(target="Error", folder="error").log_message

# 타입 정의
P = ParamSpec("P")
R = TypeVar("R")


# 사용자 정의 예외
class KafkaProcessingError(Exception):
    def __init__(
        self,
        error_type: ErrorType,
        detail: str,
        severity: ErrorSeverity,
        context: ErrorContext,
    ) -> None:
        self.error_type = error_type
        self.detail = detail
        self.severity = severity
        self.context = context
        super().__init__(f"{error_type.name}: {detail}")


class ErrorType(Enum):
    KAFKA_CONNECTION = auto()
    PROCESSING = auto()
    INITIALIZATION = auto()
    CLEANUP = auto()


class ErrorSeverity(Enum):
    """에러 심각도"""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class ErrorContext:
    """에러 컨텍스트"""

    exchange: str
    operation: str
    timestamp: float
    details: Optional[dict] = None


class BaseProcessingError(Exception):
    """기본 처리 에러"""

    def __init__(
        self, message: str, severity: ErrorSeverity, context: ErrorContext
    ) -> None:
        self.severity = severity
        self.context = context
        super().__init__(message)


class KafkaProcessingError(BaseProcessingError):
    """Kafka 처리 관련 에러"""

    pass


class DataProcessingError(BaseProcessingError):
    """데이터 처리 관련 에러"""

    pass


# 에러 처리를 위한 데코레이터
def handle_kafka_errors(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            match e:
                case KafkaProcessingError():
                    await logger(logging.ERROR, f"Kafka 처리 오류: {e.detail}")
                case ConnectionError():
                    await logger(logging.ERROR, f"연결 오류: {str(e)}")
                case ValueError():
                    await logger(logging.ERROR, f"값 오류: {str(e)}")
                case _:
                    import traceback

                    await logger(
                        logging.ERROR,
                        f"예상치 못한 오류: {str(e)} --> {traceback.print_exc()}",
                    )

    return wrapper


def handle_processing_errors(logger):
    """통합 에러 처리 데코레이터"""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except json.JSONDecodeError as e:
                error = KafkaProcessingError(
                    ErrorType.PROCESSING,
                    f"JSON 파싱 오류: {str(e)}",
                    severity=ErrorSeverity.HIGH,
                    context=ErrorContext(
                        exchange="unknown",
                        operation="json_parsing",
                        timestamp=time.time(),
                    ),
                )
                await logger.error(
                    f"처리 에러 발생: {str(error)}",
                    extra={
                        "severity": error.severity.value,
                        "exchange": error.context.exchange,
                        "operation": error.context.operation,
                        "timestamp": error.context.timestamp,
                        "details": error.context.details,
                    },
                )
                raise error
            except ValueError as e:
                error = KafkaProcessingError(
                    ErrorType.PROCESSING,
                    f"값 변환 오류: {str(e)}",
                    severity=ErrorSeverity.MEDIUM,
                    context=ErrorContext(
                        exchange="unknown",
                        operation="value_conversion",
                        timestamp=time.time(),
                    ),
                )
                await logger.error(
                    f"처리 에러 발생: {str(error)}",
                    extra={
                        "severity": error.severity.value,
                        "exchange": error.context.exchange,
                        "operation": error.context.operation,
                        "timestamp": error.context.timestamp,
                        "details": error.context.details,
                    },
                )
                raise error
            except BaseProcessingError as e:
                await logger.error(
                    f"처리 에러 발생: {str(e)}",
                    extra={
                        "severity": e.severity.value,
                        "exchange": e.context.exchange,
                        "operation": e.context.operation,
                        "timestamp": e.context.timestamp,
                        "details": e.context.details,
                    },
                )
                if e.severity in (ErrorSeverity.HIGH, ErrorSeverity.CRITICAL):
                    raise
            except Exception as e:
                import traceback

                error = KafkaProcessingError(
                    ErrorType.PROCESSING,
                    f"알 수 없는 처리 오류: {str(e)}",
                    severity=ErrorSeverity.CRITICAL,
                    context=ErrorContext(
                        exchange="unknown",
                        operation="unknown",
                        timestamp=time.time(),
                        details={"traceback": traceback.format_exc()},
                    ),
                )
                await logger.error(
                    f"예상치 못한 에러: {str(error)}",
                    extra={
                        "severity": error.severity.value,
                        "exchange": error.context.exchange,
                        "operation": error.context.operation,
                        "timestamp": error.context.timestamp,
                        "details": error.context.details,
                    },
                )
                raise error

        return wrapper

    return decorator
