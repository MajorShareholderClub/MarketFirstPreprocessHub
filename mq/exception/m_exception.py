from __future__ import annotations

import functools
import json
import logging
from enum import Enum, auto
from typing import Callable, ParamSpec, TypeVar, Awaitable
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
    details: dict | None = None


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


def handle_processing_errors(func: Callable[P, R]) -> Callable[P, R]:
    """에러를 KafkaProcessingError로 변환하는 데코레이터"""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            match type(e):
                case json.JSONDecodeError():
                    error_msg = f"JSON 파싱 오류: {str(e)}"
                    severity = ErrorSeverity.HIGH
                case ValueError():
                    error_msg = f"값 변환 오류: {str(e)}"
                    severity = ErrorSeverity.MEDIUM
                case _:
                    import traceback

                    error_msg = (
                        f"알 수 없는 처리 오류: {str(e)} -- {traceback.format_exc()}"
                    )
                    severity = ErrorSeverity.CRITICAL

            context = ErrorContext(
                exchange="unknown",
                operation="processing",
                timestamp=time.time(),
                details={"original_error": str(e)},
            )

            raise DataProcessingError(
                message=error_msg,
                severity=severity,
                context=context,
            )

    return wrapper
