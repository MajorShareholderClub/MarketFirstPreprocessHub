from __future__ import annotations

import functools
import json
import logging
from enum import Enum, auto
from typing import Callable, ParamSpec, TypeVar, Awaitable


# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 타입 정의
P = ParamSpec("P")
R = TypeVar("R")


# 사용자 정의 예외
class KafkaProcessingError(Exception):
    def __init__(self, error_type: ErrorType, detail: str) -> None:
        self.error_type = error_type
        self.detail = detail
        super().__init__(f"{error_type.name}: {detail}")


class ErrorType(Enum):
    KAFKA_CONNECTION = auto()
    PROCESSING = auto()
    INITIALIZATION = auto()
    CLEANUP = auto()


# 에러 처리를 위한 데코레이터
def handle_kafka_errors(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            match e:
                case KafkaProcessingError():
                    logger.error(f"Kafka 처리 오류: {e.detail}")
                case ConnectionError():
                    logger.error(f"연결 오류: {str(e)}")
                case ValueError():
                    logger.error(f"값 오류: {str(e)}")
                case _:
                    logger.error(f"예상치 못한 오류: {str(e)}")

    return wrapper


def handle_processing_errors(func: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            match type(e):
                case json.JSONDecodeError():
                    raise KafkaProcessingError(
                        ErrorType.PROCESSING, f"JSON 파싱 오류: {str(e)}"
                    )
                case ValueError():
                    raise KafkaProcessingError(
                        ErrorType.PROCESSING, f"값 변환 오류: {str(e)}"
                    )
                case _:
                    raise KafkaProcessingError(
                        ErrorType.PROCESSING, f"알 수 없는 처리 오류: {str(e)}"
                    )

    return wrapper
