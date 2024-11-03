from typing import Callable
import traceback
from aiokafka import AIOKafkaConsumer


def consuming_message(message: dict, c_partition: int, process_func: Callable) -> dict:
    """컨슈머 메시지 로깅

    Args:
        message (dict): 컨슈머로부터 받은 메시지
        c_partition (int): 요청한 파티션 번호
        process_func (Callable): 처리해야 할 함수

    Returns:
        dict: 로깅 정보를 담은 딕셔너리
    """
    return {
        "requested_partition": c_partition,
        "message_partition": message.partition,
        "topic": message.topic,
        "offset": message.offset,
        "timestamp": message.timestamp,
        "key": message.key,
        "process_method": process_func.__name__,
    }


def consuming_error(
    error: Exception,
    c_partition: int,
    process_func: Callable,
    consumer: AIOKafkaConsumer,
) -> dict:
    """컨슈머 에러 로깅

    Args:
        error (Exception): 발생한 예외
        c_partition (int): 요청한 파티션 번호
        process_func (Callable): 처리하려던 함수
        consumer (AIOKafkaConsumer): 카프카 컨슈머 인스턴스

    Returns:
        dict: 에러 로깅 정보를 담은 딕셔너리
    """
    return {
        "topic_info": str(consumer.assignment()),
        "consumer_id": consumer._client._client_id,
        "partition": c_partition,
        "method": process_func.__name__,
        "error_trace": traceback.format_exc(),
        "error_message": str(error),
    }


def task_init_logging(process: Callable) -> dict:
    """태스크 초기화 로깅

    Args:
        process (Callable): 초기화된 프로세스 객체

    Returns:
        dict: 초기화 상태 정보를 담은 딕셔너리
    """
    return {
        "class": process.__class__.__name__,
        "consumer_topic": process.consumer_topic,
        "partition": process.c_partition,
    }
