from typing import Callable
import traceback
from typing import Any
from aiokafka import AIOKafkaConsumer


def consuming_message(message: dict, c_partition: int, process_func: Callable) -> str:
    """컨슈머 메시지 로깅
    요청한 파티션: {c_partition}
    메시지 파티션: {message.partition}
    토픽: {message.topic}
    오프셋: {message.offset}
    타임스탬프: {message.timestamp}
    키: {message.key}
    처리해야할 메서드 : {process_func.__name__}
    """
    return f"""
        요청한 파티션: {c_partition}
        메시지 파티션: {message.partition}
        토픽: {message.topic}
        오프셋: {message.offset}
        타임스탬프: {message.timestamp}
        키: {message.key}
        처리해야할 메서드 : {process_func.__name__}
    """


def consuming_error(
    error: Exception,
    c_partition: int,
    process_func: Callable,
    consumer: AIOKafkaConsumer,
) -> str:
    """컨슈머 에러 로깅
    - message :
        - topic_infomer --> {consumer.assignment()}
        - consumer_id --> {consumer._client._client_id}
        - partition --> {c_partition}
        - method --> {process_func.__name__}
        - error_trace --> {traceback.format_exc()}
        - error_message --> {str(error)}

    error_data = {
        "error_message": str(error),
        "error_trace": traceback.format_exc(),
        "failed_message": message,
    }
    """
    message = f"""
        topic_infomer --> {consumer.assignment()}
        consumer_id --> {consumer._client._client_id}
        partition --> {c_partition}
        method --> {process_func.__name__}
        error_trace --> {traceback.format_exc()}
        error_message --> {str(error)}
    """

    error_data = {
        "error_message": str(error),
        "error_trace": traceback.format_exc(),
        "failed_message": message,
    }
    return error_data


def metrics_logging(metrics: Any) -> str:
    """메트릭스 로깅 Any -> MetricsManager
    메트릭스 현황:
            총 처리 메시지: {metrics.total_messages}
            평균 배치 크기: {metrics.avg_batch_size:.2f}
            평균 처리 시간: {metrics.avg_processing_time:.2f}초
            실패 메시지 수: {metrics.failed_messages}
    """
    return f"""메트릭스 현황:
            총 처리 메시지: {metrics.total_messages}
            평균 배치 크기: {metrics.avg_batch_size:.2f}
            평균 처리 시간: {metrics.avg_processing_time:.2f}초
            실패 메시지 수: {metrics.failed_messages}
        """


def task_init_logging(process: Callable) -> str:
    """태스크 초기화 로깅
    프로세스 초기화 상태:
        클래스: {process.__class__.__name__}
        컨슈머 토픽: {process.consumer_topic}
        파티션: {process.c_partition}
    """
    return f""" 프로세스 초기화 상태:
            클래스: {process.__class__.__name__}
            컨슈머 토픽: {process.consumer_topic}
            파티션: {process.c_partition}
        """
