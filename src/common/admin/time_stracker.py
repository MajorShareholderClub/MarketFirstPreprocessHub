from __future__ import annotations
from collections.abc import Callable, Awaitable
from dataclasses import dataclass
from typing import TypeVar, Generic
from collections import deque
from weakref import proxy
import traceback
from redis.asyncio import Redis


import time
import asyncio
from datetime import datetime
from mq.dlt_producer import DLTProducer

T = TypeVar("T")


@dataclass
class StackConfig:
    """스택 설정"""

    topic: str
    partition: int
    max_size: int = 20
    timeout_ms: int = 5000
    adaptive_timeout: bool = True  # 적응형 타임아웃 사용 여부


class TimeStack(Generic[T]):
    """시간 기반 스택"""

    def __init__(
        self,
        config: StackConfig,
        flush_handler: Callable[[list[T], StackConfig], Awaitable[None]],
        dlt_producer: DLTProducer | None = None,
    ):
        self.config = config
        self._items: deque[T] = deque(maxlen=config.max_size)
        self.items = proxy(self._items)
        self.last_update: float = time.time()
        self.created_at: datetime = datetime.now()
        self.flush_handler = flush_handler
        self.dlt_producer = dlt_producer
        self.message_rates: deque[tuple[float, int]] = deque(
            maxlen=10
        )  # 최근 10개의 (시간, 메시지 수) 기록
        self.last_rate_check = time.time()
        self.current_timeout = config.timeout_ms

    def add_item(self, item: T) -> None:
        self._items.append(item)
        self.last_update = time.time()

        # 메시지 유입 속도 측정
        current_time = time.time()
        if current_time - self.last_rate_check >= 1.0:  # 1초마다 체크
            self.message_rates.append((current_time, len(self._items)))
            self.last_rate_check = current_time
            self._adjust_timeout()

    def _adjust_timeout(self) -> None:
        """메시지 유입 속도에 따라 타임아웃 동적 조정"""
        if not self.config.adaptive_timeout or len(self.message_rates) < 2:
            return

        # 최근 메시지 유입 속도 계산
        recent_rates: list[int] = [count for _, count in self.message_rates]
        avg_rate: float = sum(recent_rates) / len(recent_rates)

        # 유입 속도에 따라 타임아웃 조정
        if avg_rate > 10:  # 초당 10개 이상
            self.current_timeout = min(1000, self.config.timeout_ms)  # 빠른 처리
        elif avg_rate > 5:  # 초당 5-10개
            self.current_timeout = min(2000, self.config.timeout_ms)
        else:  # 느린 유입
            self.current_timeout = self.config.timeout_ms

    def should_flush(self) -> bool:
        """플러시 조건 확인"""
        current_size = len(self._items)
        elapsed_ms = (time.time() - self.last_update) * 1000

        # 1. 최대 크기 도달
        if current_size >= self.config.max_size:
            return True

        # 2. 적응형 타임아웃 기반 플러시
        if self.config.adaptive_timeout:
            if current_size > 0 and elapsed_ms >= self.current_timeout:
                return True
        else:
            # 기존 타임아웃 로직
            if current_size > 0 and elapsed_ms >= self.config.timeout_ms:
                return True

        return False

    async def flush(self) -> None:
        if not self._items:  # _items 직접 사용
            return

        items = list(self._items)
        self._items.clear()
        self.last_update = time.time()

        try:
            await self.flush_handler(items, self.config)
        except Exception as e:
            if self.dlt_producer:
                async with self.dlt_producer as dlt_producer:
                    for item in items:
                        await dlt_producer.send_to_dlt(
                            original_topic=self.config.topic,
                            original_partition=self.config.partition,
                            error_message=str(e),
                            error_traceback=traceback.format_exc(),
                            payload=item,
                        )
        finally:
            del items


class TimeStacker:
    """시간 기반 스택 관리자"""

    def __init__(
        self,
        flush_handler: Callable[[list[T], StackConfig], Awaitable[None]],
        dlt_producer: DLTProducer | None = None,
    ) -> None:
        self.stacks: dict[tuple[str, int], TimeStack] = {}
        self.dlt_producer = dlt_producer
        self.flush_handler = flush_handler
        self._running = False
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._redis_client = Redis(host="localhost", port=6379, db=0)
        self._cleanup_counter = 0
        self._cleanup_threshold = 1000

    def get_or_create_stack(self, config: StackConfig) -> TimeStack:
        """스택 가져오기 또는 생성"""
        key: tuple[str, int] = (config.topic, config.partition)
        if key not in self.stacks:
            self.stacks[key] = TimeStack(config, self.flush_handler, self.dlt_producer)
        return self.stacks[key]

    async def start(self) -> None:
        """스택 관리자 시작"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._check_timeouts())

    async def stop(self) -> None:
        """스택 관리자 중지"""
        self._running = False
        if self._task:
            await self._task

    async def _check_timeouts(self) -> None:
        """타임아웃된 스택 확인"""
        while self._running:
            try:
                for stack in list(self.stacks.values()):
                    lock_key = f"lock:{stack.config.topic}:{stack.config.partition}"
                    async with self._redis_client.lock(
                        lock_key,
                        timeout=5,
                        blocking_timeout=3,
                        thread_local=False,
                    ):
                        if stack.should_flush():
                            await stack.flush()
                await asyncio.sleep(0.5)  # 고정된 체크 주기
            except Exception as e:
                print(f"Error in _check_timeouts: {e}")
                await asyncio.sleep(1)

    async def add_item(self, item: T, config: StackConfig) -> None:
        """아이템 추가"""
        lock_key = f"lock:{config.topic}:{config.partition}"
        
        async with self._redis_client.lock(
            lock_key,
            timeout=5,
            blocking_timeout=3,
            thread_local=False,
        ):
            stack: TimeStack = self.get_or_create_stack(config)
            stack.add_item(item)

            if stack.should_flush():
                await stack.flush()

            self._cleanup_counter += 1
            if self._cleanup_counter >= self._cleanup_threshold:
                await self._cleanup_old_stacks()
                self._cleanup_counter = 0

    async def _cleanup_old_stacks(self) -> None:
        """오래된 스택 정리"""
        current_time = time.time()
        keys_to_remove: deque[tuple[str, int]] = deque(maxlen=100)

        for key, stack in self.stacks.items():
            if current_time - stack.last_update > 60:  # 1분 이상 미사용
                print(f"Removing empty stack for key: {key}")

                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self.stacks[key]
