from __future__ import annotations
from collections.abc import Callable, Awaitable
from dataclasses import dataclass
from typing import TypeVar, Generic
from collections import deque
from weakref import proxy
import traceback

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
    timeout_ms: int = 500


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

    def add_item(self, item: T) -> None:
        self._items.append(item)
        self.last_update = time.time()

    def should_flush(self) -> bool:
        """플러시 조건 확인"""
        return (
            len(self.items) >= self.config.max_size
            or (time.time() - self.last_update) * 1000 >= self.config.timeout_ms
        )

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
        self, flush_handler: Callable[[list[T], StackConfig], Awaitable[None]]
    ) -> None:
        self.stacks: dict[tuple[str, int], TimeStack] = {}
        self.flush_handler = flush_handler
        self._running = False
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._cleanup_counter = 0
        self._cleanup_threshold = 1000

    def get_or_create_stack(self, config: StackConfig) -> TimeStack:
        """스택 가져오기 또는 생성"""
        key = (config.topic, config.partition)
        if key not in self.stacks:
            self.stacks[key] = TimeStack(config, self.flush_handler)
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
            async with self._lock:
                for stack in list(self.stacks.values()):
                    if stack.should_flush():
                        await stack.flush()
            await asyncio.sleep(0.1)

    async def add_item(self, item: T, config: StackConfig) -> None:
        """아이템 추가"""
        async with self._lock:
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
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self.stacks[key]
