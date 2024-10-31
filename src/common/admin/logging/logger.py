import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Any
from dataclasses import dataclass
from asyncio import Queue, QueueFull
from logging.handlers import RotatingFileHandler


@dataclass
class LogConfig:
    """로그 설정을 위한 데이터 클래스"""

    level: int = logging.INFO
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    max_queue_size: int = 1000
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


class AsyncLogHandler:
    """비동기 로그 처리를 위한 핸들러"""

    def __init__(self, handler: logging.Handler, level: int) -> None:
        self.handler = handler
        self.level = level
        self.queue: Queue = Queue(maxsize=1000)
        self.task: Optional[asyncio.Task] = None
        self._stop = False

    async def start(self) -> None:
        """로그 처리 작업 시작"""
        self.task = asyncio.create_task(self._process_logs())

    async def stop(self) -> None:
        """로그 처리 작업 중지"""
        self._stop = True
        if self.task:
            await self.queue.put(None)  # 종료 신호
            await self.task

    async def emit(self, record: logging.LogRecord) -> None:
        """로그 레코드 큐에 추가"""
        if record.levelno >= self.level:
            try:
                await self.queue.put(record)
            except QueueFull:
                print(
                    f"Log queue full, dropping message: {record.getMessage()}",
                    file=sys.stderr,
                )

    async def _process_logs(self) -> None:
        """큐에서 로그를 처리하는 메인 루프"""
        while not self._stop:
            try:
                record = await self.queue.get()
                if record is None:  # 종료 신호
                    break
                self.handler.emit(record)
            except Exception as e:
                print(f"Error processing log: {e}", file=sys.stderr)
            finally:
                self.queue.task_done()


class AsyncLogger:
    """개선된 비동기 로거"""

    def __init__(
        self,
        name: str,
        folder: str | None = None,
        file: str | None = None,
        config: LogConfig = LogConfig(),
    ):
        self.name = name
        self.config = config
        self.logger = logging.getLogger(name)
        self.logger.setLevel(config.level)

        # 로그 파일 경로 설정
        self._setup_log_paths(folder, file)

        # 비동기 핸들러 리스트
        self.async_handlers: list[AsyncLogHandler] = []

        # 로거 설정
        self._setup_handlers()

        self._started = False  # 로거 시작 상태 추적
        self._start_lock = asyncio.Lock()  # 동시 시작 방지를 위한 락

    def _setup_log_paths(self, folder: str | None, file: str | None) -> None:
        """로그 파일 경로 설정"""
        # 기본 로그 디렉토리는 'logs'
        base_dir = Path("logs")

        # 폴더 경로 설정
        if folder:
            self.log_dir = base_dir / folder
        else:
            self.log_dir = base_dir / self.name

        # 파일명 설정
        if file:
            self.file_name = file
        else:
            self.file_name = self.name

        # 로그 파일 전체 경로 설정
        self.log_paths = {
            "main": self.log_dir / f"{self.file_name}.log",
            "error": self.log_dir / f"{self.file_name}_error.log",
            "debug": self.log_dir / f"{self.file_name}_debug.log",
        }

        # 디렉토리 생성
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def _setup_handlers(self) -> None:
        """로그 핸들러 설정"""
        formatter = logging.Formatter(
            fmt=self.config.format, datefmt=self.config.date_format
        )

        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        async_console = AsyncLogHandler(console_handler, logging.INFO)
        self.async_handlers.append(async_console)

        # 파일 핸들러들
        handlers_config = {
            "main": {"level": logging.INFO, "path": self.log_paths["main"]},
            "error": {"level": logging.ERROR, "path": self.log_paths["error"]},
            "debug": {"level": logging.DEBUG, "path": self.log_paths["debug"]},
        }

        for handler_name, cfg in handlers_config.items():
            handler = RotatingFileHandler(
                filename=cfg["path"],
                maxBytes=self.config.max_file_size,
                backupCount=self.config.backup_count,
                encoding="utf-8",
            )
            handler.setLevel(cfg["level"])
            handler.setFormatter(formatter)
            async_handler = AsyncLogHandler(handler, cfg["level"])
            self.async_handlers.append(async_handler)

    async def start(self) -> None:
        """모든 핸들러 시작"""
        await asyncio.gather(*(handler.start() for handler in self.async_handlers))

    async def stop(self) -> None:
        """모든 핸들러 중지"""
        await asyncio.gather(*(handler.stop() for handler in self.async_handlers))

    async def _ensure_started(self) -> None:
        """로거가 시작되지 않았다면 시작"""
        if not self._started:
            async with self._start_lock:
                if not self._started:  # 이중 체크
                    await self.start()
                    self._started = True

    async def _log(self, level: int, message: str, *args, **kwargs) -> None:
        """내부 로깅 메서드"""
        await self._ensure_started()  # 로거 시작 확인
        if args:
            message = message % args
        record = self.logger.makeRecord(
            self.name, level, "(unknown file)", 0, message, None, None, None, kwargs
        )
        await asyncio.gather(*(handler.emit(record) for handler in self.async_handlers))

    async def debug(self, message: str, *args, **kwargs) -> None:
        await self._log(logging.DEBUG, message, *args, **kwargs)

    async def info(self, message: str, *args, **kwargs) -> None:
        await self._log(logging.INFO, message, *args, **kwargs)

    async def warning(self, message: str, *args, **kwargs) -> None:
        await self._log(logging.WARNING, message, *args, **kwargs)

    async def error(self, message: str, *args, **kwargs) -> None:
        await self._log(logging.ERROR, message, *args, **kwargs)

    async def __aenter__(self) -> "AsyncLogger":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
        if exc_val:
            await self.error(f"Error on exit: {exc_val}")
