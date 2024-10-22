import asyncio
from typing import Awaitable
from dataclasses import dataclass
from enum import Enum

from src.ticker.ne_ticker import BinanceAsyncTickerProcessor, KrakensyncTickerProcessor
from src.ticker.korea_ticker import (
    UpbithumbAsyncTickerProcessor,
    CoinonesyncTickerProcessor,
    KorbitsyncTickerProcessor,
)
from src.ticker.asia_ticker import (
    BybitAsyncTickerProcessor,
    GateIoAsyncTickerProcessor,
    OKXAsyncTickerProcessor,
)


data = {
    "consumer_topic": "asiaSocketDataInBTC",
    "c_partition": 2,
    "group_id": "ticker",
    "producer_topic": "TestProcess",
    "p_partition": 0,
    "p_key": "TEst",
}

TickerbookProcessor = type[Awaitable[None]]


class Region(str, Enum):
    """거래소 지역 구분"""

    KOREA = "Korea"
    ASIA = "Asia"
    NE = "NE"


class Exchange(Enum):
    UPBIT = ("upbit", 0, 0)
    BITHUMB = ("bithumb", 2, 2)
    COINONE = ("coinone", 4, 4)
    KORBIT = ("korbit", 6, 6)
    OKX = ("okx", 0, 0)
    BYBIT = ("bybit", 2, 2)
    GATEIO = ("gateio", 4, 4)
    BINANCE = ("binance", 0, 0)
    KRAKEN = ("kraken", 2, 2)

    def __init__(self, exchange_name: str, c_partition: int, p_partition: int) -> None:
        self.exchange_name = exchange_name
        self.c_partition = c_partition
        self.p_partition = p_partition

    @property
    def group_id(self) -> str:
        return f"{self.exchange_name}_ticker_group_id"


@dataclass
class ExchangeConfig:
    """거래소 설정 데이터 클래스"""

    exchange: Exchange
    processor: TickerbookProcessor

    @property
    def name(self) -> str:
        return self.exchange.exchange_name

    @property
    def c_partition(self) -> int:
        return self.exchange.c_partition

    @property
    def p_partition(self) -> int:
        return self.exchange.p_partition
