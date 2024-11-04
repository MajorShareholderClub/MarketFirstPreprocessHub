from enum import Enum
from typing import TypedDict


class Region(Enum):
    """거래소 지역 구분"""

    KOREA = "Korea"
    ASIA = "Asia"
    NE = "NE"


RegionTask = tuple[Region, str, int]


class ExchangeConfig(TypedDict):
    UPBIT: RegionTask
    BITHUMB: RegionTask
    COINONE: RegionTask
    KORBIT: RegionTask
    OKX: RegionTask
    BYBIT: RegionTask
    GATEIO: RegionTask
    BINANCE: RegionTask
    KRAKEN: RegionTask
