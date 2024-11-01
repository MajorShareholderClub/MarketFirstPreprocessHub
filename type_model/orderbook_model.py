from typing import TypedDict, Required


class OrderBookData(TypedDict):
    region: str
    market: str
    symbol: str
    timestamp: int
    data: list[str]


class OrderEntryData(TypedDict):
    b: list[list[str]]  # 매수 데이터: [가격, 거래량]
    a: list[list[str]]  # 매도 데이터: [가격, 거래량]


class OrderEntry(TypedDict):
    data: OrderEntryData


class ProcessedOrderBook(TypedDict):
    region: Required[str]
    market: Required[str]
    symbol: Required[str]
    highest_bid: Required[float | None]  # 최고 매수 가격
    lowest_ask: Required[float | None]  # 최저 매도 가격
    total_bid_volume: Required[float]  # 총 매수 거래량
    total_ask_volume: Required[float]  # 총 매도 거래량
    timestamp: Required[int | None]  # 타임스탬프


ExchangeResponseData = dict[str, str | int | float | dict[str, int | str]]
ExchangeOrderingData = dict[str, int]
ResponseData = ExchangeResponseData | ExchangeOrderingData
