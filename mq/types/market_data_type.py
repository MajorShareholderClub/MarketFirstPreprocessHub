from typing import TypedDict


class OrderBookData(TypedDict):
    data: list[str]
    timestamp: int


class OrderEntryData(TypedDict):
    b: list[list[str]]  # 매수 데이터: [가격, 거래량]
    a: list[list[str]]  # 매도 데이터: [가격, 거래량]


class OrderEntry(TypedDict):
    data: OrderEntryData


class ProcessedOrderBook(TypedDict):
    highest_bid: float | None  # 최고 매수 가격
    lowest_ask: float | None  # 최저 매도 가격
    spread: float | None  # 스프레드 (차이)
    total_bid_volume: float  # 총 매수 거래량
    total_ask_volume: float  # 총 매도 거래량
    bid_order_count: int | None  # 총 매수 주문량
    ask_order_count: int | None  # 총 매도 주문량
    timestamp: int | None  # 타임스탬프


ExchangeResponseData = dict[str, str | int | float | dict[str, int | str]]
ExchangeOrderingData = dict[str, int]
ResponseData = ExchangeResponseData | ExchangeOrderingData
