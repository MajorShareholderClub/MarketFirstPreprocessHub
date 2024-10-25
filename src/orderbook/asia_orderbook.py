from src.common.common_orderbook import BaseAsyncOrderbookPrepcessor
from mq.types import ProcessedOrderBook, OrderEntry


class BybitAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(
        self, item: OrderEntry, symbol: str, market: str
    ) -> ProcessedOrderBook:
        bid_data = item["data"]["b"]
        ask_data = item["data"]["a"]

        return self.orderbook_common_processing(
            bid_data=bid_data, ask_data=ask_data, market=market, symbol=symbol
        )


class GateIOAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(
        self, item: OrderEntry, symbol: str, market: str
    ) -> ProcessedOrderBook:
        bid_data = item["result"]["bids"]
        ask_data = item["result"]["asks"]

        return self.orderbook_common_processing(
            bid_data=bid_data, ask_data=ask_data, market=market, symbol=symbol
        )


class OKXAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(
        self, item: OrderEntry, symbol: str, market: str
    ) -> ProcessedOrderBook:
        bid_data = [(item[0], item[1]) for item in item["data"][0]["bids"]]
        ask_data = [(item[0], item[1]) for item in item["data"][0]["asks"]]

        return self.orderbook_common_processing(
            bid_data=bid_data, ask_data=ask_data, market=market, symbol=symbol
        )
