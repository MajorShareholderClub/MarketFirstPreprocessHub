from src.common.common_orderbook import BaseAsyncOrderbookPrepcessor
from mq.types import ProcessedOrderBook, OrderEntry


class CoinoneKorbitAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(
        self, item: OrderEntry, symbol: str, market: str
    ) -> ProcessedOrderBook:
        def price_amount(item: dict) -> tuple[float]:
            return (float(item["price"]), float(item.get("amount") or item.get("qty")))

        ask_data = [price_amount(item=item) for item in item["data"]["asks"]]
        bid_data = [price_amount(item=item) for item in item["data"]["bids"]]

        return self.orderbook_common_processing(
            bid_data=bid_data, ask_data=ask_data, market=market, symbol=symbol
        )


class UpBithumbAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(
        self, item: OrderEntry, symbol: str, market: str
    ) -> ProcessedOrderBook:
        unit_data = item["orderbook_units"]

        bid_data = [(entry["bid_price"], entry["bid_size"]) for entry in unit_data]
        ask_data = [(entry["ask_price"], entry["ask_size"]) for entry in unit_data]
        return self.orderbook_common_processing(
            bid_data=bid_data, ask_data=ask_data, market=market, symbol=symbol
        )
