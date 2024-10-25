from src.common.common_orderbook import BaseAsyncOrderbookPrepcessor
from mq.types import ProcessedOrderBook, OrderEntry


class KrakenAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):

    def order_preprocessing(
        self, item: OrderEntry, symbol: str, market: str
    ) -> ProcessedOrderBook:
        def price_amount(item: dict) -> tuple[float]:
            return (float(item["price"]), float(item.get("qty")))

        ask_data = [price_amount(item=record) for record in item["data"][0]["asks"]]
        bid_data = [price_amount(item=record) for record in item["data"][0]["bids"]]

        return self.orderbook_common_processing(
            bid_data=bid_data, ask_data=ask_data, market=market, symbol=symbol
        )


class BinanceAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):

    def order_preprocessing(
        self, item: OrderEntry, symbol: str, market: str
    ) -> ProcessedOrderBook:
        def price_amount(price, amount) -> tuple[float, float]:
            return (float(price), float(amount))

        ask_data = [price_amount(price, amount) for price, amount in item["asks"]]
        bid_data = [price_amount(price, amount) for price, amount in item["bids"]]
        return self.orderbook_common_processing(
            bid_data=bid_data, ask_data=ask_data, market=market, symbol=symbol
        )
