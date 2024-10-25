from src.common.common_orderbook import BaseAsyncOrderbookPrepcessor
from mq.types import ProcessedOrderBook, OrderEntry


class KrakenAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):

    def order_preprocessing(self, item: OrderEntry) -> ProcessedOrderBook:
        def price_amount(item: dict) -> tuple[float]:
            return (float(item["price"]), float(item.get("qty")))

        ask_data = [price_amount(item=record) for record in item["data"][0]["asks"]]
        bid_data = [price_amount(item=record) for record in item["data"][0]["bids"]]

        return self.orderbook_common_processing(bid_data=bid_data, ask_data=ask_data)


class BinanceAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):

    def order_preprocessing(self, item: OrderEntry) -> ProcessedOrderBook:
        def price_amount(price, amount) -> tuple[float, float]:
            return (float(price), float(amount))

        ask_data = [price_amount(price, amount) for price, amount in item["asks"]]
        bid_data = [price_amount(price, amount) for price, amount in item["bids"]]

        return self.orderbook_common_processing(bid_data=bid_data, ask_data=ask_data)


async def binance_orderbook_cp(
    consumer_topic: str,
    c_partition: int,
    group_id: str,
    producer_topic: str,
    p_partition: int,
    p_key: str,
) -> None:
    """시작점"""
    processor = BinanceAsyncOrderbookProcessor(
        consumer_topic=consumer_topic,
        c_partition=c_partition,
        group_id=group_id,
        producer_topic=producer_topic,
        p_partition=p_partition,
        p_key=p_key,
    )
    await processor.initialize()
    try:
        await processor.batch_process_messages(target="orderbook")
    finally:
        await processor.cleanup()
