from src.common.common_orderbook import BaseAsyncOrderbookPrepcessor
from mq.types import ProcessedOrderBook, OrderEntry


class CoinoneKorbitAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(self, item: OrderEntry) -> ProcessedOrderBook:
        def price_amount(item: dict) -> tuple[float]:
            return (float(item["price"]), float(item.get("amount") or item.get("qty")))

        ask_data = [price_amount(item=item) for item in item["data"]["asks"]]
        bid_data = [price_amount(item=item) for item in item["data"]["bids"]]

        return self.orderbook_common_processing(bid_data=bid_data, ask_data=ask_data)


class UpBithumbAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(self, item: OrderEntry) -> ProcessedOrderBook:
        unit_data = item["orderbook_units"]

        bid_data = [(entry["bid_price"], entry["bid_size"]) for entry in unit_data]
        ask_data = [(entry["ask_price"], entry["ask_size"]) for entry in unit_data]
        return self.orderbook_common_processing(bid_data=bid_data, ask_data=ask_data)


async def onekorbit_orderbook_cp(
    consumer_topic: str,
    c_partition: int,
    group_id: str,
    producer_topic: str,
    p_partition: int,
    p_key: str,
) -> None:
    """시작점"""
    processor = CoinoneKorbitAsyncOrderbookProcessor(
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
