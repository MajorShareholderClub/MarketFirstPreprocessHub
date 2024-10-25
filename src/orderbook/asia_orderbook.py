from src.common.common_orderbook import BaseAsyncOrderbookPrepcessor
from mq.types import ProcessedOrderBook, OrderEntry


class BybitAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(self, item: OrderEntry) -> ProcessedOrderBook:
        bid_data = item["data"]["b"]
        ask_data = item["data"]["a"]

        return self.orderbook_common_processing(bid_data=bid_data, ask_data=ask_data)


class GateIOAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(self, item: OrderEntry) -> ProcessedOrderBook:
        bid_data = item["result"]["bids"]
        ask_data = item["result"]["asks"]

        return self.orderbook_common_processing(bid_data=bid_data, ask_data=ask_data)


class OKXAsyncOrderbookProcessor(BaseAsyncOrderbookPrepcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def order_preprocessing(self, item: OrderEntry) -> ProcessedOrderBook:
        bid_data = [(item[0], item[1]) for item in item["data"][0]["bids"]]
        ask_data = [(item[0], item[1]) for item in item["data"][0]["asks"]]

        return self.orderbook_common_processing(bid_data=bid_data, ask_data=ask_data)


# async def okx_orderbook_cp(
#     consumer_topic: str,
#     c_partition: int,
#     group_id: str,
#     producer_topic: str,
#     p_partition: int,
#     p_key: str,
# ) -> None:
#     """시작점"""
#     processor = OKXAsyncOrderbookProcessor(
#         consumer_topic=consumer_topic,
#         c_partition=c_partition,
#         group_id=group_id,
#         producer_topic=producer_topic,
#         p_partition=p_partition,
#         p_key=p_key,
#     )
#     await processor.initialize()
#     try:
#         await processor.batch_process_messages(target="orderbook")
#     finally:
#         await processor.cleanup()
