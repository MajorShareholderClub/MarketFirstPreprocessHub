from __future__ import annotations

import json
from src.common_consumer import CommonConsumerSettingProcessor

from mq.types import OrderBookData, ProcessedOrderBook, OrderEntry
from mq.exception import handle_processing_errors


class BybitAsyncOrderbookProcessor(CommonConsumerSettingProcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    @handle_processing_errors
    def calculate_total_bid_ask(self, orderbook: OrderBookData) -> ProcessedOrderBook:
        """주문서 데이터를 기반으로 주문서 메트릭 계산.

        Args:
            orderbook (OrderBookData): 주문서 데이터

        Returns:
            ProcessedOrderBook: 처리된 주문서 데이터
        """
        for record_str in orderbook["data"]:
            record: OrderEntry = json.loads(record_str)
            bid_data = record["data"]["b"]
            ask_data = record["data"]["a"]

            return self.orderbook_common_processing(
                bid_data=bid_data, ask_data=ask_data
            )


async def bybit_orderbook_cp(
    consumer_topic: str,
    c_partition: int,
    p_partition: int,
    p_key: str,
    group_id: str,
    producer_topic: str,
) -> None:
    """시작점"""
    processor = BybitAsyncOrderbookProcessor(
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
