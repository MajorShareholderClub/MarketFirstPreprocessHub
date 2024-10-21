from __future__ import annotations

import json
from src.common_consumer import CommoneConsumerSettingProcesser

from mq.types import OrderBookData, ProcessedOrderBook, OrderEntry
from mq.exception import (
    handle_processing_errors,
)


class CoinoneKorbitAsyncOrderbookProcessor(CommoneConsumerSettingProcesser):
    """비동기 주문서 데이터를 처리하는 클래스."""

    @handle_processing_errors
    def calculate_total_bid_ask(
        self, orderbook_data: OrderBookData
    ) -> ProcessedOrderBook:
        """주문서 데이터를 기반으로 주문서 메트릭 계산.

        Args:
            orderbook_data (OrderBookData): 주문서 데이터

        Returns:
            ProcessedOrderBook: 처리된 주문서 데이터
        """

        def price_amount(item: dict) -> tuple[float]:
            return (float(item["price"]), float(item.get("amount") or item.get("qty")))

        for record_str in orderbook_data["data"]:
            record: OrderEntry = json.loads(record_str)
            ask_data = [price_amount(item=item) for item in record["data"]["asks"]]
            bid_data = [price_amount(item=item) for item in record["data"]["bids"]]

            return self.orderbook_common_precessing(
                bid_data=bid_data, ask_data=ask_data
            )


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
        await processor.batch_process_messages()
    finally:
        await processor.cleanup()
