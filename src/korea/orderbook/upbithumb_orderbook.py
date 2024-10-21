from __future__ import annotations

import json
from src.common_consumer import CommoneConsumerSettingProcesser

from mq.types import OrderBookData, ProcessedOrderBook, OrderEntry
from mq.exception import (
    handle_processing_errors,
)


class UpBithumbAsyncOrderbookProcessor(CommoneConsumerSettingProcesser):
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
            unit_data = record_str["orderbook_units"]

            bid_data = [(entry["bid_price"], entry["bid_size"]) for entry in unit_data]
            ask_data = [(entry["ask_price"], entry["ask_size"]) for entry in unit_data]
            return self.orderbook_common_precessing(
                bid_data=bid_data,
                ask_data=ask_data,
                timestamp=record_str.get("timestamp", None),
            )


async def upbithumb_orderbook_cp(
    consumer_topic: str,
    c_partition: int,
    group_id: str,
    producer_topic: str,
    p_partition: int,
) -> None:
    """시작점"""
    processor = UpBithumbAsyncOrderbookProcessor(
        consumer_topic=consumer_topic,
        c_partition=c_partition,
        group_id=group_id,
        producer_topic=producer_topic,
        p_partition=p_partition,
    )
    await processor.initialize()
    try:
        await processor.batch_process_messages()
    finally:
        await processor.cleanup()
