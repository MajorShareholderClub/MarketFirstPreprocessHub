from __future__ import annotations

import json
from src.common_consumer import CommoneConsumerSettingProcesser

from mq.types import OrderBookData, ProcessedOrderBook, OrderEntry
from mq.exception import (
    handle_processing_errors,
)


class GateIOAsyncOrderbookProcessor(CommoneConsumerSettingProcesser):
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
            bid_data = record["result"]["bids"]
            ask_data = record["result"]["asks"]
            return self.orderbook_common_precessing(
                bid_data=bid_data,
                ask_data=ask_data,
                timestamp=record.get("time", None),
            )


async def gateio_orderbook_cp(consumer_topic: str, partition: int) -> None:
    """시작점"""
    processor = GateIOAsyncOrderbookProcessor(
        consumer_topic=consumer_topic, partition=partition
    )
    await processor.initialize()
    try:
        await processor.batch_process_messages()
    finally:
        await processor.cleanup()
