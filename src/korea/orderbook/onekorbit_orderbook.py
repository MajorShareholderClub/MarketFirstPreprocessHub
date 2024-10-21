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
            recode: OrderEntry = json.loads(record_str)
            ask_data = [price_amount(item=item) for item in recode["data"]["asks"]]
            bid_data = [price_amount(item=item) for item in recode["data"]["bids"]]

            return self.orderbook_common_precessing(
                bid_data=bid_data,
                ask_data=ask_data,
                timestamp=recode.get("timestamp", None),
            )


async def onekorbit_orderbook_cp(consumer_topic: str, partition: int) -> None:
    """시작점"""
    processor = CoinoneKorbitAsyncOrderbookProcessor(
        consumer_topic=consumer_topic, partition=partition
    )
    await processor.initialize()
    try:
        await processor.batch_process_messages()
    finally:
        await processor.cleanup()
