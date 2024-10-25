from abc import abstractmethod

import json
from datetime import datetime, timezone
from src.common.common_consumer import CommonConsumerSettingProcessor

from mq.types import OrderBookData, ProcessedOrderBook, OrderEntry
from mq.exception import handle_processing_errors


class BaseAsyncOrderbookPrepcessor(CommonConsumerSettingProcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def process_order_data(
        self, price_volume_data: list[tuple[str, str]]
    ) -> tuple[float, float]:
        """주문 데이터 처리"""
        if not price_volume_data:
            return 0.0, None

        total = 0.0
        prices = []

        for price_str, volume_str in price_volume_data:
            price = float(price_str)
            total += float(volume_str)
            prices.append(price)

        return total, max(prices) if prices else None

    def orderbook_common_processing(self, bid_data, ask_data) -> ProcessedOrderBook:
        """오더북 공통 처리"""
        bid_total, highest_bid = self.process_order_data(bid_data)
        ask_total, _ = self.process_order_data(ask_data)

        lowest_ask = min(float(price) for price, _ in ask_data) if ask_data else None
        spread = (
            (lowest_ask - highest_bid)
            if (highest_bid is not None and lowest_ask is not None)
            else None
        )

        return ProcessedOrderBook(
            highest_bid=highest_bid,
            lowest_ask=lowest_ask,
            spread=spread,
            total_bid_volume=bid_total,
            total_ask_volume=ask_total,
            timestamp=str(datetime.now(timezone.utc)),
        )

    @abstractmethod
    def order_preprocessing(item: OrderEntry) -> ProcessedOrderBook: ...

    @handle_processing_errors
    def calculate_total_bid_ask(self, orderbook: OrderBookData) -> ProcessedOrderBook:
        """주문서 데이터를 기반으로 주문서 메트릭 계산.

        Args:
            orderbook (OrderBookData): 주문서 데이터

        Returns:
            ProcessedOrderBook: 처리된 주문서 데이터
        """
        for record_str in orderbook["data"]:
            if isinstance(record_str, dict):
                return self.order_preprocessing(item=record_str)

            record: OrderEntry = json.loads(record_str)
            return self.order_preprocessing(item=record)
