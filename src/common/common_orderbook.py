from abc import abstractmethod

import json
from src.common.common_consumer import CommonConsumerSettingProcessor

from mq.types import OrderBookData, ProcessedOrderBook, OrderEntry
from mq.exception import handle_processing_errors


class BaseAsyncOrderbookPrepcessor(CommonConsumerSettingProcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

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
