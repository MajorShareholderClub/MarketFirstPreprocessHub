from abc import abstractmethod

import logging
import json
from datetime import datetime, timezone
from src.common.common_consumer import CommonConsumerSettingProcessor

from mq.types import OrderBookData, ProcessedOrderBook, OrderEntry
from mq.exception import handle_processing_errors
from decimal import Decimal
import FinanceDataReader as fdr

exchange_rate = fdr.DataReader("USD/KRW")


class BaseAsyncOrderbookProcessor(CommonConsumerSettingProcessor):
    """비동기 주문서 데이터를 처리하는 클래스."""

    def process_order_data(
        self, price_volume_data: list[list[str, str]]
    ) -> tuple[float, float]:
        """주문 데이터 처리"""
        if not price_volume_data:
            return 0.0, 0.0

        total = 0.0
        prices = []

        for price_str, volume_str in price_volume_data:
            price = float(price_str)
            total += float(volume_str)
            prices.append(price)

        return total, max(prices) if prices else 0.0

    def orderbook_common_processing(
        self,
        bid_data: list[list[str, str]],
        ask_data: list[list[str, str]],
        symbol: str,
        market: str,
        region: str,
    ) -> ProcessedOrderBook:
        """오더북 공통 처리"""
        bid_total, highest_bid = self.process_order_data(bid_data)
        ask_total, _ = self.process_order_data(ask_data)
        lowest_ask: float = (
            min(float(price) for price, _ in ask_data) if ask_data else 0.0
        )

        # 한국 지역이면 원화를 달러로 변환
        if region == "korea":
            exchange_krw_usd = Decimal(round(exchange_rate["Close"].iloc[-1], 0))
            highest_bid = float(Decimal(highest_bid) / exchange_krw_usd)
            lowest_ask = float(Decimal(lowest_ask) / exchange_krw_usd)

        return ProcessedOrderBook(
            region=region,
            market=market,
            symbol=symbol,
            highest_bid=highest_bid,
            lowest_ask=lowest_ask,
            total_bid_volume=bid_total,
            total_ask_volume=ask_total,
            timestamp=str(datetime.now(timezone.utc)),
        )

    @abstractmethod
    def order_preprocessing(
        self,
        item: OrderEntry,
        symbol: str,
        market: str,
        region: str,
    ) -> ProcessedOrderBook: ...

    @handle_processing_errors
    def calculate_total_bid_ask(self, orderbook: OrderBookData) -> ProcessedOrderBook:
        """주문서 데이터를 기반으로 주문서 메트릭 계산."""
        region: str = orderbook["region"]
        market: str = orderbook["market"]
        symbol: str = orderbook["symbol"]

        for record_str in orderbook["data"]:
            if isinstance(record_str, dict):
                return self.order_preprocessing(
                    item=record_str, symbol=symbol, market=market, region=region
                )

            record: OrderEntry = json.loads(record_str)
            return self.order_preprocessing(
                item=record, symbol=symbol, market=market, region=region
            )
