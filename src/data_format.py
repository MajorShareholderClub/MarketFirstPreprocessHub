"""데이터 전처리 포맷 설계"""

from __future__ import annotations
from typing import Any
from decimal import Decimal, ROUND_HALF_UP
from pydantic import BaseModel, field_validator, Field
from mq.types import ExchangeResponseData


class PriceData(BaseModel):
    """코인 현재 가격 데이터"""

    opening_price: Decimal | None = Field(default=None, description="코인 시작가")
    trade_price: Decimal | None = Field(default=None, description="코인 시장가")
    max_price: Decimal | None = Field(default=None, description="코인 고가")
    min_price: Decimal | None = Field(default=None, description="코인저가")
    prev_closing_price: Decimal | None = Field(default=None, description="코인 종가")
    acc_trade_volume_24h: Decimal | None = Field(
        default=None, description="24시간 거래량"
    )

    @field_validator("*", mode="before")
    @classmethod
    def round_three_place_adjust(cls, value: float) -> Decimal | None:
        """모든 필드에 대한 값을 소수점 셋째 자리로 반올림"""
        if isinstance(value, (float, int, str, Decimal)):
            return Decimal(value).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


class MarketData(BaseModel):
    """Coin price data schema
    Returns:
        >>>  {
                "market": "upbit-BTC",
                "timestamp": 1232355.0,
                "coin_symbol": "BTC",
                "data": {
                    "opening_price": 38761000.0,
                    "trade_price": 38100000.0
                    "high_price": 38828000.0,
                    "low_price": 38470000.0,
                    "prev_closing_price": 38742000.0,
                    "acc_trade_volume_24h": 2754.0481778
                }
            }
    """

    data: PriceData

    @staticmethod
    def _key_and_get_first_value(dictionary: dict, key: str) -> int | bool:
        # 딕셔너리 또는 리스트 확인
        if not isinstance(dictionary, dict):
            return None

        # key가 존재하는지 확인
        if key not in dictionary or dictionary[key] in (None, ""):
            return -1

        value: int | list | str = dictionary[key]
        # match 표현식을 사용하여 값에 따른 처리
        match value:
            # value가 리스트이고 리스트가 비어 있지 않은 경우
            case list() if len(value) > 0:
                return value[0]
            case _:  # 그 외의 경우 (리스트가 아니거나 빈 리스트)
                return value

    @classmethod
    def _create_price_data(cls, api: dict[str, Any], data: list[str]) -> PriceData:
        """API 데이터에서 PriceData 객체 생성"""
        # "None" 값을 -1로 바꾸기 위해, data 리스트의 요소를 안전하게 확인
        if "None" in data:
            data[4] = -1

        filtered = MarketData._key_and_get_first_value
        return PriceData(
            opening_price=filtered(api, data[0]),
            max_price=filtered(api, data[1]),
            min_price=filtered(api, data[2]),
            trade_price=filtered(api, data[3]),
            prev_closing_price=filtered(api, data[4]),  # -1로 기본값 설정
            acc_trade_volume_24h=filtered(api, data[5]),
        )

    @classmethod
    def from_api(
        cls,
        api: ExchangeResponseData,
        data: list[str],
    ) -> MarketData:
        """API 데이터로부터 CoinMarketData 생성"""
        price_data: PriceData = cls._create_price_data(api=api, data=data)
        return cls(data=price_data)


class CoinMarketCollection(BaseModel):
    market: str
    coin_symbol: str
    timestamp: float | int
    data: list[dict]
