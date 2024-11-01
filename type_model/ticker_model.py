from __future__ import annotations
from typing import Any
from decimal import Decimal, ROUND_HALF_UP
from pydantic import BaseModel, field_validator, Field
from type_model import ExchangeResponseData
import FinanceDataReader as fdr

# USD/KRW 환율 정보 가져오기
exchange_rate = fdr.DataReader("USD/KRW")


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
    signed_change_price: Decimal | None = Field(
        default=None, description="변화액(당일 종가 - 전일 종가)"
    )
    signed_change_rate: Decimal | None = Field(
        default=None, description="변화율((당일 종가 - 전일 종가) / 전일 종가 * 100)"
    )

    @field_validator("*", mode="before")
    @classmethod
    def round_three_place_adjust(
        cls, value: float, exchange: str = "", exchange_rate: Decimal = Decimal("1")
    ) -> Decimal | None:
        """모든 필드에 대한 값을 소수점 셋째 자리로 반올림하고 한국 거래소에만 환율 적용"""
        if isinstance(value, (float, int, str, Decimal)):
            if exchange.lower() in ["upbit", "bithumb", "korbit", "coinone"]:
                value_in_usd = Decimal(str(value)) / exchange_rate
            else:
                value_in_usd = Decimal(str(value))
            return value_in_usd.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


class MarketData(BaseModel):
    """Coin price data schema
    Returns:
        >>>  {
                "region": "korea",
                "market": "upbit",
                "timestamp": 1232355.0,
                "coin_symbol": "BTC",
                "data": {
                    "opening_price": 38761000.0,
                    "trade_price": 38100000.0
                    "high_price": 38828000.0,
                    "low_price": 38470000.0,
                    "prev_closing_price": 38742000.0,
                    "acc_trade_volume_24h": 2754.0481778,
                    "signed_change_price": -642000.0,
                    "signed_change_rate": -1.66
                }
            }
    """

    data: PriceData

    @staticmethod
    def _key_and_get_first_value(dictionary: dict, key: str) -> int | bool:
        """딕셔너리에서 키에 해당하는 첫 번째 값을 안전하게 추출

        Args:
            dictionary: 검색할 딕셔너리
            key: 찾을 키

        Returns:
            int | bool: 찾은 값 또는 -1 (에러/없음)
        """
        # 딕셔너리 타입 확인
        if not isinstance(dictionary, dict):
            return -1

        # key 존재 여부 및 값 유효성 확인
        if key not in dictionary or dictionary[key] in (None, ""):
            return -1

        value: int | list | str = dictionary[key]
        # match 표현식으로 값 타입에 따른 처리
        match value:
            case list() if len(value) > 0:
                return Decimal(value[0])
            case _:
                return Decimal(value)

    @staticmethod
    def _signed_change_price(
        api: dict[str, Any], data: list[str], exchange: str = ""
    ) -> Decimal | int:
        """변화액 계산 - 거래소별 계산 방식 적용

        Args:
            api: 거래소 원본 데이터
            data: yml 파라미터 리스트
            exchange: 거래소 이름

        Returns:
            Decimal | int: 변화액. 계산 불가능한 경우 -1 반환
        """
        try:
            # 이미 변화액이 있는 경우 (예: upbit, bithumb)
            if len(data) >= 7 and MarketData._key_and_get_first_value(
                api, data[6]
            ) not in (None, "", -1):
                return MarketData._key_and_get_first_value(api, data[6])

            # 거래소별 계산 로직
            trade_price = MarketData._key_and_get_first_value(
                api, data[1]
            )  # 현재가(last)

            match exchange.lower():
                case "coinone":
                    # last - yesterday_last
                    prev_price = MarketData._key_and_get_first_value(
                        api, data[4]
                    )  # yesterday_last
                case "korbit":
                    # last - open
                    prev_price = MarketData._key_and_get_first_value(
                        api, data[0]
                    )  # open
                case "okx":
                    # last - open24h
                    prev_price = MarketData._key_and_get_first_value(
                        api, data[0]
                    )  # open24h
                case "gateio":
                    # last - last24h (highest_bid를 last24h로 사용)
                    prev_price = MarketData._key_and_get_first_value(
                        api, data[4]
                    )  # highest_bid
                case "bybit":
                    # lastPrice - prevPrice24h
                    prev_price = MarketData._key_and_get_first_value(
                        api, data[4]
                    )  # prevPrice24h
                case _:
                    # 기본: 현재가 - 전일 종가
                    prev_price = MarketData._key_and_get_first_value(api, data[4])

            if trade_price in (None, "", -1) or prev_price in (None, "", -1):
                return -1

            change_price = Decimal(str(trade_price)) - Decimal(str(prev_price))
            return change_price.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)

        except (TypeError, ValueError, KeyError):
            return -1

    @staticmethod
    def _signed_change_rate(
        api: dict[str, Any], data: list[str], exchange: str = ""
    ) -> Decimal | int:
        """변화율 계산 - 거래소별 계산 방식 적용

        Args:
            api: 거래소 원본 데이터
            data: yml 파라미터 리스트
            exchange: 거래소 이름

        Returns:
            Decimal | int: 변화율(%). 계산 불가능한 경우 -1 반환
        """
        try:
            # 이미 변화율이 있는 경우 (예: upbit, bithumb, binance)
            if len(data) >= 8 and MarketData._key_and_get_first_value(
                api, data[7]
            ) not in (None, "", -1):
                return MarketData._key_and_get_first_value(api, data[7])

            trade_price = MarketData._key_and_get_first_value(
                api, data[1]
            )  # 현재가(last)

            # 거래소별 계산 로직
            match exchange.lower():
                case "coinone":
                    # ((last - yesterday_last) / yesterday_last) * 100
                    base_price = MarketData._key_and_get_first_value(
                        api, data[4]
                    )  # yesterday_last
                case "korbit":
                    # ((last - open) / open) * 100
                    base_price = MarketData._key_and_get_first_value(
                        api, data[0]
                    )  # open
                case "okx":
                    # ((last - open24h) / open24h) * 100
                    base_price = MarketData._key_and_get_first_value(
                        api, data[0]
                    )  # open24h
                case "gateio" | "bybit" | _:
                    # 기본: ((현재가 - 전일종가) / 전일종가) * 100
                    base_price = MarketData._key_and_get_first_value(api, data[4])

            if trade_price in (None, "", -1) or base_price in (None, "", -1):
                return -1

            change_rate = (
                (Decimal(str(trade_price)) - Decimal(str(base_price)))
                / Decimal(str(base_price))
            ) * Decimal("100")
            return change_rate.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)

        except (TypeError, ValueError, KeyError, ZeroDivisionError):
            return -1

    @classmethod
    def _create_price_data(
        cls,
        api: dict[str, Any],
        data: list[str],
        exchange: str = "",
        exchange_rate: Decimal = Decimal("1"),
    ) -> PriceData:
        """API 데이터에서 PriceData 객체 생성, 한국 거래소에만 환율 적용"""

        filtered = MarketData._key_and_get_first_value
        is_korean_exchange = exchange.lower() in [
            "upbit",
            "bithumb",
            "korbit",
            "coinone",
        ]
        rate = exchange_rate if is_korean_exchange else Decimal("1")

        return PriceData(
            opening_price=filtered(api, data[0]) / rate,
            trade_price=filtered(api, data[1]) / rate,
            max_price=filtered(api, data[2]) / rate,
            min_price=filtered(api, data[3]) / rate,
            prev_closing_price=filtered(api, data[4]) / rate,
            acc_trade_volume_24h=filtered(api, data[5]),  # 거래량은 환율 적용 불필요
            signed_change_price=cls._signed_change_price(api, data, exchange) / rate,
            signed_change_rate=cls._signed_change_rate(api, data, exchange),
        )

    @classmethod
    def from_api(
        cls, api: ExchangeResponseData, data: list[str], exchange: str = ""
    ) -> MarketData:
        """API 데이터로부터 MarketData 생성, 한국 거래소에만 환율 적용"""

        # 환율 가져오기
        exchange_krw_usd = Decimal(round(exchange_rate["Close"].iloc[-1], 0))

        price_data: PriceData = cls._create_price_data(
            api=api, data=data, exchange=exchange, exchange_rate=exchange_krw_usd
        )
        return cls(data=price_data)


class CoinMarketCollection(BaseModel):
    """코인 마켓 데이터 컬렉션"""

    region: str
    market: str
    coin_symbol: str
    timestamp: float | int
    data: list[dict]
