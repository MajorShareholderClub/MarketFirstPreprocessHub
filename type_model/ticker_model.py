from __future__ import annotations
from typing import Any, ClassVar
from decimal import Decimal, ROUND_HALF_UP
from pydantic import BaseModel
from datetime import datetime, timedelta

from type_model import ExchangeResponseData
import FinanceDataReader as fdr


class PriceData(BaseModel):
    """코인 현재 가격 데이터"""

    # slots 사용으로 메모리 사용량 감소
    opening_price: Decimal | None
    trade_price: Decimal | None
    max_price: Decimal | None
    min_price: Decimal | None
    prev_closing_price: Decimal | None
    acc_trade_volume_24h: Decimal | None
    signed_change_price: Decimal | None
    signed_change_rate: Decimal | None


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

    _exchange_rate: ClassVar[Decimal | None] = None
    _last_exchange_update: ClassVar[datetime | None] = None
    _EXCHANGE_UPDATE_INTERVAL: ClassVar[timedelta] = timedelta(hours=1)
    data: PriceData

    # 자주 사용되는 Decimal 상수 캐싱
    _DECIMAL_ZERO: ClassVar[Decimal] = Decimal("0.0")
    _DECIMAL_ONE: ClassVar[Decimal] = Decimal("1.000")
    _DECIMAL_HUNDRED: ClassVar[Decimal] = Decimal("100.0")

    @classmethod
    def _update_exchange_rate(cls) -> None:
        """환율 정보 업데이트"""
        current_time = datetime.now()

        # 마지막 업데이트로부터 1시간이 지났거나, 환율 정보가 없는 경우에만 업데이트
        if (
            cls._last_exchange_update is None
            or current_time - cls._last_exchange_update > cls._EXCHANGE_UPDATE_INTERVAL
        ):
            try:
                exchange_rate_df = fdr.DataReader("USD/KRW")
                cls._exchange_rate = Decimal(
                    str(round(exchange_rate_df["Close"].iloc[-1], 3))
                )
                cls._last_exchange_update = current_time
            except Exception as e:
                # 환율 정보 가져오기 실패시 기본값 사용
                if cls._exchange_rate is None:
                    cls._exchange_rate = Decimal("1300.000")  # 기본값
                print(f"환율 정보 업데이트 실패: {e}")

    @staticmethod
    def _key_and_get_first_value(dictionary: dict, key: str) -> Decimal | int:
        """딕셔너리에서 키에 해당하는 첫 번째 값을 안전하게 추출

        Args:
            dictionary: 검색할 딕셔너리
            key: 찾을 키

        Returns:
            Decimal | int: 찾은 값 또는 -1 (에러/없음)
        """
        if key not in dictionary or dictionary[key] in (None, ""):
            return -1

        value = dictionary[key]
        try:
            return (
                Decimal(value[0])
                if isinstance(value, list) and value
                else Decimal(str(value))
            )
        except (TypeError, ValueError):
            return -1

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
                    # yesterday_last
                    # last - yesterday_last
                    prev_price = MarketData._key_and_get_first_value(api, data[4])
                case "korbit":
                    # open
                    # last - open
                    prev_price = MarketData._key_and_get_first_value(api, data[0])
                case "okx":
                    # open24h
                    # last - open24h
                    prev_price = MarketData._key_and_get_first_value(api, data[0])
                case "gateio":
                    # highest_bid
                    # last - last24h (highest_bid를 last24h로 사용)
                    prev_price = MarketData._key_and_get_first_value(api, data[4])
                case "bybit":
                    # prevPrice24h
                    # lastPrice - prevPrice24h
                    prev_price = MarketData._key_and_get_first_value(api, data[4])
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

            # 현재가(last)
            trade_price = MarketData._key_and_get_first_value(api, data[1])

            # 거래소별 계산 로직
            match exchange.lower():
                case "coinone":
                    # yesterday_last
                    # ((last - yesterday_last) / yesterday_last) * 100
                    base_price = MarketData._key_and_get_first_value(api, data[4])
                case "korbit":
                    # opend
                    # ((last - open) / open) * 100
                    base_price = MarketData._key_and_get_first_value(api, data[0])
                case "okx":
                    # open24h
                    # ((last - open24h) / open24h) * 100
                    base_price = MarketData._key_and_get_first_value(api, data[0])
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
            return Decimal("0.0")

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
        return PriceData(
            opening_price=filtered(api, data[0]) / exchange_rate,
            trade_price=filtered(api, data[1]) / exchange_rate,
            max_price=filtered(api, data[2]) / exchange_rate,
            min_price=filtered(api, data[3]) / exchange_rate,
            prev_closing_price=filtered(api, data[4]) / exchange_rate,
            acc_trade_volume_24h=filtered(api, data[5]),  # 거래량은 환율 적용 불필요
            signed_change_price=cls._signed_change_price(api, data, exchange)
            / exchange_rate,
            signed_change_rate=cls._signed_change_rate(api, data, exchange),
        )

    @classmethod
    def from_api(
        cls, api: ExchangeResponseData, data: list[str], exchange: str = ""
    ) -> MarketData:
        """메모리 효율적인 API 데이터 처리"""
        cls._update_exchange_rate()

        exchange = exchange.lower()  # 한 번만 변환
        is_korean_exchange = exchange in {
            "upbit",
            "bithumb",
            "korbit",
            "coinone",
        }  # set 사용
        exchange_rate = cls._exchange_rate if is_korean_exchange else cls._DECIMAL_ONE

        price_data = cls._create_price_data(
            api=api, data=data, exchange=exchange, exchange_rate=exchange_rate
        )

        return cls(data=price_data)


class CoinMarketCollection(BaseModel):
    """코인 마켓 데이터 컬렉션"""

    class Config:
        validate_assignment = False
        arbitrary_types_allowed = True

    region: str
    market: str
    coin_symbol: str
    timestamp: float | int
    data: list[dict]
