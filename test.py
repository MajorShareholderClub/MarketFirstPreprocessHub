from src.config import create_exchange_configs, Region

# 설정 생성
TICKER_CONFIGS = create_exchange_configs(is_ticker=True)
ORDERBOOK_CONFIGS = create_exchange_configs(is_ticker=False)

# 사용 예시:
if __name__ == "__main__":
    # Ticker 설정 접근
    korea_upbit_ticker = ORDERBOOK_CONFIGS[Region.KOREA].values()
    for i in korea_upbit_ticker:
        print(i["kafka_config"])
    # asia_okx_ticker = TICKER_CONFIGS[Region.ASIA]["okx"]

    # # Orderbook 설정 접근
    # ne_binance_orderbook = ORDERBOOK_CONFIGS[Region.NE]["binance"]
    # korea_bithumb_orderbook = ORDERBOOK_CONFIGS[Region.KOREA]["bithumb"]
    # print(korea_upbit_ticker)
