from src.meta_create import consumer_metadata


data = consumer_metadata()
print(data)

# # 함수 호출
# save_to_redis("orderbook", data["orderbook"])
# save_to_redis("ticker", data["ticker"])
