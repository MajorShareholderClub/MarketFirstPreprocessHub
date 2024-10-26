import redis
import json
import logging
from typing import Awaitable

from mq.kafka_config import Region
from src.logger import AsyncLogger
from src.config import create_exchange_configs, TickerClass, OrderbookClass


# Redis 연결
r = redis.Redis(host="localhost", port=6379, db=0)


ProcessClass = TickerClass | OrderbookClass
RegionMetaData = dict[str, list[dict[str, dict[str, str | int | Awaitable[None]]]]]


class RegionMetaDataCreate:
    """주문서 처리 메인 클래스"""

    def __init__(self, kafka_consumer_type: str, is_ticker: bool) -> None:
        self.kafka_consumer_type = kafka_consumer_type
        self.is_ticker = is_ticker
        # 로깅 설정
        self.logger = AsyncLogger(
            "init", folder=self.kafka_consumer_type
        ).log_message_sync

    def process_ticker(self) -> list[dict[str, dict[str, str | int | Awaitable[None]]]]:
        all_tasks = []
        ticker_config = create_exchange_configs(is_ticker=self.is_ticker)
        for region in Region:
            self.logger(logging.INFO, f"{region} 지역 태스크 생성 중")
            all_tasks.extend(ticker_config[region.name.upper()].values())

        return all_tasks

    @staticmethod
    def consumer_topic_list(is_ticker: bool) -> list[str]:
        config = create_exchange_configs(is_ticker=is_ticker)

        tp = config["consumer_topic"]
        return tp


def consumer_metadata(is_ticker: bool) -> RegionMetaData:
    """두 개의 코인 웹소켓을 동시에 실행."""
    data = {
        "consumer_topic": RegionMetaDataCreate.consumer_topic_list(is_ticker=is_ticker),
        "orderbook": RegionMetaDataCreate("orderbook", False).process_ticker(),
        "ticker": RegionMetaDataCreate("ticker", True).process_ticker(),
    }
    return data
    # # 중복 제거를 위해 set에 직접 추가
    # consumer_topics = {d["consumer_topic"] for j in data.values() for d in j}

    # data["consumer_topic"] = list(consumer_topics)
    # print(data)


# 예시 코드입니다.


# def save_to_redis(consum_type: str, data: RegionMetaData):
#     """
#     Redis에 orderbook 데이터를 저장하는 함수.

#     :param orderbook_data: 저장할 orderbook 데이터 리스트.
#     """
#     with r.pipeline() as pipe:
#         for c in data:
#             first = f"{consum_type}:{c['kafka_config']['group_id']}:"
#             second = f"{c['kafka_config']['p_partition']}:"
#             last = f"{c['kafka_config']['p_key']}"
#             key = first + second + last
#             pipe.hmset(
#                 key,
#                 mapping={
#                     "kafka_config": json.dumps(c["kafka_config"]),
#                     "class_address": c["class_address"],
#                 },
#             )
#         pipe.execute()  # 모든 명령을 한 번에 실행
