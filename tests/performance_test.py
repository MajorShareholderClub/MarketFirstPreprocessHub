import asyncio
import time
import cProfile
import pstats
from memory_profiler import profile
from src.common.common_ticker import BaseAsyncTickerProcessor


class TickerPerformanceTester:
    def __init__(self, processor_class: BaseAsyncTickerProcessor):
        self.processor_class = processor_class
        self.kafka_meta = {
            "consumer_topic": "test_topic",
            "producer_topic": "test_processed",
            "group_id": "test_group",
            "p_partition": 0,
            "c_partition": 0,
        }

    async def generate_test_data(self, count: int = 1000) -> list[dict]:
        """테스트용 데이터 생성"""
        return [
            {
                "region": "Korea",
                "market": "Upbit",
                "symbol": "BTC",
                "data": [
                    '{"opening_price": 97103000, "high_price": 97650000, "low_price": 95334000, "trade_price": 96033000, "prev_closing_price": 97102000, "signed_change_price": -1069000, "signed_change_rate": -0.01100904, "acc_trade_volume_24h": 2067.29648158, "timestamp": 1730618524102}'
                ]
                * 10,
            }
            for _ in range(count)
        ]

    @profile
    async def run_memory_test(self, data_count: int = 1000) -> dict[str, float]:
        """메모리 사용량 테스트"""
        processor = self.processor_class(**self.kafka_meta)
        test_data = await self.generate_test_data(data_count)

        for data in test_data:
            result = processor.data_task_a_crack_ticker(data)

    async def run_speed_test(self, data_count: int = 1000) -> dict[str, float]:
        """처리 속도 테스트"""
        processor = self.processor_class(**self.kafka_meta)
        test_data = await self.generate_test_data(data_count)

        start_time = time.time()
        for data in test_data:
            result = processor.data_task_a_crack_ticker(data)
        end_time = time.time()

        return {
            "total_time": end_time - start_time,
            "avg_time_per_item": (end_time - start_time) / data_count,
            "items_per_second": data_count / (end_time - start_time),
        }

    async def run_profiling(self, data_count: int = 1000) -> pstats.Stats:
        """상세 프로파일링"""
        profiler = cProfile.Profile()
        processor = self.processor_class(**self.kafka_meta)
        test_data = await self.generate_test_data(data_count)

        profiler.enable()
        for data in test_data:
            result = processor.data_task_a_crack_ticker(data)
        profiler.disable()

        stats = pstats.Stats(profiler).sort_stats("cumulative")
        return stats
