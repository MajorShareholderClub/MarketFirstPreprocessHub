import sys
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = str(Path(__file__).parent.absolute())
sys.path.insert(0, project_root)

import asyncio
from tests.performance_test import TickerPerformanceTester
from src.ticker.korea_ticker import UpbithumbAsyncTickerProcessor


async def main():
    # 테스트할 프로세서들
    processors = {
        "upbit": UpbithumbAsyncTickerProcessor,
        "bithumb": UpbithumbAsyncTickerProcessor,
    }

    for name, processor_class in processors.items():
        print(f"\n=== {name} 성능 테스트 ===")

        tester = TickerPerformanceTester(processor_class)

        # 속도 테스트
        speed_results = await tester.run_speed_test(1000)
        print(f"속도 테스트 결과:")
        print(f"총 처리 시간: {speed_results['total_time']:.2f}초")
        print(f"항목당 평균 처리 시간: {speed_results['avg_time_per_item']*1000:.2f}ms")
        print(f"초당 처리 항목 수: {speed_results['items_per_second']:.2f}")

        # 메모리 테스트
        print("\n메모리 사용량 테스트 실행 중...")
        await tester.run_memory_test(1000)

        # 상세 프로파일링
        print("\n상세 프로파일링 결과:")
        stats = await tester.run_profiling(1000)
        stats.print_stats(10)  # 상위 10개 결과만 출력


if __name__ == "__main__":
    asyncio.run(main())
