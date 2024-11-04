import asyncio
from concurrent.futures import ThreadPoolExecutor
from order_ticker import RegionTickerOrderbookProcessor


async def run_coin_websocket(
    connection_class, consumer_type: str, is_ticker: bool
) -> None:
    """지정된 웹소켓 클라이언트 클래스와 심볼을 사용하여 비동기 함수 실행."""
    websocket_client = connection_class(consumer_type, is_ticker)
    await websocket_client.process_ticker()


async def coin_present_websocket(connection_class) -> None:
    """두 개의 코인 웹소켓을 동시에 실행."""
    loop = asyncio.get_running_loop()

    # 스레드 풀을 생성
    with ThreadPoolExecutor(max_workers=2) as executor:
        # run_in_executor 사용하여 비동기 작업 실행
        order_task = loop.run_in_executor(
            executor,
            lambda: asyncio.run(
                run_coin_websocket(connection_class, "orderbook", False)
            ),
        )
        ticker_task = loop.run_in_executor(
            executor,
            lambda: asyncio.run(run_coin_websocket(connection_class, "ticker", True)),
        )

        # 두 작업이 완료될 때까지 기다림
        await asyncio.gather(
            order_task,
            ticker_task,
            return_exceptions=False,
        )


if __name__ == "__main__":
    asyncio.run(coin_present_websocket(RegionTickerOrderbookProcessor))
