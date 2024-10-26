from src.common.common_consumer import CommonConsumerSettingProcessor
from src.meta_create import consumer_metadata
import asyncio

# Kafka 메타데이터를 가져옵니다.
data = consumer_metadata(False)


async def _create_task() -> None:
    # CommonConsumerSettingProcessor 인스턴스를 생성합니다.
    process = CommonConsumerSettingProcessor(target="orderbook", kafka_meta_data=data)

    # 초기화 작업을 수행합니다.
    await process.initialize()

    try:
        # 배치 처리를 시작합니다.
        await process.batch_process_messages()
    finally:
        # 필요한 종료 작업 수행
        await process.cleanup()


# 비동기 실행
asyncio.run(_create_task())
