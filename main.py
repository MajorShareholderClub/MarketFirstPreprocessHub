import asyncio
from src.asia.orderbook.okx_orderbook import okx_orderbook_cp
from src.asia.orderbook.bybit_orderbook import bybit_orderbook_cp
from src.asia.orderbook.gateio_orderbook import gateio_orderbook_cp

from src.korea.orderbook.upbithumb_orderbook import upbithumb_orderbook_cp
from src.korea.orderbook.onekorbit_orderbook import onekorbit_orderbook_cp


# asyncio.run(upbithumb_orderbook_cp(consumer_topic="koreaSocketDataInBTC", partition=1))
asyncio.run(onekorbit_orderbook_cp(consumer_topic="koreaSocketDataInBTC", partition=0))
