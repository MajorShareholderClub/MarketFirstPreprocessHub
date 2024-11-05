import configparser
from pathlib import Path

path = Path(__file__).parent
parser = configparser.ConfigParser()
parser.read(f"{path}/_setting.conf")

# Kafka 기본 설정
BOOTSTRAPSERVER = parser.get("KAFKA", "BOOTSTRAPSERVER")
KAFKA_CONNECT_URL = parser.get("KAFKA", "KAFKA_CONNECT_URL")
MINIO_URL = parser.get("KAFKA", "MINIO_URL")

# Topic 설정
TOPIC_TICKER = parser.get("TOPIC", "TICKER")
TOPIC_ORDERBOOK = parser.get("TOPIC", "ORDERBOOK")
TOPIC_ARBITRAGE = parser.get("TOPIC", "ARBITRAGE")
TOPIC_TIME_METRICS = parser.get("TOPIC", "TIME_METRICS")
TOPIC_ORDERBOOK_ALL_REGION = parser.get("TOPIC", "ORDERBOOK_ALL_REGION")
TOPIC_ORDERBOOK_REGION = parser.get("TOPIC", "ORDERBOOK_REGION")
TOPIC_TICKER_SIGNAL = parser.get("TOPIC", "TICKER_SIGNAL")
TOPIC_TICKER_PROCESSED = parser.get("TOPIC", "TICKER_PROCESSED")
