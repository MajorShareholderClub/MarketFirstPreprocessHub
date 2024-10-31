from enum import Enum
from dataclasses import dataclass
from typing import Any
from mq.kafka_config import Region
import json
import requests


class DataType(Enum):
    TICKER = "Ticker"
    ORDERBOOK = "Orderbook"
    ARBITRAGE = "ArbitrageProcessed"
    TIME_METRICS = "TimeMetricsProcessed"
    ORDERBOOK_ALL_REGION = "OrderbookProcessedAllRegion"
    ORDERBOOK_REGION = "OrderbookProcessedRegion"


@dataclass
class KafkaS3ConnectorConfig:
    name: str
    data_type: DataType
    bucket_name: str
    flush_size: str
    region: Region = None  # region은 선택적으로 변경
    tasks: str = "4"
    bootstrap_servers: str = "kafka1:19092,kafka2:29092,kafka3:39092"
    kafka_connect_url: str = "http://localhost:8083"


class KafkaS3Connector:
    def __init__(self, config: KafkaS3ConnectorConfig) -> None:
        self.config = config
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get_topic_name(self) -> str:
        """토픽 이름 생성 - 리전이 있는 경우와 없는 경우 구분"""
        if self.config.region:
            return f"Region{self.config.region.value}_{self.config.data_type.value}Preprocessing"
        return f"{self.config.data_type.value}Coin"

    def _build_connector_config(self) -> dict[str, Any]:
        """Connector 설정 생성"""
        topic = self._get_topic_name()

        # name 형식 단순화
        name_parts = ["s3-sink", self.config.data_type.value.lower(), self.config.name]
        if self.config.region:
            name_parts.insert(1, self.config.region.name.lower())

        connector_name = "-".join(name_parts)

        return {
            "name": connector_name,
            "config": {
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "tasks.max": self.config.tasks,
                "topics": topic,
                "s3.bucket.name": self.config.bucket_name,
                "flush.size": self.config.flush_size,
                "file.delim": "-",
                "storage.class": "io.confluent.connect.s3.storage.S3Storage",
                "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
                "store.url": "http://minio:9000",  # MinIO 주소
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": False,
                "value.converter.schemas.enable": False,
                "s3.compression.type": "gzip",
                "partitioner.class": "io.confluent.connect.storage.partitioner.DailyPartitioner",
                "auto.offset.reset": "earliest",
                "directory.delim": "/",
                "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'topics'/${topic}",
                "locale": "ko-KR",
                "timezone": "Asia/Seoul",
                "bootstrap.servers": self.config.bootstrap_servers,
            },
        }

    def create_connector(self) -> dict[str, Any]:
        """Connector 생성"""
        try:
            connector_config = self._build_connector_config()
            print(
                f"Creating connector with config: {json.dumps(connector_config, indent=2)}"
            )
            response = requests.post(
                f"{self.config.kafka_connect_url}/connectors",
                headers=self.headers,
                data=json.dumps(connector_config),
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to create connector: {str(e)}"
            print(f"Error: {error_msg}")
            raise


def _create_connector_config(
    data_type: DataType,
    bucket_name: str,
    flush_size: str,
    region: Region = None,
) -> KafkaS3ConnectorConfig:
    """커넥터 설정 생성 헬퍼 함수"""
    return KafkaS3ConnectorConfig(
        name="market",
        data_type=data_type,
        bucket_name=bucket_name,
        flush_size=flush_size,
        region=region,
    )


def create_regional_connectors() -> None:
    """리전별 커넥터 생성 함수"""
    regional_data_types = {
        DataType.TICKER: {"bucket": "ticker-data", "flush_size": "20"},
        DataType.ORDERBOOK: {"bucket": "orderbook-data", "flush_size": "50"},
    }

    for region in [Region.KOREA, Region.ASIA, Region.NE]:
        for data_type, settings in regional_data_types.items():
            config = _create_connector_config(
                data_type=data_type,
                bucket_name=settings["bucket"],
                flush_size=settings["flush_size"],
                region=region,
            )
            _create_single_connector(config)


def create_global_connectors() -> None:
    """글로벌(리전 구분 없는) 커넥터 생성 함수"""
    global_data_types = {
        DataType.ARBITRAGE: {"bucket": "spark-streaming-ticker", "flush_size": "30"},
        DataType.TIME_METRICS: {"bucket": "spark-streaming-ticker", "flush_size": "25"},
        DataType.ORDERBOOK_ALL_REGION: {
            "bucket": "spark-streaming-orderbook",
            "flush_size": "40",
        },
        DataType.ORDERBOOK_REGION: {
            "bucket": "spark-streaming-orderbook",
            "flush_size": "40",
        },
    }

    for data_type, settings in global_data_types.items():
        config = _create_connector_config(
            data_type=data_type,
            bucket_name=settings["bucket"],
            flush_size=settings["flush_size"],
        )
        _create_single_connector(config)


def _create_single_connector(config: KafkaS3ConnectorConfig) -> None:
    """단일 커넥터 생성 헬퍼 함수"""
    connector = KafkaS3Connector(config)
    try:
        result = connector.create_connector()
        print(f"Connector created: {result}")
    except Exception as e:
        region_info = f"region: {config.region.value}, " if config.region else ""
        print(
            f"Error creating connector for {region_info}data_type: {config.data_type.value}: {str(e)}"
        )


def create_all_connectors() -> None:
    """모든 커넥터를 생성하는 메인 함수"""
    create_regional_connectors()
    create_global_connectors()


# 사용 예제
if __name__ == "__main__":
    create_all_connectors()
