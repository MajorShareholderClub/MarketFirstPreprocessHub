from enum import Enum
from dataclasses import dataclass
from typing import Any
from mq.kafka_config import Region
import json
import requests


class DataType(Enum):
    TICKER = "Ticker"
    ORDERBOOK = "Orderbook"


@dataclass
class KafkaS3ConnectorConfig:
    name: str
    region: Region
    data_type: DataType
    bucket_name: str
    flush_size: str
    tasks: str = "4"  # 최대 파티션 수
    bootstrap_servers: str = "kafka1:19092,kafka2:29092,kafka3:39092"
    kafka_connect_url: str = "http://localhost:8083"


class KafkaS3Connector:
    def __init__(self, config: KafkaS3ConnectorConfig):
        self.config = config
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get_topic_name(self) -> str:
        """리전에 따른 토픽 이름 생성"""
        return f"Region{self.config.region.value}_{self.config.data_type.value}Preprocessing"

    def _build_connector_config(self) -> dict[str, Any]:
        """Connector 설정 생성"""
        topic = self._get_topic_name()
        return {
            "name": f"s3-sink-connector-{self.config.region.name.lower()}-{self.config.data_type.value}-{self.config.name}",
            "config": {
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "tasks.max": self.config.tasks,
                "topics": topic,
                "s3.bucket.name": self.config.bucket_name,
                "flush.size": self.config.flush_size,
                # "topics.dir": f"{self.config.typed}/{self.config.name}",
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


def create_all_connectors() -> None:
    """모든 커넥터를 생성하는 함수"""
    regions = [Region.KOREA, Region.ASIA, Region.NE]
    data_types = {
        DataType.TICKER: {"bucket": "ticker-data", "flush_size": "20"},
        DataType.ORDERBOOK: {"bucket": "orderbook-data", "flush_size": "50"},
    }

    # 각 지역에 대해 데이터 타입, 해당 버킷 이름, flush_size 출력
    for region in regions:
        for data_type, settings in data_types.items():
            bucket = settings["bucket"]
            flush_size = settings["flush_size"]
            config = KafkaS3ConnectorConfig(
                name=f"{region}-World-market",
                region=region,
                data_type=data_type,
                bucket_name=bucket,
                flush_size=flush_size,
            )
            connector = KafkaS3Connector(config)
            try:
                result = connector.create_connector()
                print(f"Connector created: {result}")
            except Exception as e:
                print(
                    f"Error creating connector for {region.value} {data_type.value}: {str(e)}"
                )


# 사용 예제
if __name__ == "__main__":
    create_all_connectors()
