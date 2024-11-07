import requests
import json


def create_mirror_maker_connector(
    connector_name: str = "mirror-maker-connector",
) -> dict:
    """
    MirrorMaker 2.0 커넥터를 생성하는 함수

    Args:
        connector_name (str): 커넥터 이름

    Returns:
        dict: API 응답 결과
    """

    # Kafka Connect REST API 엔드포인트
    CONNECT_URL = "http://localhost:8083/connectors"

    # 커넥터 설정
    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
            "tasks.max": "25",
            # Source cluster 설정 (prod)
            "source.cluster.alias": "prod",
            "source.cluster.bootstrap.servers": "kafka1:19092,kafka2:29092,kafka3:39092",
            # Target cluster 설정 (dev) - 포트 번호 수정
            "target.cluster.alias": "dev",
            "target.cluster.bootstrap.servers": "kafka1-dev:19192,kafka2-dev:29192,kafka3-dev:39192",
            # 모든 토픽 복제 설정
            "topics": ".*",
            "topics.exclude": "kafka-configs,kafka-offsets,kafka-status,_schemas,__consumer_offsets,__transaction_state",
            # 나머지 설정들
            "sync.topic.configs.enabled": "true",
            "sync.topic.acls.enabled": "false",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "replication.factor": 3,
            "offset.lag.max": 100,
            "refresh.topics.interval.seconds": 10,
            "topic.creation.enable": "true",
            "topic.creation.default.replication.factor": 3,
            "topic.creation.default.partitions": 5,
            "group.id": "mirror-maker-group",
            "security.protocol": "PLAINTEXT",
        },
    }

    try:
        # POST 요청으로 커넥터 생성
        response = requests.post(
            CONNECT_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config),
        )

        # 응답 확인
        response.raise_for_status()

        print(f"MirrorMaker 커넥터 '{connector_name}' 생성 성공")
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"MirrorMaker 커넥터 생성 실패: {str(e)}")
        if hasattr(e.response, "text"):
            print(f"에러 상세: {e.response.text}")
        raise


def delete_mirror_maker_connector(
    connector_name: str = "mirror-maker-connector",
) -> bool:
    """
    MirrorMaker 2.0 커넥터를 삭제하는 함수

    Args:
        connector_name (str): 삭제할 커넥터 이름

    Returns:
        bool: 삭제 성공 여부
    """

    CONNECT_URL = f"http://localhost:8083/connectors/{connector_name}"

    try:
        response = requests.delete(CONNECT_URL)
        response.raise_for_status()

        print(f"MirrorMaker 커넥터 '{connector_name}' 삭제 성공")
        return True

    except requests.exceptions.RequestException as e:
        print(f"MirrorMaker 커넥터 삭제 실패: {str(e)}")
        return False


def get_connector_status(connector_name: str = "mirror-maker-connector") -> dict:
    """
    MirrorMaker 2.0 커넥터의 상태를 확인하는 함수

    Args:
        connector_name (str): 상태를 확인할 커넥터 이름

    Returns:
        dict: 커넥터 상태 정보
    """

    CONNECT_URL = f"http://localhost:8083/connectors/{connector_name}/status"

    try:
        response = requests.get(CONNECT_URL)
        response.raise_for_status()

        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"커넥터 상태 확인 실패: {str(e)}")
        return None


# 커넥터 생성
result = create_mirror_maker_connector()

# 커넥터 상태 확인
status = get_connector_status()
print(json.dumps(status, indent=2))

# 필요한 경우 커넥터 삭제
# delete_mirror_maker_connector()
