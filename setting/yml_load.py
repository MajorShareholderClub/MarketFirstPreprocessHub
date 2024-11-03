import yaml
from functools import lru_cache


class BaseProcessorConfig:
    """공통 설정 관리 클래스"""

    _config_instance = {}

    def __new__(cls, *args, **kwargs) -> "BaseProcessorConfig":
        config_path = kwargs.get("config_path", args[1] if len(args) > 1 else None)
        if config_path not in cls._config_instance:
            instance = super().__new__(cls)
            cls._config_instance[config_path] = instance
            return instance
        return cls._config_instance[config_path]

    def __init__(self, market: str, config_path: str) -> None:
        if hasattr(self, "config"):
            return
        self.config_path = config_path
        self._load_config()

    @lru_cache(maxsize=30)
    def _load_config(self) -> None:
        """설정 파일 로드"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            self.config = {}

    @property
    def producer_topic(self) -> str:
        """프로듀서 토픽 설정값 반환"""
        raise NotImplementedError("이 메서드는 서브클래스에서 구현되어야 합니다.")

    @property
    def group_id(self) -> str:
        """그룹 ID 설정값 반환"""
        raise NotImplementedError("이 메서드는 서브클래스에서 구현되어야 합니다.")


class TickerProcessorConfig(BaseProcessorConfig):
    """티커 처리기 설정 관리 클래스"""

    def __init__(self, market: str, config_path: str = "setting/ticker.yml") -> None:
        self.market = market
        super().__init__(market, config_path)

    @property
    def producer_topic(self) -> str:
        """프로듀서 토픽 설정값 반환"""
        return self.config.get("producer_topic", "TickerPreprocessing")

    @property
    def group_id(self) -> str:
        """그룹 ID 설정값 반환"""
        return self.config.get("group_id", "ticker")

    @property
    def ticker_parameter(self) -> list[str]:
        """티커 파라미터 반환"""
        return self.config.get(self.market.lower(), {}).get("parameter", "")
