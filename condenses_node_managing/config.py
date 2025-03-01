from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Optional


class RedisConfig(BaseModel):
    model_config = {"extra": "ignore"}
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None


class RateLimiterConfig(BaseModel):
    model_config = {"extra": "ignore"}
    limit: int = 512
    interval: int = 60


class MinerManagerConfig(BaseModel):
    model_config = {"extra": "ignore"}
    score_ema: float = 0.95


class SQLiteConfig(BaseModel):
    model_config = {"extra": "ignore"}
    database_path: str = "miner_stats.db"


class SidecarBittensorConfig(BaseModel):
    base_url: str = "http://127.0.0.1:9103"


class TaostatsAPIConfig(BaseModel):
    api_key: str = ""
    ss58_address: str = ""

    def model_post_init(self, __context):
        if self.api_key and not self.ss58_address:
            raise ValueError("If api_key is set, ss58_address must be non-empty")


class Settings(BaseSettings):
    redis: RedisConfig = RedisConfig()
    rate_limiter: RateLimiterConfig = RateLimiterConfig()
    miner_manager: MinerManagerConfig = MinerManagerConfig()
    sqlite: SQLiteConfig = SQLiteConfig()
    sidecar_bittensor: SidecarBittensorConfig = SidecarBittensorConfig()
    taostats_api: TaostatsAPIConfig = TaostatsAPIConfig()
    node_managing_api_key: str = ""

    class Config:
        env_nested_delimiter = "__"
        env_file = ".env"
        extra = "ignore"


CONFIG = Settings()


from rich.console import Console
from rich.panel import Panel

console = Console()
settings_dict = CONFIG.model_dump()

for section, values in settings_dict.items():
    console.print(
        Panel.fit(
            str(values),
            title=f"[bold blue]{section}[/bold blue]",
            border_style="green",
        )
    )
