from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Optional


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None


class MongoConfig(BaseModel):
    host: str = "localhost"
    port: int = 27017
    username: Optional[str] = None
    password: Optional[str] = None
    uri: Optional[str] = None

    database: str = "condenses"
    collection: str = "miner_stats"

    def get_uri(self) -> str:
        if self.uri:
            return self.uri
        if self.username and self.password:
            return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"mongodb://{self.host}:{self.port}"


class RateLimiterConfig(BaseModel):
    limit: int = 512
    interval: int = 60


class MinerManagerConfig(BaseModel):
    score_ema: float = 0.95


class ServerConfig(BaseModel):
    port: int = 9101
    host: str = "0.0.0.0"


class Settings(BaseSettings):
    redis: RedisConfig = RedisConfig()
    mongo: MongoConfig = MongoConfig()
    rate_limiter: RateLimiterConfig = RateLimiterConfig()
    miner_manager: MinerManagerConfig = MinerManagerConfig()
    server: ServerConfig = ServerConfig()

    class Config:
        env_nested_delimiter = "__"


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