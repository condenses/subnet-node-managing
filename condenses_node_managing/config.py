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
    path: str = "miner_stats.db"


class RestfulBittensorConfig(BaseModel):
    base_url: str = "http://127.0.0.1:9103"

class PostgresConfig(BaseModel):
    model_config = {"extra": "ignore"}
    host: str = "localhost"
    port: int = 5432
    database: str = "condenses"
    username: str = "postgres"
    password: str = "postgres"
    uri: str = ""

    def get_uri(self):
        if self.uri:
            return self.uri
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"



class Settings(BaseSettings):
    redis: RedisConfig = RedisConfig()
    rate_limiter: RateLimiterConfig = RateLimiterConfig()
    miner_manager: MinerManagerConfig = MinerManagerConfig()
    sqlite: SQLiteConfig = SQLiteConfig()
    restful_bittensor: RestfulBittensorConfig = RestfulBittensorConfig()
    postgres: PostgresConfig = PostgresConfig()
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