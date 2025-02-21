from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Optional


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None

class RateLimiterConfig(BaseModel):
    limit: int = 512
    interval: int = 60


class MinerManagerConfig(BaseModel):
    score_ema: float = 0.95

class SQLiteConfig(BaseModel):
    path: str = "miner_stats.db"


class Settings(BaseSettings):
    redis: RedisConfig = RedisConfig()
    rate_limiter: RateLimiterConfig = RateLimiterConfig()
    miner_manager: MinerManagerConfig = MinerManagerConfig()
    sqlite: SQLiteConfig = SQLiteConfig()
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