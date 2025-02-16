from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Optional


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    db: int = 0


class MongoConfig(BaseModel):
    host: str = "localhost"
    port: int = 27017
    username: Optional[str] = None
    password: Optional[str] = None


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
