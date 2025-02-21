from .serving_counter import RateLimiter
from .config import CONFIG
from pydantic import BaseModel
import numpy as np
import redis
from loguru import logger
from sqlalchemy import create_engine, Column, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from restful_bittensor.client import AsyncRestfulBittensor
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from contextlib import asynccontextmanager

Base = declarative_base()

class MinerStatsModel(Base):
    __tablename__ = 'miner_stats'
    uid = Column(Integer, primary_key=True)
    score = Column(Float, default=0.0)

class MinerStats(BaseModel):
    uid: int
    score: float = 0.0

class MinerOrchestrator:
    def __init__(self):
        logger.info("Initializing MinerOrchestrator")
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{CONFIG.sqlite.path}")
        self.SessionMaker = sessionmaker(bind=self.engine, class_=AsyncSession)
        self.redis = redis.Redis(
            host=CONFIG.redis.host,
            port=CONFIG.redis.port,
            db=CONFIG.redis.db,
            username=CONFIG.redis.username,
            password=CONFIG.redis.password,
            decode_responses=True,
        )
        self.miner_ids = list(range(0, 256))
        self.miner_keys = [f"miner:{uid}" for uid in self.miner_ids]
        self.score_ema = CONFIG.miner_manager.score_ema
        self.restful_bittensor_client = AsyncRestfulBittensor(
            base_url=CONFIG.restful_bittensor.base_url,
        )
        self.limiter = RateLimiter(
            limit=None,
            interval=CONFIG.rate_limiter.interval,
            redis_client=self.redis,
        )
        asyncio.create_task(self.sync_rate_limit())
        if not self.check_connection():
            logger.error("Failed to connect to SQLite or Redis")
            raise ConnectionError("Failed to connect to SQLite")
        logger.info("MinerOrchestrator initialized successfully")

    def _init_db(self):
        """Initialize SQLite database and create tables if they don't exist"""
        Base.metadata.create_all(self.engine)

    async def sync_rate_limit(self):
        while True:
            logger.info("Syncing rate limit")
            normalized_stake = await self.restful_bittensor_client.get_normalized_stake()
            logger.info(f"Normalized stake: {normalized_stake}")
            rate_limit = max(CONFIG.rate_limiter.limit * normalized_stake, 2)
            logger.info(f"Rate limit: {rate_limit}")
            self.limiter.limit = rate_limit
            await asyncio.sleep(600)

    @asynccontextmanager
    async def _get_db(self):
        """Async context manager for database sessions"""
        session = await self.SessionMaker()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def get_stats(self, uid: int) -> MinerStats:
        logger.debug(f"Getting stats for miner {uid}")
        async with self._get_db() as session:
            stats = await session.query(MinerStatsModel).filter_by(uid=uid).first()
            logger.info(f"Stats: {stats}")
            if not stats:
                logger.info(f"No stats found for miner {uid}, creating new entry")
                stats = MinerStatsModel(uid=uid, score=0.0)
                await session.add(stats)
                await session.commit()
                return MinerStats(uid=stats.uid, score=stats.score)

            return MinerStats(uid=stats.uid, score=stats.score)

    async def update_stats(self, uid: int, new_score: float) -> bool:
        logger.debug(f"Updating stats for miner {uid} with new score {new_score}")
        stats = await self.get_stats(uid)
        stats.score = stats.score * self.score_ema + new_score * (1 - self.score_ema)
        logger.info(f"Stats: {stats}")
        async with self._get_db() as session:
            db_stats = await session.query(MinerStatsModel).filter_by(uid=uid).first()
            db_stats.score = stats.score
            logger.debug(f"Updated stats for miner {uid}, new score: {stats.score}")
            return True

    async def consume_rate_limits(
        self,
        uid: int = None,
        top_fraction: float = 1.0,
        count: int = 1,
        acceptable_consumed_rate: float = 1.0,
    ) -> list[int]:
        """
        Check and consume rate limits for miners.

        Args:
            uid: Target miner ID. If None, selects from all miners.
            top_fraction: Proportion of top miners to consider.
            count: Number of miners to select when uid is None.

        Returns:
            List of miner IDs that passed rate limiting.
        """
        if not self.limiter.limit:
            raise ValueError("Rate limit is not set")
        logger.debug(
            f"Checking rate limits - uid: {uid}, top_fraction: {top_fraction}, count: {count}"
        )
        if uid:
            result = (
                [uid]
                if self.limiter.consume(
                    self.miner_keys[uid],
                    acceptable_consumed_rate=acceptable_consumed_rate,
                )
                else []
            )
            logger.debug(
                f"Rate limit check for miner {uid}: {'passed' if result else 'failed'}"
            )
            return result

        remaining_limits = [self.limiter.get_remaining(key) for key in self.miner_keys]
        total = sum(remaining_limits)
        probabilities = [limit / total for limit in remaining_limits]

        ranked_miners = sorted(
            zip(self.miner_ids, probabilities), key=lambda x: x[1], reverse=True
        )[: int(len(self.miner_ids) * top_fraction)]

        selected = np.random.choice(
            [miner_id for miner_id, _ in ranked_miners],
            size=min(count, len(ranked_miners)),
            replace=False,
            p=[prob for _, prob in ranked_miners],
        )

        result = [
            miner_id
            for miner_id in selected
            if self.limiter.consume(
                self.miner_keys[miner_id],
                acceptable_consumed_rate=acceptable_consumed_rate,
            )
        ]
        logger.debug(f"Selected miners after rate limit check: {result}")
        return result

    async def get_score_weights(self) -> tuple[list[int], list[float]]:
        logger.debug("Calculating score weights for all miners")
        scores = [await self.get_stats(uid).score for uid in self.miner_ids]
        total = sum(scores)
        normalized_scores = [round(score / total, 3) for score in scores]
        return self.miner_ids, normalized_scores

    async def check_connection(self) -> bool:
        """Check if both SQLite and Redis connections are alive and working."""
        logger.debug("Checking SQLite and Redis connections")
        try:
            # Check SQLite connection
            async with self._get_db() as session:
                await session.query(MinerStatsModel).first()
            # Check Redis connection
            redis_ok = await self.redis.ping()
            logger.debug(f"Connection check - SQLite: True, Redis: {redis_ok}")
            return True and redis_ok
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False
