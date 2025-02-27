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
from sidecar_bittensor.client import AsyncRestfulBittensor
import asyncio
from sqlalchemy.sql import text

Base = declarative_base()


class MinerStatsModel(Base):
    __tablename__ = "miner_stats"
    uid = Column(Integer, primary_key=True)
    score = Column(Float, default=0.0)


class MinerStats(BaseModel):
    uid: int
    score: float = 0.0


class MinerOrchestrator:
    def __init__(self):
        logger.info("Initializing MinerOrchestrator")
        self.engine = create_engine(CONFIG.postgres.get_uri())
        self.SessionMaker = sessionmaker(bind=self.engine)
        # Initialize database tables
        self._init_db()

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
        self.sidecar_bittensor_client = AsyncRestfulBittensor(
            base_url=CONFIG.sidecar_bittensor.base_url,
        )
        self.limiter = RateLimiter(
            limit=None,
            interval=CONFIG.rate_limiter.interval,
            redis_client=self.redis,
        )
        if not self.check_connection():
            logger.error("Failed to connect to PostgreSQL or Redis")
            raise ConnectionError("Failed to connect to PostgreSQL")
        logger.info("MinerOrchestrator initialized successfully")

    def _init_db(self):
        """Initialize PostgreSQL database and create tables if they don't exist"""
        default_db_uri = CONFIG.postgres.get_uri()
        temp_engine = create_engine(
            default_db_uri, pool_size=10, max_overflow=20, isolation_level="AUTOCOMMIT"
        )  # Enable autocommit
        database_name = CONFIG.postgres.database

        with temp_engine.connect() as conn:
            # Check if the database already exists
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :database"),
                {"database": database_name},
            )
            if not result.fetchone():
                # Create database if it doesn't exist
                conn.execute(
                    text(f"CREATE DATABASE {database_name}")
                )  # Removed WITH ENCODING 'utf8'
                logger.info(f"Database {database_name} created successfully")
            else:
                logger.info(f"Database {database_name} already exists")

        # Create tables
        Base.metadata.create_all(self.engine)

    async def sync_rate_limit(self):
        while True:
            logger.info("Syncing rate limit")
            normalized_stake = (
                await self.sidecar_bittensor_client.get_normalized_stake()
            )
            logger.info(f"Normalized stake: {normalized_stake}")
            rate_limit = max(CONFIG.rate_limiter.limit * normalized_stake, 2)
            logger.info(f"Rate limit: {rate_limit}")
            self.limiter.limit = rate_limit
            await asyncio.sleep(600)

    @contextmanager
    def _get_db(self):
        """Context manager for database sessions"""
        session = self.SessionMaker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_stats(self, uid: int) -> MinerStats:
        logger.debug(f"Getting stats for miner {uid}")
        with self._get_db() as session:
            stats = session.query(MinerStatsModel).filter_by(uid=uid).first()
            logger.info(f"Stats: {stats}")
            if not stats:
                logger.info(f"No stats found for miner {uid}, creating new entry")
                stats = MinerStatsModel(uid=uid, score=0.0)
                session.add(stats)
                session.commit()
                return MinerStats(uid=stats.uid, score=stats.score)

            return MinerStats(uid=stats.uid, score=stats.score)

    def update_stats(self, uid: int, new_score: float) -> bool:
        logger.debug(f"Updating stats for miner {uid} with new score {new_score}")
        stats = self.get_stats(uid)
        stats.score = stats.score * self.score_ema + new_score * (1 - self.score_ema)
        logger.info(f"Stats: {stats}")
        with self._get_db() as session:
            db_stats = session.query(MinerStatsModel).filter_by(uid=uid).first()
            db_stats.score = stats.score
            logger.debug(f"Updated stats for miner {uid}, new score: {stats.score}")
            return True

    def get_score_weights(self) -> tuple[list[int], list[float]]:
        logger.debug("Calculating score weights for all miners")
        with self._get_db() as session:
            # Get all miner stats in a single query
            all_stats = {
                stats.uid: stats.score for stats in session.query(MinerStatsModel).all()
            }

            # Use dictionary lookup instead of individual queries
            scores = [all_stats.get(uid, 0.0) for uid in self.miner_ids]
            total = sum(scores)
            normalized_scores = [
                round(score / total, 3) if total > 0 else 0.0 for score in scores
            ]

        return self.miner_ids, normalized_scores

    def check_connection(self) -> bool:
        """Check if both PostgreSQL and Redis connections are alive and working."""
        logger.debug("Checking PostgreSQL and Redis connections")
        try:
            # Check PostgreSQL connection
            with self._get_db() as session:
                session.query(MinerStatsModel).first()
            # Check Redis connection
            redis_ok = self.redis.ping()
            logger.debug(f"Connection check - PostgreSQL: True, Redis: {redis_ok}")
            return True and redis_ok
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False

    def consume_rate_limits(
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

        with self._get_db() as session:
            all_stats = {
                stats.uid: stats.score for stats in session.query(MinerStatsModel).all()
            }

        ranked_miners = sorted(all_stats.items(), key=lambda x: x[1], reverse=True)[
            : int(len(self.miner_ids) * top_fraction)
        ]

        probabilities = [
            self.limiter.get_remaining(self.miner_keys[miner_id])
            for miner_id, _ in ranked_miners
        ]
        probabilities = [
            probability / sum(probabilities) for probability in probabilities
        ]

        selected = np.random.choice(
            [miner_id for miner_id, _ in ranked_miners],
            size=min(count, len(ranked_miners)),
            replace=False,
            p=probabilities,
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
