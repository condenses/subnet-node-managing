from .serving_counter import RateLimiter
from .config import CONFIG
from pydantic import BaseModel
import numpy as np
import redis
from loguru import logger
from sqlalchemy import create_engine, Column, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from sidecar_bittensor.client import AsyncRestfulBittensor
import asyncio
import httpx
from cachetools import TTLCache, cached

Base = declarative_base()


class MinerStatsModel(Base):
    __tablename__ = "miner_stats"
    uid = Column(Integer, primary_key=True)
    score = Column(Float, default=0.01)


class MinerStats(BaseModel):
    uid: int
    score: float = 0.01


class MinerOrchestrator:
    def __init__(self):
        logger.info("Initializing MinerOrchestrator")
        # Use SQLite with optimized connection pool
        self.engine = create_engine(
            f"sqlite:///{CONFIG.sqlite.database_path}",
            connect_args={"check_same_thread": False},
            pool_size=20,
            max_overflow=30,
        )
        self.SessionMaker = scoped_session(sessionmaker(bind=self.engine))
        # Initialize database tables
        self._init_db()

        # Configure Redis connection pool
        redis_pool = redis.ConnectionPool(
            host=CONFIG.redis.host,
            port=CONFIG.redis.port,
            db=CONFIG.redis.db,
            username=CONFIG.redis.username,
            password=CONFIG.redis.password,
            decode_responses=True,
            max_connections=100,
        )
        self.redis = redis.Redis(connection_pool=redis_pool)

        # Initialize cache for frequent operations
        self.scores_cache = TTLCache(maxsize=1000, ttl=10)  # 10 seconds TTL

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
            logger.error("Failed to connect to SQLite or Redis")
            raise ConnectionError("Failed to connect to database")
        logger.info("MinerOrchestrator initialized successfully")

    def _init_db(self):
        """Initialize SQLite database and create tables if they don't exist"""
        # Create tables directly - no need to check if database exists with SQLite
        Base.metadata.create_all(self.engine)
        logger.info("SQLite database tables created successfully")

    async def get_normalized_stake_taostats(self):
        """Fetch and calculate normalized stake from Taostats API."""
        TAOSTATS_URL = "https://api.taostats.io/api/metagraph/latest/v1"
        PARAMS = "?netuid=47&validator_permit=true&limit=256"
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{TAOSTATS_URL}{PARAMS}",
                    headers={"Authorization": CONFIG.taostats_api.api_key},
                    timeout=10,
                )
                response.raise_for_status()  # Raise exception for HTTP errors
                
                items = response.json()
                stakes = [item["total_alpha_stake"] for item in items]
                total_stake = sum(stakes)
                normalized_stakes = [stake / total_stake for stake in stakes]
                hotkeys = [item["hotkey"]["ss58"] for item in items]
                
                target_index = hotkeys.index(CONFIG.taostats_api.ss58_address)
                logger.debug(f"Total stake: {total_stake}, Target index: {target_index}")
                return normalized_stakes[target_index]
            except (httpx.HTTPError, KeyError, ValueError) as e:
                logger.error(f"Taostats API error: {type(e).__name__}: {str(e)}")
                raise

    async def _get_normalized_stake_with_fallback(self):
        """Get normalized stake with fallback to Taostats if primary method fails."""
        try:
            normalized_stake = await self.sidecar_bittensor_client.get_normalized_stake()
            logger.debug("Successfully fetched stake from sidecar_bittensor")
            return normalized_stake
        except Exception as e:
            logger.warning(f"Failed to get normalized stake from primary source: {str(e)}")
            logger.warning("Attempting fallback to Taostats API")
            return await self.get_normalized_stake_taostats()

    async def sync_rate_limit(self):
        """Periodically sync rate limit based on normalized stake."""
        SYNC_INTERVAL = 600  # 10 minutes
        RETRY_INTERVAL = 10  # 10 seconds
        MAX_RETRIES = 3
        MIN_RATE_LIMIT = 2
        
        while True:
            logger.info("Syncing rate limit")
            
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # Get normalized stake with built-in fallback
                    normalized_stake = await self._get_normalized_stake_with_fallback()
                    
                    # Calculate and set the new rate limit
                    logger.info(f"Normalized stake: {normalized_stake}")
                    rate_limit = max(CONFIG.rate_limiter.limit * normalized_stake, MIN_RATE_LIMIT)
                    logger.info(f"Setting rate limit to: {rate_limit}")
                    self.limiter.limit = rate_limit
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    logger.warning(f"Rate limit sync attempt {attempt}/{MAX_RETRIES} failed: {str(e)}")
                    if attempt >= MAX_RETRIES:
                        logger.error(f"Failed to sync rate limit after {MAX_RETRIES} attempts")
                    else:
                        logger.info(f"Retrying in {RETRY_INTERVAL} seconds...")
                        await asyncio.sleep(RETRY_INTERVAL)
            
            # Wait until next sync cycle
            await asyncio.sleep(SYNC_INTERVAL)

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

    # Cache database lookup operations
    @cached(cache=lambda self: self.scores_cache, key=lambda uid: f"stats:{uid}")
    def get_stats(self, uid: int) -> MinerStats:
        logger.debug(f"Getting stats for miner {uid}")
        with self._get_db() as session:
            stats = session.query(MinerStatsModel).filter_by(uid=uid).first()
            if not stats:
                logger.info(f"No stats found for miner {uid}, creating new entry")
                stats = MinerStatsModel(uid=uid, score=0.01)
                session.add(stats)

        return MinerStats(uid=stats.uid, score=stats.score)

    def update_stats(self, uid: int, new_score: float) -> bool:
        logger.debug(f"Updating stats for miner {uid} with new score {new_score}")
        # Invalidate cache on update
        if f"stats:{uid}" in self.scores_cache:
            del self.scores_cache[f"stats:{uid}"]

        with self._get_db() as session:
            # Get and update stats in a single transaction
            stats = session.query(MinerStatsModel).filter_by(uid=uid).first()
            if not stats:
                logger.info(f"No stats found for miner {uid}, creating new entry")
                stats = MinerStatsModel(uid=uid, score=0.01)
                session.add(stats)

            # Apply the EMA calculation
            stats.score = max(
                stats.score * self.score_ema + new_score * (1 - self.score_ema), 0.01
            )
            logger.info(f"Updated stats for miner {uid}, new score: {stats.score}")
            # Session will be committed by the context manager
            return True

    def get_score_weights(self) -> tuple[list[int], list[float]]:
        logger.debug("Calculating score weights for all miners")
        with self._get_db() as session:
            # Get all miner stats in a single query
            all_stats = {
                stats.uid: stats.score for stats in session.query(MinerStatsModel).all()
            }

            # Use dictionary lookup instead of individual queries
            scores = [all_stats.get(uid, 0.01) for uid in self.miner_ids]
            total = sum(scores)
            normalized_scores = [
                round(score / total, 3) if total > 0 else 0.0 for score in scores
            ]

        return self.miner_ids, normalized_scores

    def check_connection(self) -> bool:
        """Check if both SQLite and Redis connections are alive and working."""
        logger.debug("Checking SQLite and Redis connections")
        try:
            # Check SQLite connection
            with self._get_db() as session:
                session.query(MinerStatsModel).first()
            # Check Redis connection
            redis_ok = self.redis.ping()
            logger.debug(f"Connection check - SQLite: True, Redis: {redis_ok}")
            return True and redis_ok
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False

    # Optimize rate limit consumption for batch operations
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
            if self.limiter.consume(
                self.miner_keys[uid],
                acceptable_consumed_rate=acceptable_consumed_rate,
            ):
                return [uid]
            return []

        with self._get_db() as session:
            all_stats = {
                stats.uid: stats.score for stats in session.query(MinerStatsModel).all()
            }
            default_stats = {miner_id: 0.01 for miner_id in self.miner_ids}
            all_stats = {**default_stats, **all_stats}

        ranked_miners = sorted(all_stats.items(), key=lambda x: x[1], reverse=True)[
            : int(len(self.miner_ids) * top_fraction)
        ]

        probabilities = [
            self.limiter.get_remaining(self.miner_keys[miner_id])
            for miner_id, _ in ranked_miners
        ]

        # Prevent division by zero
        sum_probabilities = sum(probabilities)
        if sum_probabilities <= 0:
            logger.warning("Sum of probabilities is zero, using equal probabilities")
            probabilities = [1.0 / len(ranked_miners) for _ in ranked_miners]
        else:
            probabilities = [
                probability / sum_probabilities for probability in probabilities
            ]

        selected = np.random.choice(
            [miner_id for miner_id, _ in ranked_miners],
            size=min(count, len(ranked_miners)),
            replace=False,
            p=probabilities,
        )
        logger.debug(f"Selected miners: {selected}")
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
