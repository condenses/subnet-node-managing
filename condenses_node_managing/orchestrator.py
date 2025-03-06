from .serving_counter import RateLimiter
from .config import CONFIG
from pydantic import BaseModel
import numpy as np
import redis
from loguru import logger
from sqlalchemy import create_engine, Column, Integer, Float, String, select, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from sidecar_bittensor.client import AsyncRestfulBittensor
import asyncio
import httpx
import time
import json

Base = declarative_base()


class MinerStatsModel(Base):
    __tablename__ = "miner_stats"
    uid = Column(Integer, primary_key=True)
    score = Column(Float, default=0.01)
    updated_at = Column(Float, default=time.time)  # Track when each record was updated
    scores_history = Column(String, default="[]")  # Store JSON array of recent scores


class MinerStats(BaseModel):
    uid: int
    score: float = 0.01
    updated_at: float = None
    scores_history: list[float] = []


class MinerOrchestrator:
    def __init__(self):
        logger.info("Initializing MinerOrchestrator")
        # Optimize SQLite connection for better concurrency
        self.engine = create_engine(
            f"sqlite:///{CONFIG.sqlite.database_path}",
            connect_args={
                "check_same_thread": False,
                "timeout": 30,  # Longer timeout for busy DB
                "isolation_level": "IMMEDIATE",  # Better concurrency control
            },
            pool_size=25,  # Increased pool size
            max_overflow=40,
            pool_timeout=30,
            pool_recycle=300,  # Recycle connections every 5 minutes
            pool_pre_ping=True,  # Check connection health before use
        )
        self.SessionMaker = scoped_session(sessionmaker(bind=self.engine))

        # Fixed size list - no more allocations needed
        self.miner_ids = list(range(0, 256))
        self.miner_keys = [f"miner:{uid}" for uid in self.miner_ids]

        # Initialize database tables
        self._init_db()

        # Configure Redis connection pool with optimized settings
        redis_pool = redis.ConnectionPool(
            host=CONFIG.redis.host,
            port=CONFIG.redis.port,
            db=CONFIG.redis.db,
            username=CONFIG.redis.username,
            password=CONFIG.redis.password,
            decode_responses=True,
            max_connections=150,  # Increased for better parallelism
            socket_timeout=5,  # Shorter timeout to fail fast
            socket_keepalive=True,
            health_check_interval=30,
        )
        self.redis = redis.Redis(connection_pool=redis_pool)

        # HTTP client configuration
        self.http_timeout = httpx.Timeout(10.0, connect=5.0)

        # Initialize API client
        self.sidecar_bittensor_client = AsyncRestfulBittensor(
            base_url=CONFIG.sidecar_bittensor.base_url,
        )

        self.limiter = RateLimiter(
            limit=None,
            interval=CONFIG.rate_limiter.interval,
            redis_client=self.redis,
        )

        # Validate connections before starting
        if not self.check_connection():
            logger.error("Failed to connect to SQLite or Redis")
            raise ConnectionError("Failed to connect to database")

        # Background task management
        self.background_tasks = set()

        logger.info("MinerOrchestrator initialized successfully")

    def _init_db(self):
        """Initialize SQLite database and create tables if they don't exist"""
        Base.metadata.create_all(self.engine)

        # Initialize default values for all miners if needed
        with self._get_db() as session:
            # Batch insert default values for all miners in a single transaction
            existing_uids = {
                uid
                for (uid,) in session.execute(select(MinerStatsModel.uid)).fetchall()
            }

            missing_uids = set(self.miner_ids) - existing_uids
            if missing_uids:
                logger.info(
                    f"Adding default entries for {len(missing_uids)} missing miners"
                )
                session.add_all(
                    [
                        MinerStatsModel(uid=uid, score=0.01, updated_at=time.time())
                        for uid in missing_uids
                    ]
                )

        logger.info("SQLite database tables and default entries created successfully")

    async def get_normalized_stake_taostats(self):
        """Fetch and calculate normalized stake from Taostats API with optimized error handling."""
        TAOSTATS_URL = "https://api.taostats.io/api/metagraph/latest/v1"
        PARAMS = "?netuid=47&validator_permit=true&limit=256"

        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            try:
                response = await client.get(
                    f"{TAOSTATS_URL}{PARAMS}",
                    headers={"Authorization": CONFIG.taostats_api.api_key},
                )
                response.raise_for_status()

                data = response.json()
                items = data.get("data", [])
                if not items:
                    raise ValueError("Empty data received from Taostats API")

                stakes = [float(item.get("total_alpha_stake", 0)) for item in items]
                total_stake = sum(stakes) or 1  # Avoid division by zero
                normalized_stakes = [stake / total_stake for stake in stakes]

                hotkeys = [item.get("hotkey", {}).get("ss58") for item in items]
                target_ss58 = CONFIG.taostats_api.ss58_address

                try:
                    target_index = hotkeys.index(target_ss58)
                    return normalized_stakes[target_index]
                except ValueError:
                    logger.error(
                        f"Target SS58 address {target_ss58} not found in hotkeys"
                    )
                    return 0.01  # Fallback default

            except (httpx.HTTPError, KeyError, ValueError) as e:
                logger.error(f"Taostats API error: {type(e).__name__}: {str(e)}")
                return 0.01  # Fallback default

    async def _get_normalized_stake_with_fallback(self):
        """Get normalized stake with fallback to Taostats if primary method fails."""
        try:
            normalized_stake = (
                await self.sidecar_bittensor_client.get_normalized_stake()
            )
            if normalized_stake is None or normalized_stake <= 0:
                raise ValueError("Invalid normalized stake value")
            return normalized_stake
        except Exception as e:
            logger.warning(
                f"Failed to get normalized stake from primary source: {str(e)}"
            )
            logger.warning("Attempting fallback to Taostats API")
            return await self.get_normalized_stake_taostats()

    async def sync_rate_limit(self):
        """Periodically sync rate limit based on normalized stake with exponential backoff."""
        SYNC_INTERVAL = 600  # 10 minutes
        MIN_RATE_LIMIT = 2
        BACKOFF_FACTOR = 2
        MAX_BACKOFF = 120  # 2 minutes max backoff

        # Register task for proper cleanup
        task = asyncio.current_task()
        self.background_tasks.add(task)

        try:
            while True:
                logger.info("Syncing rate limit")
                backoff = 10  # Initial backoff is 10 seconds
                success = False

                while not success:
                    try:
                        normalized_stake = (
                            await self._get_normalized_stake_with_fallback()
                        )

                        # Calculate and set the new rate limit
                        logger.info(f"Normalized stake: {normalized_stake}")
                        rate_limit = max(
                            CONFIG.rate_limiter.limit * normalized_stake, MIN_RATE_LIMIT
                        )
                        logger.info(f"Setting rate limit to: {rate_limit}")
                        self.limiter.limit = rate_limit
                        success = True

                    except Exception as e:
                        logger.warning(f"Rate limit sync failed: {str(e)}")
                        logger.info(f"Retrying in {backoff} seconds...")
                        await asyncio.sleep(backoff)

                        # Exponential backoff with max cap
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)

                # Wait until next sync cycle
                await asyncio.sleep(SYNC_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Rate limit sync task cancelled")
        finally:
            self.background_tasks.discard(task)

    @contextmanager
    def _get_db(self):
        """Context manager for database sessions with automatic retries"""
        MAX_RETRIES = 3
        RETRY_DELAY = 0.5  # seconds

        session = self.SessionMaker()
        for attempt in range(MAX_RETRIES):
            try:
                yield session
                session.commit()
                break
            except Exception as e:
                session.rollback()
                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"Database operation failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}"
                    )
                    time.sleep(RETRY_DELAY * (attempt + 1))  # Linear backoff
                else:
                    logger.error(
                        f"Database operation failed after {MAX_RETRIES} attempts"
                    )
                    raise
            finally:
                session.close()

    def get_stats(self, uid: int) -> MinerStats:
        """Get miner stats without caching"""
        logger.debug(f"Getting stats for miner {uid}")
        with self._get_db() as session:
            stats = session.query(MinerStatsModel).filter_by(uid=uid).first()
            if not stats:
                logger.info(f"No stats found for miner {uid}, creating new entry")
                stats = MinerStatsModel(
                    uid=uid,
                    score=0.01,
                    updated_at=time.time(),
                    scores_history=json.dumps([]),
                )
                session.add(stats)
                session.flush()  # Ensure uid is populated

            # Parse scores history
            try:
                scores_history = json.loads(stats.scores_history)
            except (json.JSONDecodeError, TypeError):
                scores_history = []

        return MinerStats(
            uid=stats.uid,
            score=stats.score,
            updated_at=stats.updated_at,
            scores_history=scores_history,
        )

    def update_stats(self, uid: int, new_score: float) -> bool:
        """Update miner stats with optimized database access"""
        logger.debug(f"Updating stats for miner {uid} with new score {new_score}")

        timestamp = time.time()
        with self._get_db() as session:
            # Optimize with upsert-like pattern
            stats = (
                session.query(MinerStatsModel)
                .filter_by(uid=uid)
                .with_for_update()
                .first()
            )
            if not stats:
                stats = MinerStatsModel(
                    uid=uid,
                    score=new_score,
                    updated_at=timestamp,
                    scores_history=json.dumps([new_score]),
                )
                session.add(stats)
            else:
                # Update scores history - keep last 64 scores
                try:
                    scores_history = json.loads(stats.scores_history)
                except (json.JSONDecodeError, TypeError):
                    scores_history = []

                # Add new score to history and limit to 64 entries
                scores_history.append(new_score)
                if len(scores_history) > 64:
                    scores_history = scores_history[-64:]

                # Calculate mean of scores history
                stats.score = max(np.mean(scores_history), 0.01)
                stats.scores_history = json.dumps(scores_history)
                stats.updated_at = timestamp

            logger.info(f"Updated stats for miner {uid}, new score: {stats.score}")
            return True

    def get_score_weights(self) -> tuple[list[int], list[float]]:
        """Calculate score weights for all miners without caching"""
        logger.debug("Calculating score weights for all miners")
        with self._get_db() as session:
            # Optimized query: load all stats in one go and create an index
            all_stats = {
                stats.uid: stats.score for stats in session.query(MinerStatsModel).all()
            }

            # Use vectorized operations for better performance
            scores = np.array([all_stats.get(uid, 0.01) for uid in self.miner_ids])
            total = np.sum(scores)

            # Prevent division by zero
            if total <= 0:
                normalized_scores = np.full(len(scores), 1.0 / len(scores))
            else:
                normalized_scores = np.round(scores / total, 3)

            return (self.miner_ids, normalized_scores.tolist())

    def check_connection(self) -> bool:
        """Check database and Redis connections with timeout protection"""
        logger.debug("Checking SQLite and Redis connections")
        try:
            # Check SQLite connection with timeout
            with self._get_db() as session:
                session.execute(text("SELECT 1")).scalar()

            # Check Redis connection with timeout
            redis_ok = self.redis.ping()
            logger.debug(f"Connection check - SQLite: True, Redis: {redis_ok}")
            return redis_ok
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False

    # Batch processing of rate limits with numpy vectorization
    def consume_rate_limits(
        self,
        uid: int = None,
        top_fraction: float = 1.0,
        count: int = 1,
        acceptable_consumed_rate: float = 1.0,
    ) -> list[int]:
        """Check and consume rate limits for miners with optimized selection algorithm"""
        if not self.limiter.limit:
            logger.error("Rate limit is not set, using fallback limit of 10")
            self.limiter.limit = 10  # Fallback to a reasonable default

        logger.debug(
            f"Checking rate limits - uid: {uid}, top_fraction: {top_fraction}, count: {count}"
        )

        # Fast path for single miner selection
        if uid is not None:
            if self.limiter.consume(
                self.miner_keys[uid],
                acceptable_consumed_rate=acceptable_consumed_rate,
            ):
                return [uid]
            return []

        # Use cached stats for better performance
        miner_ids, weights = self.get_score_weights()

        # Apply top fraction filter
        if top_fraction < 1.0:
            # Get indices of top miners by weight
            num_miners = int(len(miner_ids) * top_fraction)
            indices = np.argsort(weights)[-num_miners:]
            candidate_miners = [miner_ids[i] for i in indices]
            candidate_weights = [weights[i] for i in indices]
        else:
            candidate_miners = miner_ids
            candidate_weights = weights

        # Calculate rate limit-adjusted probabilities
        remaining_rates = np.array(
            [
                self.limiter.get_remaining(self.miner_keys[miner_id])
                for miner_id in candidate_miners
            ]
        )

        # Combine weights and remaining rates for better selection
        combined_scores = remaining_rates * np.array(candidate_weights)

        # Prevent division by zero
        sum_scores = np.sum(combined_scores)
        if sum_scores <= 0:
            logger.warning("Sum of combined scores is zero, using equal probabilities")
            probabilities = np.full(len(candidate_miners), 1.0 / len(candidate_miners))
        else:
            probabilities = combined_scores / sum_scores

        # Select miners based on combined scores
        try:
            selected = np.random.choice(
                candidate_miners,
                size=min(count, len(candidate_miners)),
                replace=False,
                p=probabilities,
            )
        except ValueError as e:
            logger.error(
                f"Selection error: {str(e)}, falling back to uniform selection"
            )
            selected = np.random.choice(
                candidate_miners,
                size=min(count, len(candidate_miners)),
                replace=False,
            )

        logger.debug(f"Selected miners: {selected}")

        # Apply rate limiting to selected miners
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

    async def start_background_tasks(self):
        """Start all background tasks"""
        logger.info("Starting background tasks")
        asyncio.create_task(self.sync_rate_limit())

    async def shutdown(self):
        """Cleanly shutdown all resources"""
        logger.info("Shutting down MinerOrchestrator")

        # Cancel all background tasks
        for task in self.background_tasks:
            task.cancel()

        # Wait for tasks to complete cancellation
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)

        # Close database connection pool
        self.engine.dispose()

        # Close Redis connection pool
        self.redis.connection_pool.disconnect()

        logger.info("MinerOrchestrator shutdown complete")
