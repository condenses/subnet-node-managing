from .serving_counter import RateLimiter
from pymongo import MongoClient
from .config import CONFIG
from pydantic import BaseModel
from pymongo.results import UpdateResult
import numpy as np
import redis
from loguru import logger


class MinerStats(BaseModel):
    uid: int
    score: float = 0.0


class MinerOrchestrator:
    def __init__(self):
        logger.info("Initializing MinerOrchestrator")
        self.db = MongoClient(CONFIG.mongo.get_uri())
        self.stats_collection = self.db.get_database(
            CONFIG.mongo.database
        ).get_collection(CONFIG.mongo.collection)
        self.redis = redis.Redis(
            host=CONFIG.redis.host,
            port=CONFIG.redis.port,
            db=CONFIG.redis.db,
            username=CONFIG.redis.username,
            password=CONFIG.redis.password,
            decode_responses=True,
        )
        self.limiter = RateLimiter(
            limit=CONFIG.rate_limiter.limit,
            interval=CONFIG.rate_limiter.interval,
            redis_client=self.redis,
        )
        self.miner_ids = list(range(0, 256))
        self.miner_keys = [f"miner:{uid}" for uid in self.miner_ids]
        self.score_ema = CONFIG.miner_manager.score_ema

        if not self.check_connection():
            logger.error("Failed to connect to MongoDB or Redis")
            raise ConnectionError("Failed to connect to MongoDB")
        logger.info("MinerOrchestrator initialized successfully")

    def get_stats(self, uid: int) -> MinerStats:
        logger.debug(f"Getting stats for miner {uid}")
        stats = self.stats_collection.find_one({"uid": uid})

        if not stats:
            logger.info(f"No stats found for miner {uid}, creating new entry")
            stats = MinerStats(uid=uid, score=0.0)
            self.stats_collection.insert_one(stats.dict())
            return stats

        return MinerStats(**dict(stats))

    def update_stats(self, uid: int, new_score: float) -> UpdateResult:
        logger.debug(f"Updating stats for miner {uid} with new score {new_score}")
        stats = self.get_stats(uid)
        stats.score = stats.score * self.score_ema + new_score * (1 - self.score_ema)
        result = self.stats_collection.update_one(
            {"uid": uid}, {"$set": {"score": stats.score}}
        )
        logger.debug(f"Updated stats for miner {uid}, new score: {stats.score}")
        return result

    def consume_rate_limits(
        self, uid: int = None, top_fraction: float = 1.0, count: int = 1
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
        logger.debug(
            f"Checking rate limits - uid: {uid}, top_fraction: {top_fraction}, count: {count}"
        )
        if uid:
            result = [uid] if self.limiter.consume(self.miner_keys[uid]) else []
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
            if self.limiter.consume(self.miner_keys[miner_id])
        ]
        logger.debug(f"Selected miners after rate limit check: {result}")
        return result

    def get_score_weights(self) -> tuple[list[int], list[float]]:
        logger.debug("Calculating score weights for all miners")
        scores = [self.get_stats(uid).score for uid in self.miner_ids]
        total = sum(scores)
        normalized_scores = [round(score / total, 3) for score in scores]
        return self.miner_ids, normalized_scores

    def check_connection(self) -> bool:
        """
        Check if both MongoDB and Redis connections are alive and working.

        Returns:
            bool: True if both connections are working, False otherwise
        """
        logger.debug("Checking MongoDB and Redis connections")
        try:
            # Check MongoDB connection
            mongo_ok = self.db.admin.command("ping")
            # Check Redis connection
            redis_ok = self.redis.ping()
            logger.debug(f"Connection check - MongoDB: {mongo_ok}, Redis: {redis_ok}")
            return mongo_ok and redis_ok
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False
