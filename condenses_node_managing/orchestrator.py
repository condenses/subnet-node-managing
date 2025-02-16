from .serving_counter import RateLimiter
from pymongo import MongoClient
from .config import CONFIG
from pydantic import BaseModel
from pymongo.results import UpdateResult
import numpy as np


class MinerStats(BaseModel):
    uid: int
    score: float = 0.0


class MinerOrchestrator:
    def __init__(self):
        self.limiter = RateLimiter(
            redis_host=CONFIG.redis.host,
            redis_port=CONFIG.redis.port,
            redis_db=CONFIG.redis.db,
            limit=CONFIG.rate_limiter.limit,
            interval=CONFIG.rate_limiter.interval,
        )
        self.db = MongoClient(
            host=CONFIG.mongo.host,
            port=CONFIG.mongo.port,
            username=CONFIG.mongo.username,
            password=CONFIG.mongo.password,
        )
        self.stats_collection = self.db.get_database("condenses").get_collection(
            "miner_stats"
        )
        self.miner_ids = list(range(0, 256))
        self.miner_keys = [f"miner:{uid}" for uid in self.miner_ids]
        self.score_ema = CONFIG.miner_manager.score_ema

    def get_stats(self, uid: int) -> MinerStats:
        stats = self.stats_collection.find_one({"uid": uid})

        if not stats:
            stats = MinerStats(uid=uid, score=0.0)
            self.stats_collection.insert_one(stats.dict())

        return MinerStats(**stats)

    def update_stats(self, uid: int, new_score: float) -> UpdateResult:
        stats = self.get_stats(uid)
        stats.score = stats.score * self.score_ema + new_score * (1 - self.score_ema)
        result = self.stats_collection.update_one(
            {"uid": uid}, {"$set": {"score": stats.score}}
        )
        return result

    def check_rate_limits(
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
        if uid:
            return [uid] if self.limiter.consume(self.miner_keys[uid]) else []

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

        return [
            miner_id
            for miner_id in selected
            if self.limiter.consume(self.miner_keys[miner_id])
        ]

    def get_score_weights(self) -> tuple[list[int], list[float]]:
        scores = [self.get_stats(uid).score for uid in self.miner_ids]
        total = sum(scores)
        normalized_scores = [round(score / total, 3) for score in scores]
        return self.miner_ids, normalized_scores
