import redis
import time
from loguru import logger


class RateLimiter:
    def __init__(self, limit=60, interval=60, redis_client: redis.Redis = None):
        """
        Initialize the rate limiter with Redis connection and default limit

        Args:
            redis_host (str): Redis host address
            redis_port (int): Redis port number
            redis_db (int): Redis database number
            limit (int): Maximum number of requests allowed per minute
        """
        self.limit = limit
        self.interval = interval
        self.redis_client = redis_client

    def _get_current_window_key(self, node_key: str) -> str:
        """
        Generate the Redis key for the current window

        Args:
            node_key (str): Identifier for the node

        Returns:
            str: Redis key in format 'ratelimit:{node_key}:{minute_timestamp}'
        """
        current_minute = int(time.time() / self.interval)
        return f"ratelimit:{node_key}:{current_minute}"

    def consume(
        self, node_key: str, amount: int = 1, acceptable_consumed_rate=1.0
    ) -> bool:
        """
        Attempt to consume rate limit tokens for a node

        Args:
            node_key (str): Identifier for the node
            amount (int): Number of tokens to consume
            acceptable_consumed_rate (float): Acceptable consumed rate
        Returns:
            bool: True if tokens were consumed successfully, False if limit exceeded
        """
        window_key = self._get_current_window_key(node_key)

        # Use Redis pipeline for atomic operations
        pipe = self.redis_client.pipeline()

        # Check if key exists, if not set expiry
        pipe.exists(window_key)
        # Increment counter
        pipe.incrby(window_key, amount)
        results = pipe.execute()

        key_existed, new_count = results

        # If key is new, set expiry to end of current minute plus 5 seconds buffer
        if not key_existed:
            seconds_until_next_minute = self.interval - (
                int(time.time()) % self.interval
            )
            self.redis_client.expire(window_key, seconds_until_next_minute + 5)

        # Check if new count exceeds limit or remaining percentage is below acceptable rate
        consumed_rate = new_count / self.limit
        if new_count > self.limit or consumed_rate > acceptable_consumed_rate:
            # Rollback the increment
            logger.debug(
                f"Rate limit exceeded for {node_key}, consumed_rate: {consumed_rate}, new_count: {new_count}, limit: {self.limit}, acceptable_consumed_rate: {acceptable_consumed_rate}"
            )
            self.redis_client.decrby(window_key, amount)
            return False
        logger.debug(
            f"Rate limit consumed for {node_key}, consumed_rate: {consumed_rate}, new_count: {new_count}, limit: {self.limit}, acceptable_consumed_rate: {acceptable_consumed_rate}"
        )
        return True

    def get_remaining(self, node_key: str) -> int:
        """
        Get remaining tokens for the current minute

        Args:
            node_key (str): Identifier for the node

        Returns:
            int: Number of tokens remaining in current window
        """
        window_key = self._get_current_window_key(node_key)
        consumed = int(self.redis_client.get(window_key) or 0)
        return max(0, self.limit - consumed)

    def get_consumed(self, node_key: str) -> int:
        """
        Get number of tokens consumed in current minute

        Args:
            node_key (str): Identifier for the node

        Returns:
            int: Number of tokens consumed in current window
        """
        window_key = self._get_current_window_key(node_key)
        return int(self.redis_client.get(window_key) or 0)
