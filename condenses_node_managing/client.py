from typing import List, Tuple, Optional
import httpx
from pydantic import BaseModel


class ScoreUpdate(BaseModel):
    uid: int
    new_score: float


class RateLimitRequest(BaseModel):
    uid: Optional[int] = None
    top_fraction: float = 1.0
    count: int = 1


class MinerStats(BaseModel):
    uid: int
    score: float
    last_update: float


class OrchestratorClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def get_stats(self, uid: int) -> MinerStats:
        """Get stats for a specific miner"""
        response = self.client.get(f"{self.base_url}/api/stats/{uid}")
        response.raise_for_status()
        return MinerStats(**response.json())

    def update_stats(self, uid: int, new_score: float) -> dict:
        """Update score for a specific miner"""
        update = ScoreUpdate(uid=uid, new_score=new_score)
        response = self.client.post(
            f"{self.base_url}/api/stats/update", json=update.model_dump()
        )
        response.raise_for_status()
        return response.json()

    def check_rate_limits(
        self, uid: Optional[int] = None, top_fraction: float = 1.0, count: int = 1
    ) -> List[int]:
        """Check rate limits for miners"""
        request = RateLimitRequest(uid=uid, top_fraction=top_fraction, count=count)
        response = self.client.post(
            f"{self.base_url}/api/rate-limits/consume", json=request.model_dump()
        )
        response.raise_for_status()
        return response.json()

    def get_score_weights(self) -> Tuple[List[int], List[float]]:
        """Get score weights for all miners"""
        response = self.client.get(f"{self.base_url}/api/weights")
        response.raise_for_status()
        return tuple(response.json())


class AsyncOrchestratorClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def get_stats(self, uid: int) -> MinerStats:
        """Get stats for a specific miner"""
        response = await self.client.get(f"{self.base_url}/api/stats/{uid}")
        response.raise_for_status()
        return MinerStats(**response.json())

    async def update_stats(self, uid: int, new_score: float) -> dict:
        """Update score for a specific miner"""
        update = ScoreUpdate(uid=uid, new_score=new_score)
        response = await self.client.post(
            f"{self.base_url}/api/stats/update", json=update.model_dump()
        )
        response.raise_for_status()
        return response.json()

    async def check_rate_limits(
        self, uid: Optional[int] = None, top_fraction: float = 1.0, count: int = 1
    ) -> List[int]:
        """Check rate limits for miners"""
        request = RateLimitRequest(uid=uid, top_fraction=top_fraction, count=count)
        response = await self.client.post(
            f"{self.base_url}/api/rate-limits/consume", json=request.model_dump()
        )
        response.raise_for_status()
        return response.json()

    async def get_score_weights(self) -> Tuple[List[int], List[float]]:
        """Get score weights for all miners"""
        response = await self.client.get(f"{self.base_url}/api/weights")
        response.raise_for_status()
        return tuple(response.json())


# Usage examples:
if __name__ == "__main__":
    # Synchronous usage
    with OrchestratorClient() as client:
        # Get stats for miner with uid 1
        stats = client.get_stats(1)
        print(f"Miner stats: {stats}")

        # Update stats
        result = client.update_stats(1, 0.95)
        print(f"Update result: {result}")

    # Async usage
    import asyncio

    async def main():
        async with AsyncOrchestratorClient() as client:
            # Get stats for miner with uid 1
            stats = await client.get_stats(1)
            print(f"Miner stats: {stats}")

            # Update stats
            result = await client.update_stats(1, 0.95)
            print(f"Update result: {result}")

    # Run async example
    asyncio.run(main())
