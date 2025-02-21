from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
from typing import List, Tuple, Optional
from .orchestrator import MinerOrchestrator, MinerStats
from .config import CONFIG
from loguru import logger

app = FastAPI()
orchestrator = MinerOrchestrator()


class ScoreUpdate(BaseModel):
    uid: int
    new_score: float


class RateLimitRequest(BaseModel):
    uid: Optional[int] = None
    top_fraction: float = 1.0
    count: int = 1
    acceptable_consumed_rate: float = 1.0


@app.get("/api/stats/{uid}", response_model=MinerStats)
async def get_stats(uid: int):
    """Get stats for a specific miner"""
    try:
        return orchestrator.get_stats(uid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/stats/update")
async def update_stats(update: ScoreUpdate):
    """Update score for a specific miner"""
    try:
        result = orchestrator.update_stats(uid=update.uid, new_score=update.new_score)
        return {"result": result}
    except Exception as e:
        logger.error(f"Error updating stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/rate-limits/consume", response_model=List[int])
async def consume_rate_limits(request: RateLimitRequest):
    """Consume rate limits for miners"""
    try:
        return orchestrator.consume_rate_limits(
            uid=request.uid,
            top_fraction=request.top_fraction,
            count=request.count,
            acceptable_consumed_rate=request.acceptable_consumed_rate,
        )
    except Exception as e:
        logger.error(f"Error consuming rate limits: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/weights", response_model=Tuple[List[int], List[float]])
async def get_score_weights():
    """Get score weights for all miners"""
    try:
        return orchestrator.get_score_weights()
    except Exception as e:
        logger.error(f"Error getting score weights: {e}")
        raise HTTPException(status_code=500, detail=str(e))
