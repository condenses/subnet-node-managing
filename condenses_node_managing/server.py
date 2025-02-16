from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
from typing import List, Tuple, Optional
from .orchestrator import MinerOrchestrator, MinerStats
from .config import CONFIG

app = FastAPI()
orchestrator = MinerOrchestrator()


class ScoreUpdate(BaseModel):
    uid: int
    new_score: float


class RateLimitRequest(BaseModel):
    uid: Optional[int] = None
    top_fraction: float = 1.0
    count: int = 1


@app.get("/stats/{uid}", response_model=MinerStats)
async def get_stats(uid: int):
    """Get stats for a specific miner"""
    try:
        return orchestrator.get_stats(uid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stats/update")
async def update_stats(update: ScoreUpdate):
    """Update score for a specific miner"""
    try:
        result = orchestrator.update_stats(uid=update.uid, new_score=update.new_score)
        return {"modified_count": result.modified_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/rate-limits/check", response_model=List[int])
async def check_rate_limits(request: RateLimitRequest):
    """Check rate limits for miners"""
    try:
        return orchestrator.check_rate_limits(
            uid=request.uid, top_fraction=request.top_fraction, count=request.count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scores/weights", response_model=Tuple[List[int], List[float]])
async def get_score_weights():
    """Get score weights for all miners"""
    try:
        return orchestrator.get_score_weights()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def start_server():
    uvicorn.run(app, host=CONFIG.server.host, port=CONFIG.server.port)
