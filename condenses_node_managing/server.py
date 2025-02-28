from fastapi import FastAPI, HTTPException, Request, Depends, BackgroundTasks
import uvicorn
from pydantic import BaseModel
from typing import List, Tuple, Optional
from .orchestrator import MinerOrchestrator, MinerStats
from .config import CONFIG
from loguru import logger
import asyncio
import time
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.concurrency import run_in_threadpool

app = FastAPI()
orchestrator = MinerOrchestrator()

# Add CORS middleware for API access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Add request logging middleware
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        logger.debug(f"Request {request.url.path} took {process_time:.4f}s")
        return response


# Add timeout middleware
class TimeoutMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            return await asyncio.wait_for(call_next(request), timeout=5.0)
        except asyncio.TimeoutError:
            return HTTPException(
                status_code=503,
                detail="Service temporarily unavailable, request timeout",
            )


app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(TimeoutMiddleware)


class ScoreUpdate(BaseModel):
    uid: int
    new_score: float


class RateLimitRequest(BaseModel):
    uid: Optional[int] = None
    top_fraction: float = 1.0
    count: int = 1
    acceptable_consumed_rate: float = 1.0


@app.on_event("startup")
async def startup_event():
    """Initialize background tasks when the app starts"""
    asyncio.create_task(orchestrator.sync_rate_limit())


@app.get("/api/stats/{uid}", response_model=MinerStats)
async def get_stats(uid: int):
    """Get stats for a specific miner"""
    try:
        # Run DB operations in threadpool to avoid blocking
        return await run_in_threadpool(orchestrator.get_stats, uid)
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/stats/update")
async def update_stats(update: ScoreUpdate):
    """Update score for a specific miner"""
    try:
        result = await run_in_threadpool(
            orchestrator.update_stats, uid=update.uid, new_score=update.new_score
        )
        return {"result": result}
    except Exception as e:
        logger.error(f"Error updating stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/rate-limits/consume", response_model=List[int])
async def consume_rate_limits(request: RateLimitRequest):
    """Consume rate limits for miners"""
    try:
        return await run_in_threadpool(
            orchestrator.consume_rate_limits,
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
        return await run_in_threadpool(orchestrator.get_score_weights)
    except Exception as e:
        logger.error(f"Error getting score weights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Add health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    db_ok = await run_in_threadpool(orchestrator.check_connection)
    return {
        "status": "healthy" if db_ok else "unhealthy",
        "database": "connected" if db_ok else "disconnected",
    }


if __name__ == "__main__":
    uvicorn.run(
        "condenses_node_managing.server:app",
        host="0.0.0.0",
        port=8000,
        workers=4,  # Increase worker count for higher throughput
        loop="uvloop",  # Use uvloop for better performance
        timeout_keep_alive=5,
    )
