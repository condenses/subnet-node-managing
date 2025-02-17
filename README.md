
<br /><br />
<div align="center">
  <h1 align="center">subnet-node-managing</h1>
  <h4 align="center"> A Python SDK for managing and interacting with Subnet nodes. This SDK provides both synchronous and asynchronous clients for managing miner statistics, rate limits, and score weights.
</div>

## Installation

```bash
pip install git+https://github.com/condenses/subnet-node-managing.git
```

## Configuration

The SDK can be configured using environment variables:

```bash
# Redis configuration
REDIS__HOST=localhost
REDIS__PORT=6379
REDIS__DB=0

# MongoDB configuration
MONGO__HOST=localhost
MONGO__PORT=27017
MONGO__USERNAME=user  # Optional
MONGO__PASSWORD=pass  # Optional

# Rate limiter configuration
RATE_LIMITER__LIMIT=512
RATE_LIMITER__INTERVAL=60

# Miner manager configuration
MINER_MANAGER__SCORE_EMA=0.95
```

## Usage

### Synchronous Client

```python
from condenses_node_managing.client import OrchestratorClient

# Initialize the client
with OrchestratorClient(base_url="http://localhost:8000") as client:
    # Get stats for a specific miner
    stats = client.get_stats(uid=1)
    print(f"Miner stats: {stats}")

    # Update miner score
    result = client.update_stats(uid=1, new_score=0.95)
    print(f"Update result: {result}")

    # Check rate limits
    allowed_miners = client.check_rate_limits(
        uid=None,  # Optional: specific miner ID
        top_fraction=0.8,  # Top 80% of miners
        count=5  # Number of miners to return
    )
    print(f"Allowed miners: {allowed_miners}")

    # Get score weights for all miners
    uids, weights = client.get_score_weights()
    print(f"Miner UIDs: {uids}")
    print(f"Weights: {weights}")
```

### Asynchronous Client

```python
import asyncio
from condenses_node_managing.client import AsyncOrchestratorClient

async def main():
    async with AsyncOrchestratorClient(base_url="http://localhost:8000") as client:
        # Get stats for a specific miner
        stats = await client.get_stats(uid=1)
        print(f"Miner stats: {stats}")

        # Update miner score
        result = await client.update_stats(uid=1, new_score=0.95)
        print(f"Update result: {result}")

# Run async code
asyncio.run(main())
```

## API Reference

### MinerStats Model

```python
class MinerStats:
    uid: int        # Unique identifier for the miner
    score: float    # Current score of the miner
    last_update: float  # Timestamp of last update
```

### OrchestratorClient / AsyncOrchestratorClient Methods

#### get_stats(uid: int) → MinerStats
Get statistics for a specific miner.

#### update_stats(uid: int, new_score: float) → dict
Update the score for a specific miner.

#### consume_rate_limits(uid: Optional[int] = None, top_fraction: float = 1.0, count: int = 1) → List[int]
Consume rate limits for miners. Returns a list of allowed miner UIDs.
- `uid`: Optional specific miner to check
- `top_fraction`: Consider only top fraction of miners (0.0 to 1.0)
- `count`: Number of miners to return

#### get_weights() → Tuple[List[int], List[float]]
Get weights for all miners. Returns a tuple of (miner_uids, weights).

## Error Handling

The SDK will raise appropriate HTTP exceptions when API calls fail. It's recommended to implement proper error handling:

```python
from httpx import HTTPError

try:
    stats = client.get_stats(uid=1)
except HTTPError as e:
    print(f"API request failed: {e}")
```
