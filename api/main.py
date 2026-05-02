"""
api/main.py — FastAPI REST Control Plane

Provides:
  - Node status overview
  - Lock management (acquire/release/state)
  - Queue management (enqueue/dequeue/ack/stats)
  - Cache management (read/write/invalidate)
  - System-wide metrics overview
  - Swagger UI at /docs (auto-generated)
"""
import asyncio
import os
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(
    title="Distributed Sync System API",
    description="""
## Distributed Synchronization System

Control plane for the distributed system with:
- **Raft Consensus** based Distributed Lock Manager
- **Consistent Hashing** Distributed Queue
- **MESI Protocol** Cache Coherence

### Nodes
By default, 4 nodes run at:
- `node1:8001`, `node2:8002`, `node3:8003`, `node4:8004`
    """,
    version="1.0.0",
    contact={"name": "TUGAS 2 - Sistem Parallel dan Terdistribusi"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.exists("dashboard"):
    app.mount("/dashboard", StaticFiles(directory="dashboard", html=True), name="dashboard")

# ── Node Configuration ────────────────────────────────────────────────────────
NODE_ADDRESSES = {
    "node1": os.getenv("NODE1_ADDR", "node1:8001"),
    "node2": os.getenv("NODE2_ADDR", "node2:8002"),
    "node3": os.getenv("NODE3_ADDR", "node3:8003"),
    "node4": os.getenv("NODE4_ADDR", "node4:8004"),
}


async def _get(node_addr: str, path: str) -> Optional[Dict]:
    url = f"http://{node_addr}{path}"
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            r = await client.get(url)
            return r.json()
        except Exception:
            return None


async def _post(node_addr: str, path: str, body: Dict) -> Optional[Dict]:
    url = f"http://{node_addr}{path}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.post(url, json=body)
            return r.json()
        except Exception:
            return None


# ── Pydantic Models ───────────────────────────────────────────────────────────

class LockAcquireRequest(BaseModel):
    txn_id: Optional[str] = None
    resource: str
    lock_type: str = "EXCLUSIVE"  # SHARED or EXCLUSIVE
    node: str = "node1"


class LockReleaseRequest(BaseModel):
    txn_id: str
    resource: str
    node: str = "node1"


class EnqueueRequest(BaseModel):
    queue: str = "default"
    body: Any
    producer_id: str = "api"
    node: str = "node1"


class DequeueRequest(BaseModel):
    queue: str = "default"
    consumer_id: str = "api"
    node: str = "node1"


class AckRequest(BaseModel):
    queue: str
    msg_id: str
    node: str = "node1"


class CacheWriteRequest(BaseModel):
    value: Any
    node: str = "node1"


# ── System Status ─────────────────────────────────────────────────────────────

@app.get("/", tags=["System"], summary="API Root")
async def root():
    return {
        "service": "Distributed Sync System",
        "version": "1.0.0",
        "description": "Distributed Lock (Raft/PBFT) + Queue (Consistent Hashing) + Cache (MESI)",
        "nodes": list(NODE_ADDRESSES.keys()),
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "liveness": "/health/live",
            "readiness": "/health/ready",
            "status": "/status",
        },
        "components": {
            "lock_manager": "Raft consensus + PBFT Byzantine fault tolerance",
            "queue": "Consistent hashing + at-least-once delivery",
            "cache": "MESI protocol + LRU replacement",
            "monitoring": "Prometheus :9090 | Grafana :3000",
        },
    }


@app.get("/status", tags=["System"], summary="All Nodes Status")
async def system_status():
    results = await asyncio.gather(
        *[_get(addr, "/status") for addr in NODE_ADDRESSES.values()],
        return_exceptions=True
    )
    return {
        node: result if not isinstance(result, Exception) else {"error": "unreachable"}
        for node, result in zip(NODE_ADDRESSES.keys(), results)
    }


@app.get("/health", tags=["System"], summary="All Nodes Health")
async def system_health():
    """Cluster-wide health check. Shows UP/DOWN per node."""
    results = await asyncio.gather(
        *[_get(addr, "/health") for addr in NODE_ADDRESSES.values()],
        return_exceptions=True
    )
    statuses = {}
    up_count = 0
    for node, result in zip(NODE_ADDRESSES.keys(), results):
        if isinstance(result, Exception) or result is None:
            statuses[node] = "DOWN"
        else:
            statuses[node] = "UP"
            up_count += 1
    total = len(NODE_ADDRESSES)
    return {
        "status": "healthy" if up_count == total else ("degraded" if up_count > 0 else "down"),
        "nodes_up": up_count,
        "nodes_total": total,
        "nodes": statuses,
    }


@app.get("/health/live", tags=["System"], summary="Liveness probe")
async def liveness():
    """Kubernetes-style liveness probe. Always returns 200 if API process is alive."""
    return {"status": "alive", "service": "distributed-sync-api"}


@app.get("/health/ready", tags=["System"], summary="Readiness probe")
async def readiness():
    """
    Readiness probe — returns 200 only if at least one node is reachable.
    Returns 503 if all nodes are down.
    """
    results = await asyncio.gather(
        *[_get(addr, "/health") for addr in NODE_ADDRESSES.values()],
        return_exceptions=True
    )
    reachable = sum(
        1 for r in results
        if not isinstance(r, Exception) and r is not None
    )
    if reachable == 0:
        from fastapi import Response
        return Response(
            content='{"status":"not_ready","reason":"all nodes unreachable"}',
            status_code=503,
            media_type="application/json",
        )
    return {
        "status": "ready",
        "nodes_reachable": reachable,
        "nodes_total": len(NODE_ADDRESSES),
    }


# ── Lock Manager ──────────────────────────────────────────────────────────────

@app.post("/lock/acquire", tags=["Lock Manager"], summary="Acquire a distributed lock")
async def acquire_lock(req: LockAcquireRequest):
    """
    Acquire a distributed lock via Raft consensus.

    - **EXCLUSIVE**: Only one transaction can hold the lock.
    - **SHARED**: Multiple transactions can hold shared locks simultaneously.
    - Lock is only granted by the current Raft leader.
    """
    addr = NODE_ADDRESSES.get(req.node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {req.node}")
    result = await _post(addr, "/lock/acquire", req.model_dump(exclude={"node"}))
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


@app.post("/lock/release", tags=["Lock Manager"], summary="Release a distributed lock")
async def release_lock(req: LockReleaseRequest):
    addr = NODE_ADDRESSES.get(req.node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {req.node}")
    result = await _post(addr, "/lock/release", req.model_dump(exclude={"node"}))
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


@app.get("/lock/state", tags=["Lock Manager"], summary="Get current lock state")
async def lock_state(node: str = Query("node1")):
    addr = NODE_ADDRESSES.get(node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {node}")
    result = await _get(addr, "/lock/state")
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


# ── Queue System ──────────────────────────────────────────────────────────────

@app.post("/queue/enqueue", tags=["Queue"], summary="Enqueue a message")
async def enqueue(req: EnqueueRequest):
    """
    Enqueue a message. The system automatically routes to the correct node
    based on consistent hashing of the queue name.
    """
    addr = NODE_ADDRESSES.get(req.node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {req.node}")
    result = await _post(addr, "/queue/enqueue", req.model_dump(exclude={"node"}))
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


@app.post("/queue/dequeue", tags=["Queue"], summary="Dequeue a message")
async def dequeue(req: DequeueRequest):
    """Dequeue next message. Message moves to in-flight state until ACK'd."""
    addr = NODE_ADDRESSES.get(req.node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {req.node}")
    result = await _post(addr, "/queue/dequeue", req.model_dump(exclude={"node"}))
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


@app.post("/queue/ack", tags=["Queue"], summary="Acknowledge message delivery")
async def ack_message(req: AckRequest):
    """ACK a message to remove it from the in-flight list and persistence store."""
    addr = NODE_ADDRESSES.get(req.node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {req.node}")
    result = await _post(addr, "/queue/ack", req.model_dump(exclude={"node"}))
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


@app.get("/queue/stats", tags=["Queue"], summary="Queue statistics")
async def queue_stats(node: str = Query("node1")):
    addr = NODE_ADDRESSES.get(node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {node}")
    result = await _get(addr, "/queue/stats")
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


# ── Cache Coherence ───────────────────────────────────────────────────────────

@app.get("/cache/{key}", tags=["Cache"], summary="Read cache key (MESI)")
async def cache_read(key: str, node: str = Query("node1")):
    """
    Read a key from the cache. Implements MESI read protocol:
    hit returns from local cache, miss fetches and sets E or S state.
    """
    addr = NODE_ADDRESSES.get(node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {node}")
    result = await _get(addr, f"/cache/{key}")
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


@app.put("/cache/{key}", tags=["Cache"], summary="Write cache key (MESI)")
async def cache_write(key: str, req: CacheWriteRequest):
    """
    Write a key. Triggers MESI write protocol:
    broadcasts Invalidate to all peers, then sets M state locally.
    """
    addr = NODE_ADDRESSES.get(req.node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {req.node}")
    result = await _post(addr, f"/cache/{key}", {"value": req.value})
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


@app.delete("/cache/{key}", tags=["Cache"], summary="Invalidate cache key")
async def cache_invalidate(key: str, node: str = Query("node1")):
    addr = NODE_ADDRESSES.get(node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {node}")
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.delete(f"http://{addr}/cache/{key}")
        return r.json()


@app.get("/cache/snapshot/all", tags=["Cache"], summary="Get cache snapshot from a node")
async def cache_snapshot(node: str = Query("node1")):
    addr = NODE_ADDRESSES.get(node)
    if not addr:
        raise HTTPException(404, f"Unknown node: {node}")
    result = await _get(addr, "/cache/snapshot/all")
    if not result:
        raise HTTPException(503, "Node unreachable")
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)
