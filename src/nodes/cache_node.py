"""
nodes/cache_node.py — Distributed Cache with MESI Protocol

MESI States:
  M (Modified)  — Dirty, exclusive. No other cache has this line.
  E (Exclusive) — Clean, exclusive. No other cache has this line.
  S (Shared)    — Clean, possibly shared with other caches.
  I (Invalid)   — Line is invalid (stale).

Cache Replacement: LRU (O(1) via OrderedDict)
"""
import asyncio
import logging
import time
import json
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig
from src.utils.metrics import (
    cache_hits, cache_misses, cache_invalidations, cache_evictions,
    cache_size_gauge, cache_state_transitions, Timer
)

logger = logging.getLogger(__name__)


class MESIState(str, Enum):
    M = "M"   # Modified
    E = "E"   # Exclusive
    S = "S"   # Shared
    I = "I"   # Invalid


@dataclass
class CacheLine:
    key: str
    value: Any
    state: MESIState = MESIState.I
    version: int = 0
    last_accessed: float = field(default_factory=time.time)
    dirty: bool = False

    def to_dict(self) -> Dict:
        return {
            "key": self.key,
            "value": self.value,
            "state": self.state.value,
            "version": self.version,
            "last_accessed": self.last_accessed,
            "dirty": self.dirty,
        }


class LRUCache:
    """O(1) LRU cache using OrderedDict."""

    def __init__(self, max_size: int, node_id: str):
        self.max_size = max_size
        self.node_id = node_id
        self._cache: OrderedDict[str, CacheLine] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[CacheLine]:
        async with self._lock:
            if key not in self._cache:
                return None
            line = self._cache[key]
            if line.state == MESIState.I:
                return None  # Invalid — treat as miss
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            line.last_accessed = time.time()
            return line

    async def put(self, line: CacheLine) -> Optional[CacheLine]:
        """Insert or update. Returns evicted line if capacity exceeded."""
        async with self._lock:
            evicted = None
            if line.key in self._cache:
                self._cache.move_to_end(line.key)
            else:
                if len(self._cache) >= self.max_size:
                    # Evict LRU (first item)
                    _, evicted = self._cache.popitem(last=False)
                    cache_evictions.labels(node_id=self.node_id).inc()
                    logger.debug("[Cache:%s] Evicted LRU key=%s", self.node_id, evicted.key)
            self._cache[line.key] = line
            cache_size_gauge.labels(node_id=self.node_id).set(len(self._cache))
            return evicted

    async def invalidate(self, key: str) -> Optional[CacheLine]:
        async with self._lock:
            line = self._cache.get(key)
            if line:
                old_state = line.state
                line.state = MESIState.I
                cache_state_transitions.labels(
                    node_id=self.node_id, from_state=old_state.value, to_state="I"
                ).inc()
            return line

    async def update_state(self, key: str, new_state: MESIState, value: Any = None) -> bool:
        async with self._lock:
            line = self._cache.get(key)
            if not line:
                return False
            old_state = line.state
            line.state = new_state
            if value is not None:
                line.value = value
                line.version += 1
            cache_state_transitions.labels(
                node_id=self.node_id, from_state=old_state.value, to_state=new_state.value
            ).inc()
            return True

    async def keys(self) -> List[str]:
        async with self._lock:
            return list(self._cache.keys())

    async def snapshot(self) -> Dict:
        async with self._lock:
            return {k: v.to_dict() for k, v in self._cache.items()}

    def size(self) -> int:
        return len(self._cache)


class CacheNode(BaseNode):
    """
    Distributed cache node implementing MESI protocol.

    Protocol Summary:
      READ:
        Hit (M/E/S): serve from cache
        Miss (I / not present): fetch from "memory" (Redis/other), set E or S

      WRITE:
        Hit M: update locally, stay M
        Hit E: update locally, → M
        Hit S: broadcast Invalidate to all peers → M, update locally
        Miss:  broadcast Invalidate → fetch/create → M
    """
    def __init__(self, config: NodeConfig):
        super().__init__(config)
        self._lru = LRUCache(max_size=config.cache_max_size, node_id=config.node_id)
        self._redis: Optional[aioredis.Redis] = None
        self._lock = asyncio.Lock()
        self._setup_cache_routes()

    def _setup_cache_routes(self):
        self._app.router.add_get("/cache/{key}", self._http_read)
        self._app.router.add_put("/cache/{key}", self._http_write)
        self._app.router.add_delete("/cache/{key}", self._http_invalidate)
        self._app.router.add_get("/cache/snapshot/all", self._http_snapshot)

    # ── Public API (HTTP) ─────────────────────────────────────────────────────

    async def _http_read(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        result = await self.read(key)
        return web.json_response(result)

    async def _http_write(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        data = await request.json()
        value = data.get("value")
        result = await self.write(key, value)
        return web.json_response(result)

    async def _http_invalidate(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        await self._invalidate_local(key)
        return web.json_response({"status": "invalidated", "key": key})

    async def _http_snapshot(self, _: web.Request) -> web.Response:
        snap = await self._lru.snapshot()
        return web.json_response({"node_id": self.node_id, "cache": snap})

    # ── MESI Read ─────────────────────────────────────────────────────────────

    async def read(self, key: str) -> Dict:
        line = await self._lru.get(key)

        if line is not None:
            # Cache HIT
            cache_hits.labels(node_id=self.node_id).inc()
            return {"hit": True, "key": key, "value": line.value, "state": line.state.value}

        # Cache MISS — fetch from backing store
        cache_misses.labels(node_id=self.node_id).inc()
        value_raw = await self._redis.get(f"cache_store:{key}")
        value = json.loads(value_raw) if value_raw else None

        if value is None:
            return {"hit": False, "key": key, "value": None, "state": "I"}

        # Check if any peers have it (for S vs E decision)
        peer_has = await self._check_peers_have(key)

        new_state = MESIState.S if peer_has else MESIState.E
        line = CacheLine(key=key, value=value, state=new_state)
        await self._lru.put(line)

        cache_state_transitions.labels(
            node_id=self.node_id, from_state="I", to_state=new_state.value
        ).inc()

        return {"hit": False, "key": key, "value": value, "state": new_state.value}

    # ── MESI Write ────────────────────────────────────────────────────────────

    async def write(self, key: str, value: Any) -> Dict:
        line = await self._lru.get(key)

        if line is not None:
            old_state = line.state

            if old_state == MESIState.M:
                # Already modified exclusively — update directly
                line.value = value
                line.version += 1
                await self._redis.set(f"cache_store:{key}", json.dumps(value))
                return {"status": "ok", "key": key, "state": "M", "transition": "M→M"}

            elif old_state == MESIState.E:
                # Exclusive — upgrade to M
                await self._lru.update_state(key, MESIState.M, value)
                await self._redis.set(f"cache_store:{key}", json.dumps(value))
                return {"status": "ok", "key": key, "state": "M", "transition": "E→M"}

            elif old_state == MESIState.S:
                # Shared — must invalidate all peers first
                await self._broadcast_invalidate(key)
                await self._lru.update_state(key, MESIState.M, value)
                await self._redis.set(f"cache_store:{key}", json.dumps(value))
                return {"status": "ok", "key": key, "state": "M", "transition": "S→M"}

        # Miss — invalidate all peers, then load M
        await self._broadcast_invalidate(key)
        await self._redis.set(f"cache_store:{key}", json.dumps(value))
        new_line = CacheLine(key=key, value=value, state=MESIState.M, dirty=True)
        await self._lru.put(new_line)
        return {"status": "ok", "key": key, "state": "M", "transition": "I→M"}

    # ── Peer Operations ───────────────────────────────────────────────────────

    async def _broadcast_invalidate(self, key: str):
        """Send invalidate to all peers."""
        cache_invalidations.labels(node_id=self.node_id).inc()
        results = await self.bus.broadcast(
            self.peers, "cache_invalidate", {"key": key, "from_node": self.node_id}
        )
        logger.debug("[Cache:%s] Invalidated %s on peers: %s", self.node_id, key, list(results.keys()))

    async def _check_peers_have(self, key: str) -> bool:
        """Ask peers if they have a valid copy of key."""
        results = await self.bus.broadcast(
            self.peers, "cache_check", {"key": key}
        )
        return any(r and r.get("has", False) for r in results.values())

    async def _invalidate_local(self, key: str):
        await self._lru.invalidate(key)

    # ── RPC Handler ───────────────────────────────────────────────────────────

    async def handle_rpc(self, method: str, payload: dict) -> dict:
        if method == "cache_invalidate":
            key = payload["key"]
            await self._invalidate_local(key)
            return {"status": "invalidated", "key": key}

        elif method == "cache_check":
            key = payload["key"]
            line = await self._lru.get(key)
            return {"has": line is not None and line.state != MESIState.I, "key": key}

        elif method == "cache_update":
            # Another node wrote, we need to update our S copy if we have it
            key = payload["key"]
            value = payload.get("value")
            await self._lru.update_state(key, MESIState.S, value)
            return {"status": "updated", "key": key}

        return {"error": "unknown_rpc"}

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "cache_size": self._lru.size(),
            "max_size": self.config.cache_max_size,
            "protocol": "MESI",
            "replacement": "LRU",
        }

    async def on_start(self):
        self._redis = aioredis.from_url(self.config.redis_url, decode_responses=True)
        logger.info("[%s] CacheNode started (MESI, LRU max=%d)", self.node_id, self.config.cache_max_size)

    async def on_stop(self):
        if self._redis:
            await self._redis.aclose()
