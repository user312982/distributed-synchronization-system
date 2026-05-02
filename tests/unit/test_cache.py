"""
tests/unit/test_cache.py — Unit tests for MESI Cache Coherence
"""
import asyncio
import pytest

from src.nodes.cache_node import LRUCache, CacheLine, MESIState, CacheNode
from src.utils.config import NodeConfig


# ── LRU Cache ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_lru_basic_put_get():
    cache = LRUCache(max_size=5, node_id="test")
    line = CacheLine(key="k1", value="v1", state=MESIState.E)
    await cache.put(line)
    result = await cache.get("k1")
    assert result is not None
    assert result.value == "v1"


@pytest.mark.asyncio
async def test_lru_miss_returns_none():
    cache = LRUCache(max_size=5, node_id="test")
    result = await cache.get("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_lru_eviction():
    cache = LRUCache(max_size=3, node_id="test")
    for i in range(4):
        await cache.put(CacheLine(key=f"k{i}", value=i, state=MESIState.E))

    # k0 should be evicted (LRU)
    result = await cache.get("k0")
    assert result is None
    assert cache.size() == 3


@pytest.mark.asyncio
async def test_lru_eviction_order():
    cache = LRUCache(max_size=3, node_id="test")
    await cache.put(CacheLine(key="k1", value=1, state=MESIState.E))
    await cache.put(CacheLine(key="k2", value=2, state=MESIState.E))
    await cache.put(CacheLine(key="k3", value=3, state=MESIState.E))

    # Access k1 to make it recently used
    await cache.get("k1")

    # Add k4 → k2 should be evicted (oldest unused)
    await cache.put(CacheLine(key="k4", value=4, state=MESIState.E))
    assert await cache.get("k2") is None
    assert await cache.get("k1") is not None
    assert await cache.get("k3") is not None
    assert await cache.get("k4") is not None


@pytest.mark.asyncio
async def test_lru_invalidate():
    cache = LRUCache(max_size=5, node_id="test")
    await cache.put(CacheLine(key="k1", value="v1", state=MESIState.S))
    await cache.invalidate("k1")
    result = await cache.get("k1")
    assert result is None  # Invalid state = treated as miss


@pytest.mark.asyncio
async def test_lru_state_transition():
    cache = LRUCache(max_size=5, node_id="test")
    await cache.put(CacheLine(key="k1", value="old", state=MESIState.S))
    await cache.update_state("k1", MESIState.M, "new")
    line = await cache.get("k1")
    assert line.state == MESIState.M
    assert line.value == "new"


# ── MESI State Machine ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_mesi_read_miss_sets_exclusive():
    """On read miss with no peers, should set E state."""
    CacheNode._memory_store = {"mykey": "myvalue"}

    config = NodeConfig()
    config.node_id = "cache-test"
    config.peers = []
    config.cache_max_size = 100

    node = CacheNode(config)
    result = await node.read("mykey")

    assert result["hit"] is False
    assert result["value"] == "myvalue"
    assert result["state"] == "E"  # No peers → Exclusive


@pytest.mark.asyncio
async def test_mesi_write_creates_modified():
    """Write should result in M state."""
    config = NodeConfig()
    config.node_id = "cache-test"
    config.peers = []
    config.cache_max_size = 100

    node = CacheNode(config)
    result = await node.write("writekey", "writevalue")

    assert result["state"] == "M"
    assert result["key"] == "writekey"


@pytest.mark.asyncio
async def test_mesi_read_hit():
    """Second read should be a cache hit."""
    CacheNode._memory_store = {"hitkey": "hitvalue"}

    config = NodeConfig()
    config.node_id = "cache-test"
    config.peers = []
    config.cache_max_size = 100

    node = CacheNode(config)
    await node.read("hitkey")   # Miss — loads into cache
    result = await node.read("hitkey")  # Should be hit now

    assert result["hit"] is True
    assert result["value"] == "hitvalue"
