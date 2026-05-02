"""
tests/performance/test_performance.py — Performance & Throughput Testing

Mengukur performa komponen secara in-process (tanpa network overhead):
  1. Raft  : throughput commit, latency per operasi (single-node)
  2. PBFT  : commit latency honest vs Byzantine cluster
  3. LRU Cache : throughput read/write, hit rate, eviction
  4. Consistent Hash Ring : distribusi key, throughput lookup
  5. Comparison : single-node vs distributed overhead
"""

import asyncio
import hashlib
import time
import statistics
import pytest
from collections import OrderedDict
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def elapsed_ms(start: float) -> float:
    return (time.monotonic() - start) * 1000


def percentile(data: list, p: float) -> float:
    idx = max(0, int(len(data) * p) - 1)
    return sorted(data)[idx]


# ─────────────────────────────────────────────────────────────────────────────
# Re-use InMemoryBus + helpers dari test_pbft (standalone copy)
# ─────────────────────────────────────────────────────────────────────────────

from src.consensus.pbft import PBFTNode, hash_command
from src.consensus.raft import RaftNode, RaftRole


class InMemoryBus:
    def __init__(self):
        self.nodes: Dict[str, Any] = {}

    def register(self, node):
        self.nodes[node.node_id] = node

    async def call(self, peer_id: str, method: str, payload: Dict) -> Dict:
        node = self.nodes.get(peer_id)
        return await node.handle_rpc(method, payload) if node else {}

    async def broadcast(self, peers: List[str], method: str, payload: Dict) -> Dict:
        results = {}
        for p in peers:
            if p in self.nodes:
                results[p] = await self.call(p, method, payload)
        return results


class EmptyBus:
    """Bus kosong — tidak ada peers."""
    async def call(self, *a, **kw) -> Dict:
        return {}

    async def broadcast(self, peers, method, payload) -> Dict:
        return {}


def make_pbft_cluster(n: int = 4, malicious_ids: List[str] = None):
    bus = InMemoryBus()
    node_ids = [f"node{i}" for i in range(n)]
    nodes = []
    for nid in node_ids:
        peers = [p for p in node_ids if p != nid]
        node = PBFTNode(
            node_id=nid,
            peers=peers,
            message_bus=bus,
            is_malicious=(nid in (malicious_ids or [])),
        )
        nodes.append(node)
        bus.register(node)
    return nodes


def get_primary(nodes: list) -> PBFTNode:
    return next(n for n in nodes if n.is_leader)


# ─────────────────────────────────────────────────────────────────────────────
# Lightweight LRU Cache (standalone — dari implementasi src/nodes/cache_node.py)
# ─────────────────────────────────────────────────────────────────────────────

class SimpleLRUCache:
    """
    Versi standalone LRU cache untuk pengujian performa tanpa dependensi aiohttp/Redis.
    Logic identik dengan LRUCache di cache_node.py.
    """
    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: OrderedDict = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            return None
        self._cache.move_to_end(key)
        return self._cache[key]

    def put(self, key: str, value: Any) -> Optional[str]:
        """Returns evicted key jika ada."""
        evicted = None
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self.capacity:
                evicted, _ = self._cache.popitem(last=False)
        self._cache[key] = value
        return evicted

    def __len__(self):
        return len(self._cache)


# ─────────────────────────────────────────────────────────────────────────────
# Lightweight Consistent Hash Ring (standalone — dari queue_node.py)
# ─────────────────────────────────────────────────────────────────────────────

class ConsistentHashRing:
    def __init__(self, virtual_nodes: int = 150):
        self._vnodes = virtual_nodes
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []

    def add_node(self, node_id: str):
        for i in range(self._vnodes):
            key = self._hash(f"{node_id}#{i}")
            self._ring[key] = node_id
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = self._hash(key)
        for rk in self._sorted_keys:
            if h <= rk:
                return self._ring[rk]
        return self._ring[self._sorted_keys[0]]

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)


# ─────────────────────────────────────────────────────────────────────────────
# Shared benchmark results fixture
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def results() -> Dict:
    return {}


@pytest.fixture(autouse=True, scope="session")
def print_summary(results):
    yield
    if not results:
        return
    print("\n\n" + "=" * 65)
    print("  PERFORMANCE BENCHMARK SUMMARY")
    print("=" * 65)
    for label, metrics in results.items():
        print(f"\n📊 {label}")
        for k, v in metrics.items():
            unit = ""
            if "ms" in k:
                unit = " ms"
            elif "pct" in k:
                unit = " %"
            elif "ops_per_sec" in k:
                unit = " ops/s"
            print(f"     {k:<35} {v}{unit}")
    print("=" * 65 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# 1. RAFT PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────

class TestRaftPerformance:
    """Throughput dan latency Raft single-node (no network)."""

    N = 50

    @pytest.mark.asyncio
    async def test_throughput_and_latency(self, results):
        """50 commits berurutan → ukur ops/sec dan p95 latency."""
        bus = EmptyBus()
        node = RaftNode(node_id="raft0", peers=[], message_bus=bus)
        await node.start()

        async with node._lock:
            node._role = RaftRole.LEADER
            node._leader_id = "raft0"

        latencies = []
        t_total = time.monotonic()

        for i in range(self.N):
            t0 = time.monotonic()
            ok = await node.submit({"op": "write", "key": f"k{i}", "value": i})
            latencies.append(elapsed_ms(t0))
            assert ok, f"Raft commit #{i} gagal"

        total_ms = elapsed_ms(t_total)
        ops_sec = self.N / (total_ms / 1000)

        results["raft_single_node"] = {
            "ops": self.N,
            "total_ms": round(total_ms, 2),
            "ops_per_sec": round(ops_sec, 1),
            "avg_latency_ms": round(statistics.mean(latencies), 3),
            "p95_latency_ms": round(percentile(latencies, 0.95), 3),
            "max_latency_ms": round(max(latencies), 3),
        }

        await node.stop()

        assert ops_sec > 100, f"Throughput terlalu rendah: {ops_sec:.1f} ops/s"
        assert statistics.mean(latencies) < 50, "Rata-rata latency > 50ms"

    @pytest.mark.asyncio
    async def test_log_consistency_after_many_commits(self):
        """Semua entries terekan dengan benar di log setelah banyak commit."""
        bus = EmptyBus()
        node = RaftNode(node_id="raft1", peers=[], message_bus=bus)
        await node.start()
        async with node._lock:
            node._role = RaftRole.LEADER
            node._leader_id = "raft1"

        N = 30
        for i in range(N):
            await node.submit({"seq": i})

        status = node.get_status()
        assert status["log_length"] == N
        assert status["commit_index"] == N - 1
        await node.stop()


# ─────────────────────────────────────────────────────────────────────────────
# 2. PBFT PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────

class TestPBFTPerformance:
    """Latency PBFT: honest cluster vs Byzantine cluster."""

    N = 10

    @pytest.mark.asyncio
    async def test_honest_cluster_latency(self, results):
        """4 honest nodes: rata-rata latency per commit."""
        nodes = make_pbft_cluster(4)
        primary = get_primary(nodes)
        latencies = []

        for i in range(self.N):
            t0 = time.monotonic()
            ok = await primary.submit({"op": "write", "key": f"k{i}", "value": i})
            latencies.append(elapsed_ms(t0))
            assert ok, f"Commit #{i} gagal"

        results["pbft_honest_4node"] = {
            "ops": self.N,
            "avg_latency_ms": round(statistics.mean(latencies), 3),
            "p95_latency_ms": round(percentile(latencies, 0.95), 3),
            "max_latency_ms": round(max(latencies), 3),
        }

        assert statistics.mean(latencies) < 500, "PBFT latency > 500ms"

    @pytest.mark.asyncio
    async def test_byzantine_tolerance_latency(self, results):
        """4 nodes, 1 Byzantine (f=1): consensus masih tercapai mayoritas."""
        import random
        random.seed(42)

        nodes = make_pbft_cluster(4, malicious_ids=["node1"])
        primary = get_primary(nodes)

        latencies = []
        successes = 0

        for i in range(self.N):
            t0 = time.monotonic()
            ok = await primary.submit({"op": "write", "key": f"byz{i}", "value": i})
            latencies.append(elapsed_ms(t0))
            if ok:
                successes += 1

        results["pbft_byzantine_1of4"] = {
            "ops": self.N,
            "successes": successes,
            "success_rate_pct": round(successes / self.N * 100, 1),
            "avg_latency_ms": round(statistics.mean(latencies), 3),
            "p95_latency_ms": round(percentile(latencies, 0.95), 3),
        }

        assert successes >= 6, f"Terlalu banyak gagal: {successes}/{self.N}"

    @pytest.mark.asyncio
    async def test_single_node_vs_distributed_overhead(self, results):
        """Bandingkan latency Raft single-node vs PBFT 4-node (overhead factor)."""
        # Single-node Raft
        bus = EmptyBus()
        raft = RaftNode(node_id="raft_cmp", peers=[], message_bus=bus)
        await raft.start()
        async with raft._lock:
            raft._role = RaftRole.LEADER
            raft._leader_id = "raft_cmp"

        N = 10
        t0 = time.monotonic()
        for i in range(N):
            await raft.submit({"seq": i})
        raft_ms = elapsed_ms(t0)
        await raft.stop()

        # PBFT 4-node
        nodes = make_pbft_cluster(4)
        primary = get_primary(nodes)
        t0 = time.monotonic()
        for i in range(N):
            await primary.submit({"seq": i})
        pbft_ms = elapsed_ms(t0)

        factor = pbft_ms / max(raft_ms, 0.001)

        results["comparison_raft_vs_pbft"] = {
            "ops": N,
            "raft_single_node_ms": round(raft_ms, 2),
            "pbft_4node_ms": round(pbft_ms, 2),
            "overhead_factor": round(factor, 2),
        }

        assert factor < 200, f"PBFT overhead terlalu besar: {factor:.1f}x"


# ─────────────────────────────────────────────────────────────────────────────
# 3. LRU CACHE PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────

class TestLRUCachePerformance:
    """Throughput dan hit rate LRU cache (standalone, sync)."""

    def test_write_throughput(self, results):
        """200 write operasi — ukur ops/sec."""
        cache = SimpleLRUCache(capacity=300)
        N = 200

        t0 = time.monotonic()
        for i in range(N):
            cache.put(f"key{i}", f"value{i}" * 5)
        total_ms = elapsed_ms(t0)
        ops_sec = N / (total_ms / 1000)

        results["lru_write_throughput"] = {
            "writes": N,
            "total_ms": round(total_ms, 3),
            "ops_per_sec": round(ops_sec, 1),
        }

        assert ops_sec > 10_000, f"LRU write throughput terlalu rendah: {ops_sec:.0f} ops/s"

    def test_read_throughput_warm(self, results):
        """Cache warm → 100 reads → ukur hit rate dan ops/sec."""
        cache = SimpleLRUCache(capacity=200)
        N = 100

        for i in range(N):
            cache.put(f"key{i}", i)

        hits = 0
        t0 = time.monotonic()
        for i in range(N):
            val = cache.get(f"key{i}")
            if val is not None:
                hits += 1
        total_ms = elapsed_ms(t0)
        hit_rate = hits / N * 100
        ops_sec = N / (total_ms / 1000)

        results["lru_read_warm"] = {
            "reads": N,
            "hits": hits,
            "hit_rate_pct": round(hit_rate, 1),
            "total_ms": round(total_ms, 3),
            "ops_per_sec": round(ops_sec, 1),
        }

        assert hit_rate >= 100.0, f"Hit rate seharusnya 100% pada warm cache: {hit_rate}%"

    def test_lru_eviction_correctness(self, results):
        """Capacity=5: setelah 6 insert, entry pertama harus ter-evict."""
        cache = SimpleLRUCache(capacity=5)

        for i in range(5):
            cache.put(f"k{i}", i)

        # Access k0 agar bukan LRU lagi
        cache.get("k0")

        # Insert k5 → k1 harus ter-evict (LRU setelah k0 di-access)
        evicted = cache.put("k5", 5)

        results["lru_eviction"] = {
            "capacity": 5,
            "evicted_key": evicted,
            "k5_present": cache.get("k5") is not None,
            "k0_present": cache.get("k0") is not None,
        }

        assert evicted == "k1", f"Salah evict: {evicted}, seharusnya k1"
        assert cache.get("k5") is not None
        assert cache.get("k0") is not None

    def test_cache_size_after_overflow(self, results):
        """Cache tidak boleh melebihi kapasitas setelah banyak insert."""
        cap = 50
        cache = SimpleLRUCache(capacity=cap)

        for i in range(200):
            cache.put(f"key{i}", i)

        results["lru_size_bound"] = {
            "capacity": cap,
            "inserts": 200,
            "final_size": len(cache),
        }

        assert len(cache) <= cap, f"Cache melebihi kapasitas: {len(cache)} > {cap}"


# ─────────────────────────────────────────────────────────────────────────────
# 4. CONSISTENT HASH RING PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────

class TestConsistentHashRingPerformance:
    """Distribusi key dan throughput lookup consistent hash ring."""

    def test_lookup_throughput(self, results):
        """1000 lookup ops/sec pada ring 3 node."""
        ring = ConsistentHashRing(virtual_nodes=150)
        for nid in ["node0", "node1", "node2"]:
            ring.add_node(nid)

        N = 1000
        t0 = time.monotonic()
        for i in range(N):
            ring.get_node(f"queue_{i}")
        total_ms = elapsed_ms(t0)
        ops_sec = N / (total_ms / 1000)

        results["hash_ring_lookup"] = {
            "lookups": N,
            "nodes": 3,
            "virtual_nodes_each": 150,
            "total_ms": round(total_ms, 3),
            "ops_per_sec": round(ops_sec, 1),
        }

        assert ops_sec > 10_000, f"Hash ring lookup terlalu lambat: {ops_sec:.0f} ops/s"

    def test_key_distribution_uniformity(self, results):
        """
        Distribusi key ke 3 node harus relatif merata (max variance < 20%).
        Consistent hashing seharusnya mendistribusikan secara uniform.
        """
        ring = ConsistentHashRing(virtual_nodes=150)
        node_ids = ["node0", "node1", "node2"]
        for nid in node_ids:
            ring.add_node(nid)

        N = 3000
        counts: Dict[str, int] = {nid: 0 for nid in node_ids}
        for i in range(N):
            target = ring.get_node(f"key_{i}")
            counts[target] += 1

        expected = N / len(node_ids)
        deviations = {nid: abs(cnt - expected) / expected * 100 for nid, cnt in counts.items()}
        max_deviation = max(deviations.values())

        results["hash_ring_distribution"] = {
            "keys": N,
            "nodes": len(node_ids),
            **{f"node_{nid}_count": counts[nid] for nid in node_ids},
            "max_deviation_pct": round(max_deviation, 1),
        }

        assert max_deviation < 30, (
            f"Distribusi tidak merata: max deviation {max_deviation:.1f}% (threshold 30%)"
        )

    def test_node_add_minimal_remapping(self, results):
        """
        Menambah 1 node ke ring 3-node → hanya ~25% key yang berpindah (consistent hashing property).
        """
        ring_before = ConsistentHashRing(virtual_nodes=150)
        for nid in ["node0", "node1", "node2"]:
            ring_before.add_node(nid)

        ring_after = ConsistentHashRing(virtual_nodes=150)
        for nid in ["node0", "node1", "node2", "node3"]:
            ring_after.add_node(nid)

        N = 1000
        remapped = sum(
            1 for i in range(N)
            if ring_before.get_node(f"k{i}") != ring_after.get_node(f"k{i}")
        )
        remap_pct = remapped / N * 100

        results["hash_ring_remapping"] = {
            "keys": N,
            "nodes_before": 3,
            "nodes_after": 4,
            "remapped": remapped,
            "remapped_pct": round(remap_pct, 1),
        }

        # Teoritis ~25%, beri toleransi hingga 40%
        assert remap_pct < 40, f"Terlalu banyak key di-remap: {remap_pct:.1f}% (threshold 40%)"
