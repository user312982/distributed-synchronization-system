"""
utils/metrics.py — Prometheus metrics collector
"""
from prometheus_client import Counter, Histogram, Gauge, Summary, start_http_server
import time

# ── Raft Metrics ──────────────────────────────────────────────────────────────
raft_leader_elections = Counter(
    "raft_leader_elections_total", "Total leader elections triggered"
)
raft_term_gauge = Gauge("raft_current_term", "Current Raft term", ["node_id"])
raft_role_gauge = Gauge("raft_node_role", "Node role (0=follower,1=candidate,2=leader)", ["node_id"])
raft_log_size = Gauge("raft_log_entries_total", "Total log entries", ["node_id"])
raft_commit_latency = Histogram(
    "raft_commit_latency_seconds",
    "Latency for log entry to be committed",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

# ── Lock Metrics ──────────────────────────────────────────────────────────────
lock_acquire_total = Counter(
    "lock_acquire_total", "Total lock acquire attempts", ["lock_type", "status"]
)
lock_wait_time = Histogram(
    "lock_wait_seconds",
    "Time waiting for lock acquisition",
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)
lock_held_gauge = Gauge("locks_held_total", "Number of locks currently held")
deadlock_detected = Counter("deadlock_detected_total", "Total deadlocks detected and resolved")

# ── Queue Metrics ─────────────────────────────────────────────────────────────
queue_enqueue_total = Counter("queue_enqueue_total", "Messages enqueued", ["queue_name"])
queue_dequeue_total = Counter("queue_dequeue_total", "Messages dequeued", ["queue_name"])
queue_redelivery_total = Counter("queue_redelivery_total", "Messages redelivered", ["queue_name"])
queue_depth_gauge = Gauge("queue_depth", "Current queue depth", ["queue_name", "node_id"])
queue_latency = Histogram(
    "queue_message_latency_seconds",
    "End-to-end message latency",
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

# ── Cache Metrics ─────────────────────────────────────────────────────────────
cache_hits = Counter("cache_hits_total", "Cache hits", ["node_id"])
cache_misses = Counter("cache_misses_total", "Cache misses", ["node_id"])
cache_invalidations = Counter("cache_invalidations_total", "Cache invalidations sent", ["node_id"])
cache_evictions = Counter("cache_evictions_total", "Cache evictions (LRU)", ["node_id"])
cache_size_gauge = Gauge("cache_size", "Current number of items in cache", ["node_id"])
cache_state_transitions = Counter(
    "cache_state_transitions_total",
    "MESI state transitions",
    ["node_id", "from_state", "to_state"],
)

# ── Node Metrics ──────────────────────────────────────────────────────────────
node_up = Gauge("node_up", "Node is running", ["node_id"])
rpc_requests_total = Counter(
    "rpc_requests_total", "RPC calls made", ["node_id", "endpoint", "status"]
)
rpc_latency = Histogram(
    "rpc_latency_seconds",
    "RPC call latency",
    ["endpoint"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)


def start_metrics_server(port: int) -> None:
    """Start Prometheus HTTP metrics server."""
    start_http_server(port)


class Timer:
    """Context manager for timing operations."""
    def __init__(self, histogram: Histogram, labels: dict | None = None):
        self.histogram = histogram
        self.labels = labels or {}
        self._start = 0.0

    def __enter__(self):
        self._start = time.monotonic()
        return self

    def __exit__(self, *_):
        elapsed = time.monotonic() - self._start
        if self.labels:
            self.histogram.labels(**self.labels).observe(elapsed)
        else:
            self.histogram.observe(elapsed)
