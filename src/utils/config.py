"""
utils/config.py — Configuration loader from .env
"""
import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

load_dotenv()


def _get_peers() -> List[str]:
    raw = os.getenv("NODE_PEERS", "")
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass
class NodeConfig:
    node_id: str = field(default_factory=lambda: os.getenv("NODE_ID", "node1"))
    host: str = field(default_factory=lambda: os.getenv("NODE_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("NODE_PORT", "8001")))
    peers: List[str] = field(default_factory=_get_peers)

    # Redis
    redis_host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    redis_db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))

    # Raft
    election_timeout_min: int = field(default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "150")))
    election_timeout_max: int = field(default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "300")))
    heartbeat_interval: int = field(default_factory=lambda: int(os.getenv("RAFT_HEARTBEAT_INTERVAL", "50")))

    # Queue
    queue_virtual_nodes: int = field(default_factory=lambda: int(os.getenv("QUEUE_VIRTUAL_NODES", "150")))
    queue_delivery_timeout: int = field(default_factory=lambda: int(os.getenv("QUEUE_DELIVERY_TIMEOUT", "30")))
    queue_max_retry: int = field(default_factory=lambda: int(os.getenv("QUEUE_MAX_RETRY", "5")))

    # Cache
    cache_max_size: int = field(default_factory=lambda: int(os.getenv("CACHE_MAX_SIZE", "1000")))

    # Metrics
    metrics_port: int = field(default_factory=lambda: int(os.getenv("METRICS_PORT", "9090")))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # PBFT
    consensus_type: str = field(default_factory=lambda: os.getenv("CONSENSUS_TYPE", "raft").lower())
    is_malicious: bool = field(default_factory=lambda: os.getenv("IS_MALICIOUS", "false").lower() == "true")

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


# Singleton
config = NodeConfig()
