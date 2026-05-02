"""
nodes/queue_node.py — Distributed Queue System

Features:
  - Consistent hashing ring for message routing (virtual nodes)
  - Multiple producers and consumers
  - Message persistence to Redis (before ACK)
  - Node failure recovery (re-read from Redis on restart)
  - At-least-once delivery guarantee (timeout + retry)
"""
import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig
from src.utils.metrics import (
    queue_enqueue_total, queue_dequeue_total, queue_redelivery_total,
    queue_depth_gauge, queue_latency, Timer
)

logger = logging.getLogger(__name__)


# ── Consistent Hash Ring ──────────────────────────────────────────────────────

class ConsistentHashRing:
    """
    Consistent hash ring with virtual nodes.
    Maps queue names / keys to node IDs.
    """

    def __init__(self, virtual_nodes: int = 150):
        self._vnodes = virtual_nodes
        self._ring: Dict[int, str] = {}          # hash → node_id
        self._sorted_keys: List[int] = []

    def add_node(self, node_id: str):
        for i in range(self._vnodes):
            key = self._hash(f"{node_id}#{i}")
            self._ring[key] = node_id
        self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node_id: str):
        for i in range(self._vnodes):
            key = self._hash(f"{node_id}#{i}")
            self._ring.pop(key, None)
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = self._hash(key)
        for ring_key in self._sorted_keys:
            if h <= ring_key:
                return self._ring[ring_key]
        return self._ring[self._sorted_keys[0]]  # wrap around

    def get_all_nodes(self) -> List[str]:
        return list(set(self._ring.values()))

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def debug_ring(self) -> Dict:
        nodes = {}
        for node_id in set(self._ring.values()):
            nodes[node_id] = sum(1 for v in self._ring.values() if v == node_id)
        return nodes


# ── Message ───────────────────────────────────────────────────────────────────

@dataclass
class Message:
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    queue: str = ""
    body: Any = None
    producer_id: str = ""
    created_at: float = field(default_factory=time.time)
    delivery_count: int = 0
    last_delivered_at: float = 0.0

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "Message":
        return cls(**d)


# ── Queue Node ────────────────────────────────────────────────────────────────

class QueueNode(BaseNode):
    """
    Distributed queue node.

    - Stores messages locally (in-memory + Redis for persistence)
    - Routes enqueue requests to the correct node via consistent hashing
    - At-least-once delivery: unacked messages re-delivered after timeout
    """

    def __init__(self, config: NodeConfig):
        super().__init__(config)
        self._ring = ConsistentHashRing(virtual_nodes=config.queue_virtual_nodes)
        self._redis: Optional[aioredis.Redis] = None
        self._queues: Dict[str, List[Message]] = {}          # queue → [messages]
        self._in_flight: Dict[str, Dict[str, Message]] = {}  # queue → {msg_id → msg}
        self._delivery_timeout = config.queue_delivery_timeout
        self._max_retry = config.queue_max_retry
        self._lock = asyncio.Lock()
        self._redelivery_task: Optional[asyncio.Task] = None
        self._setup_queue_routes()

    def _setup_queue_routes(self):
        self._app.router.add_post("/queue/enqueue", self._http_enqueue)
        self._app.router.add_post("/queue/dequeue", self._http_dequeue)
        self._app.router.add_post("/queue/ack", self._http_ack)
        self._app.router.add_get("/queue/stats", self._http_stats)
        # Internal peer RPC
        self._app.router.add_post("/queue/internal/enqueue", self._http_internal_enqueue)

    async def on_start(self):
        # Connect Redis
        self._redis = aioredis.from_url(self.config.redis_url, decode_responses=True)

        # Initialize hash ring with all nodes
        all_nodes = [self.node_id] + self.peers
        for node in all_nodes:
            self._ring.add_node(node)

        # Recover unacked messages from Redis
        await self._recover_from_redis()

        # Start redelivery checker
        self._redelivery_task = asyncio.create_task(self._redelivery_loop())
        logger.info("[%s] QueueNode started. Ring: %s", self.node_id, self._ring.debug_ring())

    async def on_stop(self):
        if self._redelivery_task:
            self._redelivery_task.cancel()
        if self._redis:
            await self._redis.aclose()

    async def handle_rpc(self, method: str, payload: dict) -> dict:
        """Handle internal RPC calls from peer nodes."""
        if method == "queue_enqueue":
            return await self._internal_enqueue(payload)
        elif method == "peer_status_update":
            # Update ring on node join/leave
            action = payload.get("action")
            node = payload.get("node_id")
            if action == "join":
                self._ring.add_node(node)
            elif action == "leave":
                self._ring.remove_node(node)
            return {"status": "ok"}
        return {"error": "unknown_rpc"}

    # ── HTTP Handlers ─────────────────────────────────────────────────────────

    async def _http_enqueue(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data.get("queue", "default")
        body = data.get("body")
        producer_id = data.get("producer_id", "unknown")

        # Route to correct node
        target = self._ring.get_node(queue_name)
        if target != self.node_id:
            # Forward to correct node
            peer_host = target  # "node2:8001" format
            result = await self.bus.call(peer_host, "queue_enqueue", {
                "queue": queue_name, "body": body, "producer_id": producer_id
            })
            if result:
                return web.json_response(result)
            return web.json_response({"error": "forward_failed"}, status=503)

        result = await self._internal_enqueue({
            "queue": queue_name, "body": body, "producer_id": producer_id
        })
        return web.json_response(result)

    async def _http_internal_enqueue(self, request: web.Request) -> web.Response:
        data = await request.json()
        result = await self._internal_enqueue(data)
        return web.json_response(result)

    async def _internal_enqueue(self, data: Dict) -> Dict:
        queue_name = data.get("queue", "default")
        msg = Message(
            queue=queue_name,
            body=data.get("body"),
            producer_id=data.get("producer_id", "unknown"),
        )

        # Persist to Redis BEFORE acknowledging
        await self._persist_message(msg)

        async with self._lock:
            if queue_name not in self._queues:
                self._queues[queue_name] = []
            self._queues[queue_name].append(msg)
            depth = len(self._queues[queue_name])

        queue_enqueue_total.labels(queue_name=queue_name).inc()
        queue_depth_gauge.labels(queue_name=queue_name, node_id=self.node_id).set(depth)
        logger.debug("[%s] Enqueued msg %s to %s", self.node_id, msg.msg_id, queue_name)
        return {"status": "ok", "msg_id": msg.msg_id, "queue": queue_name}

    async def _http_dequeue(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data.get("queue", "default")
        consumer_id = data.get("consumer_id", "unknown")

        target = self._ring.get_node(queue_name)
        if target != self.node_id:
            result = await self.bus.call(target, "queue_dequeue", {
                "queue": queue_name, "consumer_id": consumer_id
            })
            if result:
                return web.json_response(result)
            return web.json_response({"error": "forward_failed"}, status=503)

        async with self._lock:
            q = self._queues.get(queue_name, [])
            if not q:
                return web.json_response({"status": "empty"})

            msg = q.pop(0)
            msg.delivery_count += 1
            msg.last_delivered_at = time.time()

            if queue_name not in self._in_flight:
                self._in_flight[queue_name] = {}
            self._in_flight[queue_name][msg.msg_id] = msg
            depth = len(q)

        queue_dequeue_total.labels(queue_name=queue_name).inc()
        queue_depth_gauge.labels(queue_name=queue_name, node_id=self.node_id).set(depth)

        # Track latency (enqueue → dequeue)
        latency = time.time() - msg.created_at
        queue_latency.observe(latency)

        return web.json_response({
            "msg_id": msg.msg_id,
            "queue": queue_name,
            "body": msg.body,
            "delivery_count": msg.delivery_count,
        })

    async def _http_ack(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data.get("queue", "default")
        msg_id = data.get("msg_id")

        async with self._lock:
            in_flight = self._in_flight.get(queue_name, {})
            msg = in_flight.pop(msg_id, None)

        if msg:
            # Remove from Redis persistence
            await self._remove_persisted(msg)
            return web.json_response({"status": "acked", "msg_id": msg_id})
        return web.json_response({"status": "not_found"}, status=404)

    async def _http_stats(self, _: web.Request) -> web.Response:
        stats = {}
        async with self._lock:
            for q, msgs in self._queues.items():
                stats[q] = {
                    "depth": len(msgs),
                    "in_flight": len(self._in_flight.get(q, {})),
                    "owner_node": self._ring.get_node(q),
                }
        return web.json_response({
            "node_id": self.node_id,
            "queues": stats,
            "ring": self._ring.debug_ring(),
        })

    # ── Persistence ───────────────────────────────────────────────────────────

    async def _persist_message(self, msg: Message):
        key = f"queue:{self.node_id}:{msg.queue}:{msg.msg_id}"
        await self._redis.set(key, json.dumps(msg.to_dict()), ex=3600)

    async def _remove_persisted(self, msg: Message):
        key = f"queue:{self.node_id}:{msg.queue}:{msg.msg_id}"
        await self._redis.delete(key)

    async def _recover_from_redis(self):
        """On restart: reload unacked messages from Redis."""
        pattern = f"queue:{self.node_id}:*"
        keys = await self._redis.keys(pattern)
        recovered = 0
        for key in keys:
            raw = await self._redis.get(key)
            if raw:
                try:
                    msg = Message.from_dict(json.loads(raw))
                    async with self._lock:
                        if msg.queue not in self._queues:
                            self._queues[msg.queue] = []
                        self._queues[msg.queue].append(msg)
                    recovered += 1
                except Exception as e:
                    logger.error("Recovery error for key %s: %s", key, e)
        if recovered:
            logger.info("[%s] Recovered %d messages from Redis", self.node_id, recovered)

    # ── Redelivery ────────────────────────────────────────────────────────────

    async def _redelivery_loop(self):
        """At-least-once: re-enqueue in-flight messages that exceed ack timeout."""
        while True:
            await asyncio.sleep(5.0)
            now = time.time()
            to_redeliver = []

            async with self._lock:
                for queue_name, inflight in self._in_flight.items():
                    for msg_id, msg in list(inflight.items()):
                        age = now - msg.last_delivered_at
                        if age > self._delivery_timeout:
                            if msg.delivery_count >= self._max_retry:
                                # Dead-letter: remove from in-flight
                                logger.warning("[%s] DLQ: msg %s exceeded max retries", self.node_id, msg_id)
                                inflight.pop(msg_id)
                                asyncio.create_task(self._remove_persisted(msg))
                            else:
                                to_redeliver.append((queue_name, msg))
                                inflight.pop(msg_id)

            for queue_name, msg in to_redeliver:
                async with self._lock:
                    if queue_name not in self._queues:
                        self._queues[queue_name] = []
                    self._queues[queue_name].insert(0, msg)  # front for priority
                queue_redelivery_total.labels(queue_name=queue_name).inc()
                logger.info("[%s] Redelivering msg %s (attempt %d)", self.node_id, msg.msg_id, msg.delivery_count)

    def get_status(self) -> Dict:
        queues_info = {}
        for q, msgs in self._queues.items():
            queues_info[q] = {"depth": len(msgs), "in_flight": len(self._in_flight.get(q, {}))}
        return {
            "node_id": self.node_id,
            "queues": queues_info,
            "ring_nodes": self._ring.debug_ring(),
        }
