"""
nodes/lock_manager.py — Distributed Lock Manager via Raft Consensus

Features:
  - Shared (READ) and Exclusive (WRITE) locks
  - Lock state replicated through Raft log
  - Deadlock detection using Wait-For Graph (DFS cycle detection)
  - Automatic victim selection (youngest txn)
  - Network partition safety (locks only granted by leader)
"""
import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Set

from src.consensus.raft import RaftNode, LogEntry
from src.consensus.pbft import PBFTNode
from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig
from src.utils.metrics import (
    lock_acquire_total, lock_wait_time, lock_held_gauge,
    deadlock_detected, Timer
)

logger = logging.getLogger(__name__)


class LockType(str, Enum):
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


class LockStatus(str, Enum):
    GRANTED = "GRANTED"
    WAITING = "WAITING"
    RELEASED = "RELEASED"
    DENIED = "DENIED"
    DEADLOCK = "DEADLOCK"


@dataclass
class LockRequest:
    txn_id: str
    resource: str
    lock_type: LockType
    requested_at: float = field(default_factory=time.time)
    status: LockStatus = LockStatus.WAITING


@dataclass
class LockEntry:
    resource: str
    holders: Dict[str, LockType] = field(default_factory=dict)   # txn_id → type
    waiters: List[LockRequest] = field(default_factory=list)


class WaitForGraph:
    """Detects deadlocks in distributed lock manager via DFS cycle detection."""

    def __init__(self):
        # txn_id → set of txn_ids it's waiting for
        self._edges: Dict[str, Set[str]] = defaultdict(set)

    def add_wait(self, waiter: str, *holders: str):
        for holder in holders:
            if holder != waiter:
                self._edges[waiter].add(holder)

    def remove_txn(self, txn_id: str):
        self._edges.pop(txn_id, None)
        for deps in self._edges.values():
            deps.discard(txn_id)

    def detect_cycle(self) -> Optional[List[str]]:
        """Returns cycle path if deadlock found, else None."""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def dfs(node: str, path: List[str]) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for neighbor in self._edges.get(node, set()):
                if neighbor not in visited:
                    result = dfs(neighbor, path)
                    if result:
                        return result
                elif neighbor in rec_stack:
                    # Found cycle — return the cycle portion
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:]
            rec_stack.discard(node)
            path.pop()
            return None

        for node in list(self._edges.keys()):
            if node not in visited:
                result = dfs(node, [])
                if result:
                    return result
        return None


class LockStateMachine:
    """
    In-memory lock state applied from Raft log entries.
    Thread-safe via asyncio.Lock.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._locks: Dict[str, LockEntry] = {}
        self._wfg = WaitForGraph()
        self._lock = asyncio.Lock()
        # Futures waiting for lock grant: (txn_id, resource) → Future
        self._pending: Dict[str, asyncio.Future] = {}

    async def apply(self, command: Dict) -> Dict:
        """Apply a log entry command to the lock state machine."""
        op = command.get("op")
        if op == "acquire":
            return await self._acquire(command)
        elif op == "release":
            return await self._release(command)
        return {"error": "unknown_op"}

    async def _acquire(self, cmd: Dict) -> Dict:
        async with self._lock:
            txn_id = cmd["txn_id"]
            resource = cmd["resource"]
            lock_type = LockType(cmd["lock_type"])

            entry = self._locks.setdefault(resource, LockEntry(resource=resource))

            # Check compatibility
            if self._compatible(entry, txn_id, lock_type):
                entry.holders[txn_id] = lock_type
                lock_held_gauge.set(self._total_locks())
                lock_acquire_total.labels(lock_type=lock_type.value, status="granted").inc()
                return {"status": LockStatus.GRANTED, "txn_id": txn_id, "resource": resource}

            # Not compatible — add to waiters
            req = LockRequest(txn_id=txn_id, resource=resource, lock_type=lock_type)
            entry.waiters.append(req)
            # Update WFG
            self._wfg.add_wait(txn_id, *entry.holders.keys())

            # Deadlock check
            cycle = self._wfg.detect_cycle()
            if cycle:
                # Abort youngest (most recent) transaction in cycle
                victim = self._select_victim(cycle, entry)
                deadlock_detected.inc()
                logger.warning("[LockMgr] Deadlock detected! Cycle=%s, Victim=%s", cycle, victim)
                entry.waiters = [w for w in entry.waiters if w.txn_id != victim]
                self._wfg.remove_txn(victim)
                if victim == txn_id:
                    lock_acquire_total.labels(lock_type=lock_type.value, status="deadlock").inc()
                    return {"status": LockStatus.DEADLOCK, "txn_id": txn_id}

            lock_acquire_total.labels(lock_type=lock_type.value, status="waiting").inc()
            return {"status": LockStatus.WAITING, "txn_id": txn_id, "resource": resource}

    async def _release(self, cmd: Dict) -> Dict:
        async with self._lock:
            txn_id = cmd["txn_id"]
            resource = cmd["resource"]

            entry = self._locks.get(resource)
            if not entry or txn_id not in entry.holders:
                return {"status": "not_held"}

            del entry.holders[txn_id]
            self._wfg.remove_txn(txn_id)
            lock_held_gauge.set(self._total_locks())

            # Try to grant waiting requests
            granted = []
            remaining = []
            for waiter in entry.waiters:
                if self._compatible(entry, waiter.txn_id, waiter.lock_type):
                    entry.holders[waiter.txn_id] = waiter.lock_type
                    granted.append(waiter.txn_id)
                    lock_acquire_total.labels(lock_type=waiter.lock_type.value, status="granted").inc()
                else:
                    remaining.append(waiter)
            entry.waiters = remaining

            # Notify pending futures
            for txn in granted:
                key = f"{txn}:{resource}"
                fut = self._pending.get(key)
                if fut and not fut.done():
                    fut.set_result(LockStatus.GRANTED)

            return {"status": LockStatus.RELEASED, "txn_id": txn_id, "granted": granted}

    def _compatible(self, entry: LockEntry, txn_id: str, lock_type: LockType) -> bool:
        if not entry.holders:
            return True
        if lock_type == LockType.SHARED:
            # OK if all holders are SHARED
            return all(t == LockType.SHARED for t in entry.holders.values())
        # EXCLUSIVE — only OK if this txn already holds it (upgrade) or no holders
        return len(entry.holders) == 0 or (len(entry.holders) == 1 and txn_id in entry.holders)

    def _select_victim(self, cycle: List[str], entry: LockEntry) -> str:
        # Select txn that has waited the shortest time (youngest)
        waiter_map = {w.txn_id: w.requested_at for w in entry.waiters}
        cycle_waiters = [(txn, waiter_map.get(txn, 0)) for txn in cycle]
        return max(cycle_waiters, key=lambda x: x[1])[0]  # most recent = largest timestamp

    def _total_locks(self) -> int:
        return sum(len(e.holders) for e in self._locks.values())

    def get_state(self) -> Dict:
        return {
            resource: {
                "holders": {txn: lt.value for txn, lt in entry.holders.items()},
                "waiters": [{"txn_id": w.txn_id, "type": w.lock_type.value} for w in entry.waiters],
            }
            for resource, entry in self._locks.items()
        }


class LockManagerNode(BaseNode):
    """
    Distributed Lock Manager node.
    Exposes REST + RPC API for acquiring/releasing distributed locks.
    """

    def __init__(self, config: NodeConfig):
        super().__init__(config)
        self._sm = LockStateMachine(config.node_id)
        self._consensus = None
        # txn_id → asyncio.Future for waiting clients
        self._waiting: Dict[str, asyncio.Future] = {}
        self._setup_lock_routes()

    def _setup_lock_routes(self):
        self._app.router.add_post("/lock/acquire", self._http_acquire)
        self._app.router.add_post("/lock/release", self._http_release)
        self._app.router.add_get("/lock/state", self._http_state)

    async def on_start(self):
        if self.config.consensus_type == "pbft":
            self._consensus = PBFTNode(
                node_id=self.node_id,
                peers=self.peers,
                message_bus=self.bus,
                on_commit=self._on_raft_commit,
                is_malicious=self.config.is_malicious,
            )
        else:
            self._consensus = RaftNode(
                node_id=self.node_id,
                peers=self.peers,
                message_bus=self.bus,
                on_commit=self._on_raft_commit,
                election_timeout_ms=(
                    self.config.election_timeout_min,
                    self.config.election_timeout_max,
                ),
                heartbeat_ms=self.config.heartbeat_interval,
            )
        await self._consensus.start()

    async def on_stop(self):
        if self._consensus:
            await self._consensus.stop()

    def _on_raft_commit(self, entry: LogEntry):
        """Apply committed Raft entries to the lock state machine."""
        asyncio.create_task(self._apply_entry(entry))

    async def _apply_entry(self, entry: LogEntry):
        result = await self._sm.apply(entry.command)
        txn_id = entry.command.get("txn_id", "")
        key = f"{txn_id}:{entry.command.get('resource', '')}"
        fut = self._waiting.get(key)
        if fut and not fut.done():
            fut.set_result(result)

    async def handle_rpc(self, method: str, payload: dict) -> dict:
        if method in ("request_vote", "append_entries", "pre_prepare", "prepare", "commit"):
            return await self._consensus.handle_rpc(method, payload)
        return {"error": "unknown_rpc"}

    # ── HTTP Handlers ─────────────────────────────────────────────────────────

    async def _http_acquire(self, request) -> "web.Response":
        from aiohttp import web
        data = await request.json()
        txn_id = data.get("txn_id", str(uuid.uuid4()))
        resource = data.get("resource", "default")
        lock_type = data.get("lock_type", "EXCLUSIVE")

        if not self._consensus or not self._consensus.is_leader:
            return web.json_response({
                "error": "not_leader",
                "leader_id": self._consensus.leader_id if self._consensus else None,
            }, status=307)

        command = {
            "op": "acquire",
            "txn_id": txn_id,
            "resource": resource,
            "lock_type": lock_type,
            "timestamp": time.time(),
        }

        key = f"{txn_id}:{resource}"
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._waiting[key] = fut

        start = time.monotonic()
        committed = await self._consensus.submit(command)
        if not committed:
            return web.json_response({"error": "commit_failed"}, status=503)

        try:
            result = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            result = {"status": LockStatus.DENIED, "txn_id": txn_id}
        finally:
            self._waiting.pop(key, None)

        elapsed = time.monotonic() - start
        lock_wait_time.observe(elapsed)
        return web.json_response(result)

    async def _http_release(self, request) -> "web.Response":
        from aiohttp import web
        data = await request.json()
        txn_id = data.get("txn_id")
        resource = data.get("resource")

        if not self._consensus or not self._consensus.is_leader:
            return web.json_response({"error": "not_leader"}, status=307)

        command = {"op": "release", "txn_id": txn_id, "resource": resource}
        committed = await self._consensus.submit(command)
        if not committed:
            return web.json_response({"error": "commit_failed"}, status=503)
        return web.json_response({"status": "ok"})

    async def _http_state(self, request) -> "web.Response":
        from aiohttp import web
        return web.json_response({
            "lock_state": self._sm.get_state(),
            "consensus": self._consensus.get_status() if self._consensus else {},
        })

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "consensus": self._consensus.get_status() if self._consensus else {},
            "locks": self._sm.get_state(),
        }
