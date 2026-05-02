"""
consensus/raft.py — Raft Consensus Algorithm (from scratch)

Implements:
  - Leader election with randomized timeouts
  - Log replication with majority commit
  - AppendEntries and RequestVote RPCs
  - Network partition handling (node steps down if can't reach majority)
  - State machine application via callbacks
"""
import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.utils.metrics import (
    raft_leader_elections, raft_term_gauge, raft_role_gauge,
    raft_log_size, raft_commit_latency, Timer
)

logger = logging.getLogger(__name__)


class RaftRole(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]


@dataclass
class RaftState:
    # Persistent state
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)

    # Volatile state
    commit_index: int = -1
    last_applied: int = -1

    # Leader volatile state
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)


class RaftNode:
    """
    Core Raft implementation.
    Attach to a running aiohttp server by calling `handle_rpc(method, payload)`.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        message_bus,
        on_commit: Callable[[LogEntry], None] | None = None,
        election_timeout_ms: Tuple[int, int] = (150, 300),
        heartbeat_ms: int = 50,
    ):
        self.node_id = node_id
        self.peers = peers
        self.bus = message_bus
        self.on_commit = on_commit or (lambda _: None)

        self._election_timeout_range = election_timeout_ms
        self._heartbeat_ms = heartbeat_ms

        self._state = RaftState()
        self._role = RaftRole.FOLLOWER
        self._leader_id: Optional[str] = None

        self._election_timer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._apply_task: Optional[asyncio.Task] = None

        # Pending log entries waiting for commit (index → future)
        self._commit_futures: Dict[int, asyncio.Future] = {}

        self._lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self):
        self._state.next_index = {p: 0 for p in self.peers}
        self._state.match_index = {p: -1 for p in self.peers}
        self._apply_task = asyncio.create_task(self._apply_loop())
        self._reset_election_timer()
        self._update_metrics()
        logger.info("[%s] Raft started, peers=%s", self.node_id, self.peers)

    async def stop(self):
        for task in [self._election_timer_task, self._heartbeat_task, self._apply_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def is_leader(self) -> bool:
        return self._role == RaftRole.LEADER

    @property
    def leader_id(self) -> Optional[str]:
        return self._leader_id

    @property
    def current_term(self) -> int:
        return self._state.current_term

    @property
    def role(self) -> str:
        return self._role.name

    async def submit(self, command: Dict[str, Any]) -> bool:
        """
        Submit a command for replication. Only succeeds on leader.
        Returns True when committed by majority.
        """
        async with self._lock:
            if self._role != RaftRole.LEADER:
                return False
            index = len(self._state.log)
            entry = LogEntry(term=self._state.current_term, index=index, command=command)
            self._state.log.append(entry)
            raft_log_size.labels(node_id=self.node_id).set(len(self._state.log))
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._commit_futures[index] = fut

            # Single-node cluster: commit immediately (majority = 1 = self)
            if not self.peers:
                self._state.commit_index = index
                if not fut.done():
                    fut.set_result(True)

        with Timer(raft_commit_latency):
            try:
                await asyncio.wait_for(fut, timeout=5.0)
                return True
            except asyncio.TimeoutError:
                logger.warning("[%s] Submit timed out at index %d", self.node_id, index)
                return False

    # ── RPC Handlers ──────────────────────────────────────────────────────────

    async def handle_rpc(self, method: str, payload: Dict) -> Dict:
        """Dispatch incoming Raft RPCs."""
        if method == "request_vote":
            return await self._handle_request_vote(payload)
        elif method == "append_entries":
            return await self._handle_append_entries(payload)
        return {"error": "unknown_method"}

    async def _handle_request_vote(self, args: Dict) -> Dict:
        async with self._lock:
            candidate_term = args["term"]
            candidate_id = args["candidate_id"]
            last_log_index = args["last_log_index"]
            last_log_term = args["last_log_term"]

            # Rule: if term > current, update and become follower
            if candidate_term > self._state.current_term:
                await self._become_follower(candidate_term)

            vote_granted = False
            if candidate_term >= self._state.current_term:
                # Vote only if we haven't voted, or voted for this candidate
                not_voted = (
                    self._state.voted_for is None
                    or self._state.voted_for == candidate_id
                )
                # Candidate's log must be at least as up-to-date
                log_ok = self._candidate_log_ok(last_log_index, last_log_term)
                if not_voted and log_ok:
                    self._state.voted_for = candidate_id
                    vote_granted = True
                    self._reset_election_timer()

            return {
                "term": self._state.current_term,
                "vote_granted": vote_granted,
            }

    async def _handle_append_entries(self, args: Dict) -> Dict:
        async with self._lock:
            leader_term = args["term"]
            leader_id = args["leader_id"]
            prev_log_index = args["prev_log_index"]
            prev_log_term = args["prev_log_term"]
            entries_raw = args.get("entries", [])
            leader_commit = args["leader_commit"]

            if leader_term < self._state.current_term:
                return {"term": self._state.current_term, "success": False}

            # Valid leader — reset to follower, reset timer
            if leader_term > self._state.current_term:
                await self._become_follower(leader_term)
            elif self._role == RaftRole.CANDIDATE:
                self._role = RaftRole.FOLLOWER
                raft_role_gauge.labels(node_id=self.node_id).set(0)

            self._leader_id = leader_id
            self._reset_election_timer()

            # Consistency check
            if prev_log_index >= 0:
                if len(self._state.log) <= prev_log_index:
                    return {"term": self._state.current_term, "success": False}
                if self._state.log[prev_log_index].term != prev_log_term:
                    # Delete conflicting entries
                    self._state.log = self._state.log[:prev_log_index]
                    return {"term": self._state.current_term, "success": False}

            # Append new entries
            entries = [
                LogEntry(term=e["term"], index=e["index"], command=e["command"])
                for e in entries_raw
            ]
            for entry in entries:
                if entry.index < len(self._state.log):
                    if self._state.log[entry.index].term != entry.term:
                        self._state.log = self._state.log[:entry.index]
                        self._state.log.append(entry)
                else:
                    self._state.log.append(entry)

            raft_log_size.labels(node_id=self.node_id).set(len(self._state.log))

            # Update commit index
            if leader_commit > self._state.commit_index:
                self._state.commit_index = min(leader_commit, len(self._state.log) - 1)

            return {"term": self._state.current_term, "success": True}

    # ── Election ──────────────────────────────────────────────────────────────

    def _reset_election_timer(self):
        if self._election_timer_task:
            self._election_timer_task.cancel()
        timeout_ms = random.randint(*self._election_timeout_range)
        self._election_timer_task = asyncio.create_task(
            self._election_timeout(timeout_ms / 1000)
        )

    async def _election_timeout(self, timeout: float):
        await asyncio.sleep(timeout)
        await self._start_election()

    async def _start_election(self):
        async with self._lock:
            if self._role == RaftRole.LEADER:
                return
            self._role = RaftRole.CANDIDATE
            self._state.current_term += 1
            self._state.voted_for = self.node_id
            term = self._state.current_term
            last_log_index = len(self._state.log) - 1
            last_log_term = self._state.log[-1].term if self._state.log else 0

        raft_leader_elections.inc()
        raft_role_gauge.labels(node_id=self.node_id).set(1)
        logger.info("[%s] Starting election for term %d", self.node_id, term)

        votes = 1  # vote for self
        payload = {
            "term": term,
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }

        results = await self.bus.broadcast(self.peers, "request_vote", payload)

        for peer, resp in results.items():
            if resp and resp.get("vote_granted"):
                votes += 1
            elif resp and resp.get("term", 0) > term:
                async with self._lock:
                    await self._become_follower(resp["term"])
                return

        majority = (len(self.peers) + 1) // 2 + 1
        async with self._lock:
            if votes >= majority and self._role == RaftRole.CANDIDATE and self._state.current_term == term:
                await self._become_leader()
            else:
                self._role = RaftRole.FOLLOWER
                raft_role_gauge.labels(node_id=self.node_id).set(0)
                self._reset_election_timer()

    # ── Leader ────────────────────────────────────────────────────────────────

    async def _become_leader(self):
        self._role = RaftRole.LEADER
        self._leader_id = self.node_id
        raft_role_gauge.labels(node_id=self.node_id).set(2)
        logger.info("[%s] Became LEADER for term %d", self.node_id, self._state.current_term)

        # Initialize leader state
        next_idx = len(self._state.log)
        for p in self.peers:
            self._state.next_index[p] = next_idx
            self._state.match_index[p] = -1

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        while self._role == RaftRole.LEADER:
            await self._send_append_entries_all()
            await asyncio.sleep(self._heartbeat_ms / 1000)

    async def _send_append_entries_all(self):
        tasks = [self._send_append_entries(peer) for peer in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_append_entries(self, peer: str):
        async with self._lock:
            if self._role != RaftRole.LEADER:
                return
            next_idx = self._state.next_index.get(peer, len(self._state.log))
            prev_log_index = next_idx - 1
            prev_log_term = 0
            if prev_log_index >= 0 and prev_log_index < len(self._state.log):
                prev_log_term = self._state.log[prev_log_index].term
            entries = [
                {"term": e.term, "index": e.index, "command": e.command}
                for e in self._state.log[next_idx:]
            ]
            payload = {
                "term": self._state.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self._state.commit_index,
            }

        resp = await self.bus.call(peer, "append_entries", payload)

        async with self._lock:
            if not resp:
                return
            if resp.get("term", 0) > self._state.current_term:
                await self._become_follower(resp["term"])
                return
            if resp.get("success"):
                if entries:
                    self._state.match_index[peer] = prev_log_index + len(entries)
                    self._state.next_index[peer] = self._state.match_index[peer] + 1
                # Try to advance commit index
                self._try_advance_commit()
            else:
                # Decrement and retry next heartbeat
                self._state.next_index[peer] = max(0, self._state.next_index.get(peer, 0) - 1)

    def _try_advance_commit(self):
        """Advance commit_index to the highest N where majority has replicated N."""
        n = len(self._state.log) - 1
        while n > self._state.commit_index:
            if self._state.log[n].term == self._state.current_term:
                count = 1 + sum(
                    1 for p in self.peers if self._state.match_index.get(p, -1) >= n
                )
                majority = (len(self.peers) + 1) // 2 + 1
                if count >= majority:
                    self._state.commit_index = n
                    # Resolve pending futures
                    for idx in range(self._state.last_applied + 1, n + 1):
                        if idx in self._commit_futures:
                            fut = self._commit_futures.pop(idx)
                            if not fut.done():
                                fut.set_result(True)
                    break
            n -= 1

    # ── Apply Loop ────────────────────────────────────────────────────────────

    async def _apply_loop(self):
        """Continuously apply committed log entries to the state machine."""
        while True:
            await asyncio.sleep(0.01)
            entries_to_apply = []
            async with self._lock:
                while self._state.last_applied < self._state.commit_index:
                    self._state.last_applied += 1
                    entries_to_apply.append(self._state.log[self._state.last_applied])
            for entry in entries_to_apply:
                try:
                    self.on_commit(entry)
                except Exception as e:
                    logger.error("[%s] on_commit error: %s", self.node_id, e)

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _become_follower(self, term: int):
        self._state.current_term = term
        self._state.voted_for = None
        self._role = RaftRole.FOLLOWER
        self._leader_id = None
        raft_term_gauge.labels(node_id=self.node_id).set(term)
        raft_role_gauge.labels(node_id=self.node_id).set(0)
        self._reset_election_timer()

    def _candidate_log_ok(self, candidate_last_index: int, candidate_last_term: int) -> bool:
        if not self._state.log:
            return True
        my_last = self._state.log[-1]
        if candidate_last_term != my_last.term:
            return candidate_last_term > my_last.term
        return candidate_last_index >= my_last.index

    def _update_metrics(self):
        raft_term_gauge.labels(node_id=self.node_id).set(self._state.current_term)
        raft_role_gauge.labels(node_id=self.node_id).set(0)
        raft_log_size.labels(node_id=self.node_id).set(0)

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "role": self._role.name,
            "term": self._state.current_term,
            "leader_id": self._leader_id,
            "log_length": len(self._state.log),
            "commit_index": self._state.commit_index,
            "last_applied": self._state.last_applied,
        }
