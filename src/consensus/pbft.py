"""
consensus/pbft.py — Practical Byzantine Fault Tolerance (PBFT)

Implements a basic PBFT consensus protocol to tolerate Byzantine faults.
Supports N >= 3f + 1 nodes.
"""
import asyncio
import hashlib
import json
import logging
import random
from typing import Any, Callable, Dict, List, Optional

from src.utils.metrics import Timer

logger = logging.getLogger(__name__)


def hash_command(command: Dict) -> str:
    """Stable hash of a command."""
    s = json.dumps(command, sort_keys=True)
    return hashlib.sha256(s.encode()).hexdigest()


class PBFTNode:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        message_bus,
        on_commit: Callable[[Any], None] | None = None,
        is_malicious: bool = False,
    ):
        self.node_id = node_id
        self.peers = peers
        self.all_nodes = sorted([node_id.split(':')[0]] + [p.split(':')[0] for p in peers])
        self.bus = message_bus
        self.on_commit = on_commit or (lambda _: None)
        self.is_malicious = is_malicious

        self.view_number = 0
        self.sequence_number = 0
        self.f = (len(self.all_nodes) - 1) // 3

        # State storage
        self.log: Dict[int, Dict] = {}  # seq -> command
        self.pre_prepares: Dict[int, Dict] = {}  # seq -> msg
        self.prepares: Dict[int, Dict[str, Dict]] = {}  # seq -> sender -> msg
        self.commits: Dict[int, Dict[str, Dict]] = {}  # seq -> sender -> msg

        self.prepared_seqs = set()
        self.committed_seqs = set()

        self._commit_futures: Dict[int, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    @property
    def primary_id(self) -> str:
        return self.all_nodes[self.view_number % len(self.all_nodes)]

    @property
    def is_leader(self) -> bool:
        return self.node_id == self.primary_id

    @property
    def leader_id(self) -> str:
        return self.primary_id

    async def start(self):
        logger.info(
            "[%s] PBFT started. peers=%s, N=%d, f=%d, Malicious=%s",
            self.node_id, self.peers, len(self.all_nodes), self.f, self.is_malicious
        )

    async def stop(self):
        pass

    async def submit(self, command: Dict[str, Any]) -> bool:
        """Client submits request. Only Primary handles this directly."""
        if not self.is_leader:
            return False

        async with self._lock:
            self.sequence_number += 1
            seq = self.sequence_number
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._commit_futures[seq] = fut

        if self.is_malicious and random.random() < 0.5:
            # Malicious primary: subtly alter the command
            logger.warning("[%s] MALICIOUS: Altering command before PRE-PREPARE", self.node_id)
            command = dict(command)
            if "txn_id" in command:
                command["txn_id"] = command["txn_id"] + "-corrupted"

        digest = hash_command(command)
        msg = {
            "view": self.view_number,
            "seq": seq,
            "command": command,
            "digest": digest,
            "sender": self.node_id,
        }

        # Self pre-prepare
        await self._handle_pre_prepare(msg)
        
        # Multicast pre-prepare
        await self.bus.broadcast(self.peers, "pre_prepare", msg)

        try:
            await asyncio.wait_for(fut, timeout=10.0)
            return True
        except asyncio.TimeoutError:
            logger.warning("[%s] PBFT submit timed out for seq %d", self.node_id, seq)
            return False

    async def handle_rpc(self, method: str, payload: Dict) -> Dict:
        """Dispatch incoming PBFT RPCs."""
        if method == "pre_prepare":
            await self._handle_pre_prepare(payload)
        elif method == "prepare":
            await self._handle_prepare(payload)
        elif method == "commit":
            await self._handle_commit(payload)
        return {"status": "ok"}

    async def _handle_pre_prepare(self, msg: Dict):
        async with self._lock:
            seq = msg["seq"]
            view = msg["view"]
            command = msg["command"]
            digest = msg["digest"]
            sender = msg["sender"]

            if view != self.view_number or sender != self.primary_id:
                return

            # Verification
            if hash_command(command) != digest:
                return

            if seq in self.pre_prepares:
                return

            self.pre_prepares[seq] = msg
            self.log[seq] = command

        # Multicast PREPARE
        prep_msg = {
            "view": self.view_number,
            "seq": seq,
            "digest": digest,
            "sender": self.node_id,
        }

        if self.is_malicious and random.random() < 0.3:
            logger.warning("[%s] MALICIOUS: Sending bad PREPARE digest", self.node_id)
            prep_msg["digest"] = "bad-digest-123"
        elif self.is_malicious and random.random() < 0.3:
            logger.warning("[%s] MALICIOUS: Dropping PREPARE", self.node_id)
            return

        # Handle own prepare
        await self._handle_prepare(prep_msg)
        # Broadcast prepare
        await self.bus.broadcast(self.peers, "prepare", prep_msg)

    async def _handle_prepare(self, msg: Dict):
        seq = msg["seq"]
        view = msg["view"]
        digest = msg["digest"]
        sender = msg["sender"]

        if view != self.view_number:
            return

        async with self._lock:
            if seq not in self.prepares:
                self.prepares[seq] = {}
            self.prepares[seq][sender] = msg

            # Check if prepared (2f prepares + pre-prepare)
            # Note: The primary's pre-prepare counts as a prepare technically in some PBFT variants, 
            # but we just count strictly 2f PREPAREs from backups.
            if seq not in self.prepared_seqs and seq in self.pre_prepares:
                my_digest = self.pre_prepares[seq]["digest"]
                valid_prepares = sum(1 for m in self.prepares[seq].values() if m["digest"] == my_digest)
                
                # 2f prepares from other nodes (primary pre-prepare serves as its prepare)
                if valid_prepares >= 2 * self.f:
                    self.prepared_seqs.add(seq)
                    asyncio.create_task(self._send_commit(seq, my_digest))

    async def _send_commit(self, seq: int, digest: str):
        commit_msg = {
            "view": self.view_number,
            "seq": seq,
            "digest": digest,
            "sender": self.node_id,
        }

        if self.is_malicious and random.random() < 0.3:
            logger.warning("[%s] MALICIOUS: Sending bad COMMIT digest", self.node_id)
            commit_msg["digest"] = "bad-commit-digest-456"
        elif self.is_malicious and random.random() < 0.3:
            logger.warning("[%s] MALICIOUS: Dropping COMMIT", self.node_id)
            return

        await self._handle_commit(commit_msg)
        await self.bus.broadcast(self.peers, "commit", commit_msg)

    async def _handle_commit(self, msg: Dict):
        seq = msg["seq"]
        view = msg["view"]
        digest = msg["digest"]
        sender = msg["sender"]

        if view != self.view_number:
            return

        async with self._lock:
            if seq not in self.commits:
                self.commits[seq] = {}
            self.commits[seq][sender] = msg

            if seq not in self.committed_seqs and seq in self.pre_prepares:
                my_digest = self.pre_prepares[seq]["digest"]
                valid_commits = sum(1 for m in self.commits[seq].values() if m["digest"] == my_digest)
                
                # 2f + 1 commits
                if valid_commits >= 2 * self.f + 1:
                    self.committed_seqs.add(seq)
                    asyncio.create_task(self._execute_command(seq))

    async def _execute_command(self, seq: int):
        async with self._lock:
            command = self.log[seq]
        
        # Execute via callback
        try:
            # We wrap it in a mock LogEntry to keep compatibility with Raft's callback signature
            from src.consensus.raft import LogEntry
            entry = LogEntry(term=self.view_number, index=seq, command=command)
            self.on_commit(entry)
        except Exception as e:
            logger.error("[%s] PBFT on_commit error: %s", self.node_id, e)

        # Notify submit future if any
        async with self._lock:
            if seq in self._commit_futures:
                fut = self._commit_futures.pop(seq)
                if not fut.done():
                    fut.set_result(True)

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "consensus": "pbft",
            "malicious": self.is_malicious,
            "view_number": self.view_number,
            "primary_id": self.primary_id,
            "prepared": len(self.prepared_seqs),
            "committed": len(self.committed_seqs),
        }
