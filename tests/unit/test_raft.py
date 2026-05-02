"""
tests/unit/test_raft.py — Unit tests for Raft consensus
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.consensus.raft import RaftNode, RaftRole, LogEntry


class MockBus:
    async def start(self): pass
    async def stop(self): pass

    async def call(self, peer, method, payload, timeout=3.0):
        return None  # Simulate unreachable peers

    async def broadcast(self, peers, method, payload, timeout=3.0):
        return {p: None for p in peers}



@pytest.fixture
def bus():
    return MockBus()


@pytest.fixture
def raft_node(bus):
    return RaftNode(
        node_id="test-node",
        peers=[],
        message_bus=bus,
        election_timeout_ms=(150, 300),
        heartbeat_ms=50,
    )


@pytest.mark.asyncio
async def test_initial_state(raft_node):
    """Node starts as follower with term 0."""
    await raft_node.start()
    assert raft_node._role == RaftRole.FOLLOWER
    assert raft_node.current_term == 0
    await raft_node.stop()


@pytest.mark.asyncio
async def test_single_node_becomes_leader(bus):
    """Single node (no peers) should win election and become leader."""
    node = RaftNode(
        node_id="solo",
        peers=[],  # No peers — majority of 1 = just itself
        message_bus=bus,
        election_timeout_ms=(50, 100),
        heartbeat_ms=20,
    )
    await node.start()
    await asyncio.sleep(0.5)  # Wait for election
    assert node._role == RaftRole.LEADER
    await node.stop()


@pytest.mark.asyncio
async def test_request_vote_higher_term(raft_node):
    """Higher-term RequestVote should update term and grant vote."""
    await raft_node.start()
    response = await raft_node.handle_rpc("request_vote", {
        "term": 5,
        "candidate_id": "other-node",
        "last_log_index": -1,
        "last_log_term": 0,
    })
    assert response["vote_granted"] is True
    assert raft_node.current_term == 5
    await raft_node.stop()


@pytest.mark.asyncio
async def test_request_vote_lower_term_rejected(raft_node):
    """Lower-term RequestVote should be rejected."""
    await raft_node.start()
    raft_node._state.current_term = 10
    response = await raft_node.handle_rpc("request_vote", {
        "term": 3,
        "candidate_id": "stale-node",
        "last_log_index": -1,
        "last_log_term": 0,
    })
    assert response["vote_granted"] is False
    await raft_node.stop()


@pytest.mark.asyncio
async def test_append_entries_heartbeat(raft_node):
    """Valid AppendEntries (heartbeat) should succeed and reset timer."""
    await raft_node.start()
    response = await raft_node.handle_rpc("append_entries", {
        "term": 1,
        "leader_id": "leader",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": [],
        "leader_commit": -1,
    })
    assert response["success"] is True
    await raft_node.stop()


@pytest.mark.asyncio
async def test_log_append_and_commit(bus):
    """Leader should commit entry when no peers (majority of 1)."""
    node = RaftNode(
        node_id="leader",
        peers=[],
        message_bus=bus,
        election_timeout_ms=(50, 80),
        heartbeat_ms=20,
    )
    await node.start()
    await asyncio.sleep(0.3)  # Become leader

    success = await node.submit({"op": "test", "value": 42})
    assert success is True
    assert node._state.commit_index >= 0
    await node.stop()


@pytest.mark.asyncio
async def test_submit_on_follower_fails(raft_node):
    """Submitting to a non-leader should return False."""
    await raft_node.start()
    result = await raft_node.submit({"op": "test"})
    assert result is False
    await raft_node.stop()
