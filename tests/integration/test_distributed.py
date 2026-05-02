"""
tests/integration/test_distributed.py — Integration tests for Distributed Sync System
"""
import asyncio
import os
import pytest
import httpx
from unittest.mock import patch

from src.utils.config import NodeConfig
from src.nodes.lock_manager import LockManagerNode

@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.mark.asyncio
async def test_raft_cluster_election():
    """
    Test that a cluster of 3 nodes can elect a leader.
    """
    # Create configs for 3 nodes
    config1 = NodeConfig(node_id="node1", port=8011, peers=["node2:8012", "node3:8013"], metrics_port=9011)
    config2 = NodeConfig(node_id="node2", port=8012, peers=["node1:8011", "node3:8013"], metrics_port=9012)
    config3 = NodeConfig(node_id="node3", port=8013, peers=["node1:8011", "node2:8012"], metrics_port=9013)

    # Initialize nodes
    node1 = LockManagerNode(config1)
    node2 = LockManagerNode(config2)
    node3 = LockManagerNode(config3)

    # We patch redis connection inside nodes for testing to avoid needing a real Redis
    # or assume the test environment doesn't need it. LockManager uses raft.
    # Start nodes
    t1 = asyncio.create_task(node1.run_forever())
    t2 = asyncio.create_task(node2.run_forever())
    t3 = asyncio.create_task(node3.run_forever())

    # Wait for leader election
    await asyncio.sleep(2.0)

    # Check if a leader was elected
    roles = [
        node1._raft._role.value,
        node2._raft._role.value,
        node3._raft._role.value,
    ]
    
    # At least one node should be LEADER
    assert "LEADER" in roles
    
    # The others should be FOLLOWER
    assert roles.count("FOLLOWER") >= 2

    # Stop nodes
    await node1.stop()
    await node2.stop()
    await node3.stop()

    t1.cancel()
    t2.cancel()
    t3.cancel()
