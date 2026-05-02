"""
tests/unit/test_queue.py — Unit tests for Distributed Queue
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.nodes.queue_node import ConsistentHashRing, Message, QueueNode


# ── Consistent Hash Ring ──────────────────────────────────────────────────────

def test_ring_single_node():
    ring = ConsistentHashRing(virtual_nodes=10)
    ring.add_node("node1")
    assert ring.get_node("any-key") == "node1"


def test_ring_multiple_nodes_distribution():
    ring = ConsistentHashRing(virtual_nodes=100)
    nodes = ["node1", "node2", "node3"]
    for n in nodes:
        ring.add_node(n)

    # All keys should map to one of the nodes
    keys = [f"queue-{i}" for i in range(100)]
    results = set(ring.get_node(k) for k in keys)
    assert results.issubset(set(nodes))
    assert len(results) > 1  # Keys should be distributed


def test_ring_remove_node():
    ring = ConsistentHashRing(virtual_nodes=50)
    ring.add_node("node1")
    ring.add_node("node2")
    ring.add_node("node3")

    ring.remove_node("node2")
    for key in [f"key{i}" for i in range(20)]:
        result = ring.get_node(key)
        assert result in ("node1", "node3")


def test_ring_consistent_routing():
    """Same key must always map to same node (before topology changes)."""
    ring = ConsistentHashRing(virtual_nodes=100)
    for n in ["node1", "node2", "node3"]:
        ring.add_node(n)

    key = "orders"
    first = ring.get_node(key)
    for _ in range(10):
        assert ring.get_node(key) == first


def test_ring_empty_returns_none():
    ring = ConsistentHashRing()
    assert ring.get_node("key") is None


# ── Message ───────────────────────────────────────────────────────────────────

def test_message_serialization():
    msg = Message(queue="test", body={"data": 42}, producer_id="p1")
    d = msg.to_dict()
    restored = Message.from_dict(d)
    assert restored.msg_id == msg.msg_id
    assert restored.body == msg.body
    assert restored.queue == msg.queue


# ── Deadlock Detection in Lock Manager ───────────────────────────────────────
from src.nodes.lock_manager import WaitForGraph


def test_wait_for_graph_no_deadlock():
    wfg = WaitForGraph()
    wfg.add_wait("T1", "T2")
    wfg.add_wait("T2", "T3")
    assert wfg.detect_cycle() is None


def test_wait_for_graph_simple_deadlock():
    wfg = WaitForGraph()
    wfg.add_wait("T1", "T2")
    wfg.add_wait("T2", "T1")
    cycle = wfg.detect_cycle()
    assert cycle is not None
    assert "T1" in cycle and "T2" in cycle


def test_wait_for_graph_three_way_deadlock():
    wfg = WaitForGraph()
    wfg.add_wait("T1", "T2")
    wfg.add_wait("T2", "T3")
    wfg.add_wait("T3", "T1")
    cycle = wfg.detect_cycle()
    assert cycle is not None
    assert len(cycle) == 3


def test_wait_for_graph_remove_resolves():
    wfg = WaitForGraph()
    wfg.add_wait("T1", "T2")
    wfg.add_wait("T2", "T1")
    wfg.remove_txn("T2")
    assert wfg.detect_cycle() is None
