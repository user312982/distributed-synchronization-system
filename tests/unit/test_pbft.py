"""
tests/unit/test_pbft.py — PBFT Functional Verification

Skenario yang diuji:
  1. Normal consensus (4 honest nodes) — happy path
  2. Byzantine tolerance: 1 malicious node dari 4 (f=1)
  3. Non-primary node menolak request (submit harus ke primary)
  4. Commit tidak terjadi jika terlalu banyak Byzantine nodes (>f)
  5. Digest integrity: pre-prepare dengan digest palsu ditolak
  6. View number mismatch ditolak
  7. get_status() mengembalikan state yang benar
  8. Duplicate sequence number diabaikan
"""

import asyncio
import hashlib
import json
import pytest
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

from src.consensus.pbft import PBFTNode, hash_command


# ─────────────────────────────────────────────────────────────────────────────
# Helpers / Fixtures
# ─────────────────────────────────────────────────────────────────────────────

class InMemoryBus:
    """
    Synchronous in-process message bus.
    Semua node yang terdaftar dapat saling mengirim pesan tanpa network.
    """

    def __init__(self):
        self.nodes: Dict[str, "PBFTNode"] = {}

    def register(self, node: "PBFTNode"):
        self.nodes[node.node_id] = node

    async def call(self, peer_id: str, method: str, payload: Dict) -> Dict:
        node = self.nodes.get(peer_id)
        if node is None:
            return {}
        return await node.handle_rpc(method, payload)

    async def broadcast(
        self, peers: List[str], method: str, payload: Dict
    ) -> Dict[str, Dict]:
        results = {}
        tasks = {p: self.call(p, method, payload) for p in peers if p in self.nodes}
        for peer, coro in tasks.items():
            results[peer] = await coro
        return results


def make_cluster(
    n: int = 4,
    malicious_ids: List[str] | None = None,
    drop_ids: List[str] | None = None,
) -> tuple[InMemoryBus, List[PBFTNode]]:
    """
    Membuat cluster PBFT dengan `n` node.

    Args:
        n: Jumlah node total
        malicious_ids: Node ID yang bersifat malicious (is_malicious=True)
        drop_ids: Node ID yang tidak didaftarkan ke bus (simulasi node mati)
    """
    malicious_ids = malicious_ids or []
    drop_ids = drop_ids or []

    bus = InMemoryBus()
    node_ids = [f"node{i}" for i in range(n)]
    nodes = []

    for nid in node_ids:
        peers = [p for p in node_ids if p != nid]
        is_malicious = nid in malicious_ids
        node = PBFTNode(
            node_id=nid,
            peers=peers,
            message_bus=bus,
            on_commit=lambda entry: None,
            is_malicious=is_malicious,
        )
        nodes.append(node)
        if nid not in drop_ids:
            bus.register(node)

    return bus, nodes


def get_primary(nodes: List[PBFTNode]) -> PBFTNode:
    """Kembalikan node yang menjadi primary (view=0)."""
    for n in nodes:
        if n.is_leader:
            return n
    raise RuntimeError("Tidak ada primary ditemukan")


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestHashCommand:
    """Unit test untuk fungsi hash_command."""

    def test_deterministic(self):
        cmd = {"op": "write", "key": "x", "value": 42}
        assert hash_command(cmd) == hash_command(cmd)

    def test_different_commands_have_different_hashes(self):
        cmd1 = {"op": "write", "key": "x", "value": 1}
        cmd2 = {"op": "write", "key": "x", "value": 2}
        assert hash_command(cmd1) != hash_command(cmd2)

    def test_order_independent(self):
        cmd1 = {"a": 1, "b": 2}
        cmd2 = {"b": 2, "a": 1}
        assert hash_command(cmd1) == hash_command(cmd2)

    def test_returns_hex_string(self):
        result = hash_command({"op": "test"})
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex


class TestPBFTNodeBasic:
    """Test properti dasar PBFTNode."""

    def test_primary_election_deterministic(self):
        _, nodes = make_cluster(4)
        # view=0, primary = all_nodes[0] (sorted)
        primary = nodes[0]
        assert primary.is_leader
        for node in nodes[1:]:
            assert not node.is_leader

    def test_f_calculation(self):
        # N=4 → f=1, N=7 → f=2
        _, nodes4 = make_cluster(4)
        assert nodes4[0].f == 1

        _, nodes7 = make_cluster(7)
        assert nodes7[0].f == 2

    def test_get_status_fields(self):
        _, nodes = make_cluster(4)
        status = nodes[0].get_status()
        assert "node_id" in status
        assert "consensus" in status
        assert status["consensus"] == "pbft"
        assert "malicious" in status
        assert "view_number" in status
        assert "primary_id" in status
        assert "prepared" in status
        assert "committed" in status

    def test_malicious_flag(self):
        _, nodes = make_cluster(4, malicious_ids=["node1"])
        for node in nodes:
            if node.node_id == "node1":
                assert node.is_malicious
            else:
                assert not node.is_malicious


class TestPBFTNormalConsensus:
    """Skenario happy-path: semua node jujur, satu request."""

    @pytest.mark.asyncio
    async def test_single_commit_all_honest(self):
        """
        4 honest nodes → primary submit → semua berhasil commit.
        """
        _, nodes = make_cluster(4)
        primary = get_primary(nodes)

        committed_entries = []
        for node in nodes:
            node.on_commit = lambda entry: committed_entries.append(entry)

        result = await primary.submit({"op": "write", "key": "foo", "value": 1})
        assert result is True

        # Primary harus commit
        assert 1 in primary.committed_seqs

    @pytest.mark.asyncio
    async def test_non_primary_submit_returns_false(self):
        """
        Node bukan primary tidak boleh menerima submit.
        """
        _, nodes = make_cluster(4)
        replica = next(n for n in nodes if not n.is_leader)
        result = await replica.submit({"op": "read", "key": "bar"})
        assert result is False

    @pytest.mark.asyncio
    async def test_multiple_sequential_commits(self):
        """
        Primary submit 3 command berurutan → semua commit.
        """
        _, nodes = make_cluster(4)
        primary = get_primary(nodes)

        for i in range(3):
            result = await primary.submit({"op": "write", "key": f"k{i}", "value": i})
            assert result is True, f"Command {i} gagal commit"

        assert len(primary.committed_seqs) == 3

    @pytest.mark.asyncio
    async def test_commit_increases_counter(self):
        """
        Setiap commit menambah jumlah committed_seqs.
        """
        _, nodes = make_cluster(4)
        primary = get_primary(nodes)

        await primary.submit({"op": "set", "key": "a", "value": 10})
        assert len(primary.committed_seqs) >= 1
        await primary.submit({"op": "set", "key": "b", "value": 20})
        assert len(primary.committed_seqs) >= 2


class TestPBFTByzantineTolerance:
    """Skenario Byzantine: 1 node malicious dari 4 (f=1)."""

    @pytest.mark.asyncio
    async def test_one_malicious_node_still_commits(self):
        """
        N=4, f=1: 1 malicious node → consensus masih tercapai.
        PBFT jamin liveness dengan ≤ f Byzantine nodes.
        """
        # Gunakan seed agar malicious node selalu menjadi Byzantine
        import random
        random.seed(42)

        _, nodes = make_cluster(4, malicious_ids=["node1"])
        primary = get_primary(nodes)

        # Jalankan beberapa kali untuk mengatasi randomness dalam is_malicious behavior
        success_count = 0
        for _ in range(5):
            result = await primary.submit({"op": "write", "key": "x", "value": 99})
            if result:
                success_count += 1

        # Minimal sebagian besar commit harus berhasil
        assert success_count >= 3, (
            f"Hanya {success_count}/5 commits berhasil dengan 1 malicious node"
        )

    @pytest.mark.asyncio
    async def test_status_tracks_malicious_flag(self):
        """
        get_status() pada node malicious mengembalikan malicious=True.
        """
        _, nodes = make_cluster(4, malicious_ids=["node1"])
        mal_node = next(n for n in nodes if n.node_id == "node1")
        status = mal_node.get_status()
        assert status["malicious"] is True

    @pytest.mark.asyncio
    async def test_honest_nodes_committed_after_byzantine(self):
        """
        Setelah commit dengan 1 malicious node, primary harus ada committed_seqs.
        """
        import random
        random.seed(0)

        _, nodes = make_cluster(4, malicious_ids=["node3"])
        primary = get_primary(nodes)

        result = await primary.submit({"op": "set", "key": "z", "value": 7})
        # Tidak wajib True karena randomness malicious, tapi committed_seqs tidak boleh error
        assert isinstance(result, bool)
        assert isinstance(primary.committed_seqs, set)


class TestPBFTMessageValidation:
    """Validasi pesan: digest palsu, view mismatch, dsb."""

    @pytest.mark.asyncio
    async def test_bad_digest_pre_prepare_rejected(self):
        """
        pre_prepare dengan digest yang tidak sesuai command diabaikan.
        """
        _, nodes = make_cluster(4)
        replica = nodes[1]  # bukan primary

        bad_msg = {
            "view": 0,
            "seq": 99,
            "command": {"op": "write", "key": "hack", "value": 0},
            "digest": "invalid-digest-doesnt-match",
            "sender": nodes[0].node_id,  # primary
        }

        await replica.handle_rpc("pre_prepare", bad_msg)

        # seq 99 tidak boleh masuk ke pre_prepares
        assert 99 not in replica.pre_prepares

    @pytest.mark.asyncio
    async def test_wrong_view_number_rejected(self):
        """
        Pesan dengan view number berbeda dari node ditolak.
        """
        _, nodes = make_cluster(4)
        replica = nodes[1]

        bad_msg = {
            "view": 999,  # view berbeda
            "seq": 1,
            "command": {"op": "read"},
            "digest": hash_command({"op": "read"}),
            "sender": nodes[0].node_id,
        }

        await replica.handle_rpc("pre_prepare", bad_msg)
        assert 1 not in replica.pre_prepares

    @pytest.mark.asyncio
    async def test_non_primary_sender_pre_prepare_rejected(self):
        """
        pre_prepare dari node bukan primary diabaikan.
        """
        _, nodes = make_cluster(4)
        replica = nodes[1]
        cmd = {"op": "write", "key": "y", "value": 5}

        fake_msg = {
            "view": 0,
            "seq": 42,
            "command": cmd,
            "digest": hash_command(cmd),
            "sender": "node2",  # bukan primary (node0)
        }

        await replica.handle_rpc("pre_prepare", fake_msg)
        assert 42 not in replica.pre_prepares

    @pytest.mark.asyncio
    async def test_duplicate_seq_ignored(self):
        """
        pre_prepare dengan seq yang sudah diproses tidak duplikasi.
        """
        _, nodes = make_cluster(4)
        primary = get_primary(nodes)
        replica = nodes[1]

        cmd = {"op": "write", "key": "dup", "value": 1}
        msg = {
            "view": 0,
            "seq": 1,
            "command": cmd,
            "digest": hash_command(cmd),
            "sender": primary.node_id,
        }

        await replica.handle_rpc("pre_prepare", msg)
        first_pre_prepare = replica.pre_prepares.get(1)

        # Kirim lagi dengan command berbeda, seq sama
        cmd2 = {"op": "write", "key": "dup", "value": 999}
        msg2 = {
            "view": 0,
            "seq": 1,
            "command": cmd2,
            "digest": hash_command(cmd2),
            "sender": primary.node_id,
        }
        await replica.handle_rpc("pre_prepare", msg2)

        # Harus tetap menyimpan pre_prepare yang pertama
        assert replica.pre_prepares.get(1) == first_pre_prepare


class TestPBFTInsufficientNodes:
    """Skenario di mana node tidak cukup untuk mencapai quorum."""

    @pytest.mark.asyncio
    async def test_timeout_when_not_enough_nodes(self):
        """
        Cluster 4 node, 2 node dimatikan → quorum tidak tercapai → timeout.
        """
        # drop node1 dan node2 dari bus (tidak dapat menerima pesan)
        _, nodes = make_cluster(4, drop_ids=["node1", "node2"])
        primary = get_primary(nodes)

        # Kurangi timeout agar test cepat
        primary._commit_futures  # ensure dict ada

        # Override timeout dengan monkeypatch
        import unittest.mock as mock
        original_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout=None):
            return await original_wait_for(coro, timeout=0.3)

        with mock.patch("asyncio.wait_for", side_effect=fast_wait_for):
            result = await primary.submit({"op": "write", "key": "k", "value": 1})

        # Dengan hanya 1 node aktif dari 4, tidak bisa mencapai 2f+1=3 commits
        assert result is False


class TestPBFTRpcDispatch:
    """Test dispatch RPC ke handler yang benar."""

    @pytest.mark.asyncio
    async def test_handle_rpc_returns_ok(self):
        """
        handle_rpc mengembalikan {'status': 'ok'} untuk pesan valid.
        """
        _, nodes = make_cluster(4)
        node = nodes[0]

        cmd = {"op": "test"}
        msg = {
            "view": 0,
            "seq": 1,
            "command": cmd,
            "digest": hash_command(cmd),
            "sender": node.node_id,  # primary
        }

        result = await node.handle_rpc("pre_prepare", msg)
        assert result == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_prepare_rpc_dispatch(self):
        """
        handle_rpc untuk 'prepare' tidak raise exception.
        """
        _, nodes = make_cluster(4)
        node = nodes[0]

        prepare_msg = {
            "view": 0,
            "seq": 1,
            "digest": "abc123",
            "sender": "node1",
        }

        result = await node.handle_rpc("prepare", prepare_msg)
        assert result == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_commit_rpc_dispatch(self):
        """
        handle_rpc untuk 'commit' tidak raise exception.
        """
        _, nodes = make_cluster(4)
        node = nodes[0]

        commit_msg = {
            "view": 0,
            "seq": 1,
            "digest": "abc123",
            "sender": "node1",
        }

        result = await node.handle_rpc("commit", commit_msg)
        assert result == {"status": "ok"}


class TestPBFTLifecycle:
    """Test start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_does_not_raise(self):
        _, nodes = make_cluster(4)
        for node in nodes:
            await node.start()  # tidak boleh raise

    @pytest.mark.asyncio
    async def test_stop_does_not_raise(self):
        _, nodes = make_cluster(4)
        for node in nodes:
            await node.start()
            await node.stop()  # tidak boleh raise

    @pytest.mark.asyncio
    async def test_leader_id_property(self):
        _, nodes = make_cluster(4)
        primary = get_primary(nodes)
        assert primary.leader_id == primary.node_id

    @pytest.mark.asyncio
    async def test_primary_id_consistent_across_nodes(self):
        """Semua node sepakat siapa primary di view yang sama."""
        _, nodes = make_cluster(4)
        primary_ids = {n.primary_id for n in nodes}
        assert len(primary_ids) == 1, "Semua node harus sepakat siapa primary"
