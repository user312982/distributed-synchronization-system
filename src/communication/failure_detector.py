"""
communication/failure_detector.py — Heartbeat-based failure detector
Uses a simple timeout window: if no heartbeat in window → suspect failure.
"""
import asyncio
import logging
import time
from typing import Callable, Dict, Set

logger = logging.getLogger(__name__)


class FailureDetector:
    """
    Monitors peer liveness via heartbeat timestamps.
    Calls `on_suspect` when a peer is suspected dead.
    Calls `on_restore` when a suspected peer recovers.
    """

    def __init__(
        self,
        node_id: str,
        suspect_timeout: float = 1.5,   # seconds without heartbeat → suspect
        on_suspect: Callable[[str], None] | None = None,
        on_restore: Callable[[str], None] | None = None,
    ):
        self.node_id = node_id
        self.suspect_timeout = suspect_timeout
        self.on_suspect = on_suspect or (lambda _: None)
        self.on_restore = on_restore or (lambda _: None)

        self._last_seen: Dict[str, float] = {}
        self._suspected: Set[str] = set()
        self._task: asyncio.Task | None = None

    def register_peer(self, peer: str):
        """Register a peer to monitor (called once at startup)."""
        self._last_seen[peer] = time.monotonic()

    def record_heartbeat(self, peer: str):
        """Update last-seen timestamp for a peer."""
        was_suspected = peer in self._suspected
        self._last_seen[peer] = time.monotonic()
        if was_suspected:
            self._suspected.discard(peer)
            logger.info("FD: %s restored", peer)
            self.on_restore(peer)

    def is_alive(self, peer: str) -> bool:
        return peer not in self._suspected

    def suspected_peers(self) -> Set[str]:
        return set(self._suspected)

    async def start(self):
        self._task = asyncio.create_task(self._monitor_loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _monitor_loop(self):
        while True:
            await asyncio.sleep(0.5)
            now = time.monotonic()
            for peer, last in list(self._last_seen.items()):
                if now - last > self.suspect_timeout:
                    if peer not in self._suspected:
                        self._suspected.add(peer)
                        logger.warning("FD: %s suspected DEAD (no heartbeat %.1fs)", peer, now - last)
                        self.on_suspect(peer)
