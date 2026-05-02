"""
communication/message_passing.py — async HTTP/JSON RPC between nodes
"""
import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import ClientTimeout

from src.utils.metrics import rpc_requests_total, rpc_latency

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = ClientTimeout(total=5.0)


class MessageBus:
    """
    Lightweight async RPC client used by nodes to call each other.
    All RPCs are HTTP POST to /rpc/<method> with JSON body.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        connector = aiohttp.TCPConnector(limit=100, enable_cleanup_closed=True)
        self._session = aiohttp.ClientSession(
            connector=connector, timeout=DEFAULT_TIMEOUT
        )

    async def stop(self):
        if self._session:
            await self._session.close()

    async def call(
        self,
        peer: str,         # "host:port"
        method: str,
        payload: Dict[str, Any],
        timeout: float = 3.0,
    ) -> Optional[Dict[str, Any]]:
        """Make a JSON-RPC call to a peer. Returns None on failure."""
        url = f"http://{peer}/rpc/{method}"
        start = time.monotonic()
        status_label = "ok"
        try:
            async with self._session.post(
                url,
                json=payload,
                timeout=ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result
                else:
                    status_label = f"http_{resp.status}"
                    logger.warning("RPC %s → %s returned %d", method, peer, resp.status)
                    return None
        except asyncio.TimeoutError:
            status_label = "timeout"
            logger.debug("RPC %s → %s timed out", method, peer)
            return None
        except aiohttp.ClientConnectorError:
            status_label = "conn_error"
            logger.debug("RPC %s → %s connection refused", method, peer)
            return None
        except Exception as exc:
            status_label = "error"
            logger.warning("RPC %s → %s failed: %s", method, peer, exc)
            return None
        finally:
            elapsed = time.monotonic() - start
            rpc_requests_total.labels(
                node_id=self.node_id, endpoint=method, status=status_label
            ).inc()
            rpc_latency.labels(endpoint=method).observe(elapsed)

    async def broadcast(
        self,
        peers: list[str],
        method: str,
        payload: Dict[str, Any],
        timeout: float = 3.0,
    ) -> Dict[str, Optional[Dict]]:
        """Broadcast to all peers concurrently. Returns dict of peer → result."""
        tasks = {
            peer: asyncio.create_task(self.call(peer, method, payload, timeout))
            for peer in peers
        }
        results = {}
        for peer, task in tasks.items():
            results[peer] = await task
        return results
