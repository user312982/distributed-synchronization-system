"""
nodes/base_node.py — Base class for all distributed nodes
"""
import asyncio
import logging
from typing import Dict, List

from aiohttp import web

from src.communication.failure_detector import FailureDetector
from src.communication.message_passing import MessageBus
from src.utils.config import NodeConfig
from src.utils.metrics import node_up, start_metrics_server

logger = logging.getLogger(__name__)


class BaseNode:
    """
    Abstract base for lock, queue, and cache nodes.
    Provides:
      - aiohttp web server for receiving RPCs
      - MessageBus for making RPC calls
      - FailureDetector for monitoring peers
    """

    def __init__(self, config: NodeConfig):
        self.config = config
        self.node_id = config.node_id
        self.peers = config.peers

        self.bus = MessageBus(node_id=self.node_id)
        self.failure_detector = FailureDetector(
            node_id=self.node_id,
            suspect_timeout=1.5,
            on_suspect=self._on_peer_suspect,
            on_restore=self._on_peer_restore,
        )

        self._app = web.Application()
        self._setup_routes()
        self._runner: web.AppRunner | None = None

    def _setup_routes(self):
        """Register HTTP routes. Subclasses add their own routes."""
        self._app.router.add_post("/rpc/{method}", self._rpc_handler)
        self._app.router.add_get("/health", self._health_handler)
        self._app.router.add_get("/status", self._status_handler)

    async def _rpc_handler(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        try:
            payload = await request.json()
            result = await self.handle_rpc(method, payload)
            return web.json_response(result)
        except Exception as e:
            logger.error("RPC handler error [%s]: %s", method, e)
            return web.json_response({"error": str(e)}, status=500)

    async def _health_handler(self, _: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "node_id": self.node_id})

    async def _status_handler(self, _: web.Request) -> web.Response:
        return web.json_response(self.get_status())

    async def handle_rpc(self, method: str, payload: dict) -> dict:
        """Override in subclasses to handle application-specific RPCs."""
        return {"error": "not_implemented"}

    def get_status(self) -> dict:
        """Override for node-specific status."""
        return {"node_id": self.node_id, "peers": self.peers}

    async def start(self):
        """Start the node: web server + bus + failure detector."""
        logging.basicConfig(level=getattr(logging, self.config.log_level))
        node_up.labels(node_id=self.node_id).set(1)

        await self.bus.start()
        await self.failure_detector.start()
        for peer in self.peers:
            self.failure_detector.register_peer(peer)

        # Start metrics server
        start_metrics_server(self.config.metrics_port)

        await self.on_start()

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.config.host, self.config.port)
        await site.start()
        logger.info("[%s] Node listening on %s:%d", self.node_id, self.config.host, self.config.port)

    async def stop(self):
        node_up.labels(node_id=self.node_id).set(0)
        await self.on_stop()
        await self.failure_detector.stop()
        await self.bus.stop()
        if self._runner:
            await self._runner.cleanup()

    async def on_start(self):
        """Hook: called after infra is up, before HTTP server starts."""
        pass

    async def on_stop(self):
        """Hook: called before shutdown."""
        pass

    def _on_peer_suspect(self, peer: str):
        logger.warning("[%s] Peer %s suspected FAILED", self.node_id, peer)

    def _on_peer_restore(self, peer: str):
        logger.info("[%s] Peer %s RECOVERED", self.node_id, peer)

    async def run_forever(self):
        """Start and block until SIGINT."""
        await self.start()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
