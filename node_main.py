"""
Node entrypoint — launch node based on NODE_ROLE env var.
NODE_ROLE: lock | queue | cache (default: lock)
"""
import asyncio
import logging
import os
import sys

from src.utils.config import NodeConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


async def main():
    config = NodeConfig()
    role = os.getenv("NODE_ROLE", "lock").lower()

    logger.info("Starting node: id=%s role=%s port=%d peers=%s",
                config.node_id, role, config.port, config.peers)

    if role == "lock":
        from src.nodes.lock_manager import LockManagerNode
        node = LockManagerNode(config)
    elif role == "queue":
        from src.nodes.queue_node import QueueNode
        node = QueueNode(config)
    elif role == "cache":
        from src.nodes.cache_node import CacheNode
        node = CacheNode(config)
    else:
        raise ValueError(f"Unknown NODE_ROLE: {role}")

    await node.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
