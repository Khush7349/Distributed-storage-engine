"""
NEXUS — Heartbeat Monitor
Tracks node liveness and provides a registry of healthy nodes for chunk
placement decisions. Supports both manual failure simulation and automatic
periodic health polling.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nodes.storage_node import StorageNode

logger = logging.getLogger(__name__)


class HeartbeatMonitor:
    """
    Maintains an up-to-date view of which nodes are alive.

    Usage
    -----
    monitor = HeartbeatMonitor(nodes)
    monitor.start()          # background polling loop
    monitor.fail_node("node_b")
    monitor.recover_node("node_b")
    monitor.stop()
    """

    def __init__(
        self,
        nodes: dict[str, "StorageNode"],
        interval: float = 5.0,
    ) -> None:
        self._nodes    = nodes
        self._interval = interval
        self._status: dict[str, bool] = {name: True for name in nodes}
        self._lock     = threading.Lock()
        self._thread: threading.Thread | None = None
        self._running  = False

    # ─── Lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info("HeartbeatMonitor started (interval=%.1fs)", self._interval)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._interval + 1)
        logger.info("HeartbeatMonitor stopped")

    # ─── Query ────────────────────────────────────────────────────────────────

    def is_alive(self, node_name: str) -> bool:
        with self._lock:
            return self._status.get(node_name, False)

    def healthy_nodes(self) -> list[str]:
        """Return names of all currently healthy nodes."""
        with self._lock:
            return [n for n, alive in self._status.items() if alive]

    def all_status(self) -> dict[str, bool]:
        with self._lock:
            return dict(self._status)

    # ─── Manual simulation ───────────────────────────────────────────────────

    def fail_node(self, node_name: str) -> None:
        """Simulate a node going offline."""
        node = self._nodes.get(node_name)
        if node is None:
            raise ValueError(f"Unknown node: {node_name}")
        node.fail()
        with self._lock:
            self._status[node_name] = False
        logger.warning("HeartbeatMonitor: '%s' marked FAILED", node_name)

    def recover_node(self, node_name: str) -> None:
        """Bring a simulated-failed node back online."""
        node = self._nodes.get(node_name)
        if node is None:
            raise ValueError(f"Unknown node: {node_name}")
        node.recover()
        with self._lock:
            self._status[node_name] = True
        logger.info("HeartbeatMonitor: '%s' marked RECOVERED", node_name)

    # ─── Background polling ───────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        while self._running:
            self._check_all()
            time.sleep(self._interval)

    def _check_all(self) -> None:
        with self._lock:
            for name, node in self._nodes.items():
                prev = self._status[name]
                curr = node.alive
                self._status[name] = curr
                if prev and not curr:
                    logger.warning("Heartbeat: '%s' transitioned → DOWN", name)
                elif not prev and curr:
                    logger.info("Heartbeat: '%s' transitioned → UP", name)
