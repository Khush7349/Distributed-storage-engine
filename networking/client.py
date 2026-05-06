"""
NEXUS — Networking Client
High-level client interface for interacting with the coordinator.
Provides upload, download, and cluster introspection in a single facade.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from coordinator.manager import CoordinatorManager
from coordinator.metadata import FileMeta

logger = logging.getLogger(__name__)

# Module-level singleton so callers don't need to manage lifecycle
_coordinator: Optional[CoordinatorManager] = None


def get_coordinator() -> CoordinatorManager:
    """Return the module-level CoordinatorManager, creating it on first call."""
    global _coordinator
    if _coordinator is None:
        _coordinator = CoordinatorManager()
    return _coordinator


class NexusClient:
    """
    Facade for all NEXUS distributed storage operations.

    Example
    -------
    client = NexusClient()
    file_id = client.upload("/path/to/video.mp4")
    client.download(file_id, "/tmp/recovered_video.mp4")
    """

    def __init__(self) -> None:
        self._coordinator = get_coordinator()

    # ─── File operations ──────────────────────────────────────────────────────

    def upload(self, filepath: str | Path) -> str:
        """
        Upload *filepath* to the NEXUS cluster.

        Returns the unique file_id to reference this file later.
        """
        return self._coordinator.upload_file(filepath)

    def download(
        self,
        file_id: str,
        output_path: Optional[str | Path] = None,
    ) -> Path:
        """
        Reconstruct the file identified by *file_id* and write to *output_path*.

        Returns the resolved output path.
        """
        return self._coordinator.download_file(file_id, output_path)

    # ─── Cluster management ───────────────────────────────────────────────────

    def fail_node(self, node_name: str) -> None:
        """Simulate a node failure for fault-tolerance testing."""
        self._coordinator.simulate_node_failure(node_name)

    def recover_node(self, node_name: str) -> None:
        """Recover a previously simulated-failed node."""
        self._coordinator.simulate_node_recovery(node_name)

    def cluster_health(self) -> dict:
        """Return a dict with per-node stats and global counters."""
        return self._coordinator.cluster_health()

    # ─── Metadata queries ─────────────────────────────────────────────────────

    def list_files(self) -> list[FileMeta]:
        """Return all file records in the metadata store."""
        return self._coordinator.metadata.list_files()

    def get_file_meta(self, file_id: str) -> Optional[FileMeta]:
        """Return the FileMeta for *file_id*, or None if not found."""
        return self._coordinator.metadata.get_file(file_id)

    def find_by_name(self, filename: str) -> Optional[FileMeta]:
        """Return the FileMeta for the first file matching *filename*."""
        return self._coordinator.metadata.find_file_by_name(filename)
