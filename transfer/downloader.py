"""
NEXUS — Downloader
Retrieves chunks from storage nodes with automatic replica fallback.
If the primary node is unavailable or returns a corrupted chunk, the
downloader transparently falls back to the next available replica.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Optional

from integrity.checksum import verify_chunk
from networking.protocol import StatusCode, make_retrieve_request

if TYPE_CHECKING:
    from nodes.storage_node import StorageNode
    from nodes.heartbeat import HeartbeatMonitor

import config as cfg

logger = logging.getLogger(__name__)


class Downloader:
    """
    Retrieves chunks from the cluster with replica-aware fallback.

    Parameters
    ----------
    nodes:
        Map of node name → StorageNode instance.
    heartbeat:
        HeartbeatMonitor used to skip offline nodes before even trying.
    max_workers:
        Thread pool size for concurrent chunk fetches.
    """

    def __init__(
        self,
        nodes: dict[str, "StorageNode"],
        heartbeat: "HeartbeatMonitor",
        max_workers: int = cfg.MAX_DOWNLOAD_WORKERS,
    ) -> None:
        self._nodes      = nodes
        self._heartbeat  = heartbeat
        self._max_workers = max_workers

    def retrieve_chunk(
        self,
        chunk_id: str,
        replica_nodes: list[str],
        expected_checksum: str,
    ) -> Optional[bytes]:
        """
        Try each node in *replica_nodes* (healthy nodes first) until the
        chunk is successfully retrieved and its checksum verified.

        Returns the chunk bytes, or None if every replica failed.
        """
        healthy = self._heartbeat.healthy_nodes()

        # Prioritise healthy replicas, append unhealthy ones as last resort
        ordered = [n for n in replica_nodes if n in healthy]
        ordered += [n for n in replica_nodes if n not in healthy]

        for node_name in ordered:
            node = self._nodes.get(node_name)
            if node is None:
                continue

            request  = make_retrieve_request(chunk_id)
            response = node.handle(request)

            if not response.ok:
                logger.warning(
                    "Chunk %s unavailable on '%s': %s",
                    chunk_id, node_name, response.payload.get("error"),
                )
                continue

            data: bytes = response.payload["data"]
            if not verify_chunk(data, expected_checksum):
                logger.warning(
                    "Chunk %s on '%s' failed integrity check — trying next replica",
                    chunk_id, node_name,
                )
                continue

            logger.debug("Chunk %s retrieved from '%s'", chunk_id, node_name)
            return data

        logger.error("All replicas exhausted for chunk %s — recovery FAILED", chunk_id)
        return None

    def retrieve_all_chunks(
        self,
        chunk_plan: list[tuple[str, list[str], str]],
    ) -> dict[str, Optional[bytes]]:
        """
        Download multiple chunks concurrently.

        Parameters
        ----------
        chunk_plan:
            List of ``(chunk_id, [replica_node_names], expected_checksum)``.

        Returns
        -------
        dict mapping chunk_id → bytes (or None on failure)
        """
        results: dict[str, Optional[bytes]] = {}
        start = time.perf_counter()

        def _fetch(args: tuple[str, list[str], str]) -> tuple[str, Optional[bytes]]:
            chunk_id, replicas, checksum = args
            return chunk_id, self.retrieve_chunk(chunk_id, replicas, checksum)

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(_fetch, args): args[0] for args in chunk_plan}
            for fut in as_completed(futures):
                chunk_id, data = fut.result()
                results[chunk_id] = data

        elapsed = (time.perf_counter() - start) * 1000
        failed  = [cid for cid, d in results.items() if d is None]
        logger.info(
            "Retrieved %d/%d chunks in %.1f ms%s",
            len(results) - len(failed),
            len(chunk_plan),
            elapsed,
            f" — FAILED: {failed}" if failed else "",
        )
        return results
