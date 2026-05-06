"""
NEXUS — Uploader
Handles parallel chunk uploads to storage nodes using a thread pool.
Each chunk is sent to its assigned primary and replica nodes concurrently.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

from integrity.checksum import compute_sha256
from networking.protocol import (
    StatusCode, make_store_request,
)

if TYPE_CHECKING:
    from nodes.storage_node import StorageNode

import config as cfg

logger = logging.getLogger(__name__)


class Uploader:
    """
    Distributes chunk uploads across storage nodes in parallel.

    Parameters
    ----------
    nodes:
        Map of node name → StorageNode instance.
    max_workers:
        Thread pool size for concurrent transfers.
    """

    def __init__(
        self,
        nodes: dict[str, "StorageNode"],
        max_workers: int = cfg.MAX_UPLOAD_WORKERS,
    ) -> None:
        self._nodes = nodes
        self._max_workers = max_workers

    def upload_chunk(
        self,
        chunk_id: str,
        data: bytes,
        node_names: list[str],
    ) -> dict[str, bool]:
        """
        Upload *data* as *chunk_id* to every node in *node_names* concurrently.

        Returns
        -------
        dict mapping node_name → success (bool)
        """
        checksum = compute_sha256(data)
        results: dict[str, bool] = {}

        def _send_to_node(name: str) -> tuple[str, bool]:
            node = self._nodes.get(name)
            if node is None:
                logger.error("Unknown node '%s' — skipping", name)
                return name, False
            request = make_store_request(chunk_id, data, checksum)
            response = node.handle(request)
            if not response.ok:
                logger.warning(
                    "Upload of chunk %s to '%s' failed: %s",
                    chunk_id, name, response.payload.get("error"),
                )
            return name, response.ok

        with ThreadPoolExecutor(max_workers=min(self._max_workers, len(node_names))) as pool:
            futures = {pool.submit(_send_to_node, n): n for n in node_names}
            for fut in as_completed(futures):
                name, ok = fut.result()
                results[name] = ok

        success_count = sum(results.values())
        logger.info(
            "Chunk %s uploaded to %d/%d nodes: %s",
            chunk_id, success_count, len(node_names),
            {n: ("OK" if ok else "FAIL") for n, ok in results.items()},
        )
        return results

    def upload_all_chunks(
        self,
        chunk_assignments: list[tuple[str, bytes, list[str]]],
    ) -> dict[str, dict[str, bool]]:
        """
        Upload multiple chunks concurrently.

        Parameters
        ----------
        chunk_assignments:
            List of ``(chunk_id, data, [node_names])``.

        Returns
        -------
        dict mapping chunk_id → {node_name: success}
        """
        all_results: dict[str, dict[str, bool]] = {}
        start = time.perf_counter()

        def _upload_one(args: tuple[str, bytes, list[str]]) -> tuple[str, dict[str, bool]]:
            chunk_id, data, node_names = args
            return chunk_id, self.upload_chunk(chunk_id, data, node_names)

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(_upload_one, args): args[0] for args in chunk_assignments}
            for fut in as_completed(futures):
                chunk_id, node_results = fut.result()
                all_results[chunk_id] = node_results

        elapsed = (time.perf_counter() - start) * 1000
        logger.info(
            "Uploaded %d chunks in %.1f ms", len(chunk_assignments), elapsed
        )
        return all_results
