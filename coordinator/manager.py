"""
NEXUS — Coordinator Manager
Central orchestrator for the distributed storage cluster.
Coordinates file uploads (chunking → replication assignment → parallel store)
and downloads (metadata lookup → parallel retrieval → reconstruction).
"""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path
from typing import Optional

import config as cfg
from coordinator.metadata import MetadataStore
from coordinator.replication import ReplicationManager
from integrity.checksum import compute_sha256, compute_file_sha256
from nodes.heartbeat import HeartbeatMonitor
from nodes.storage_node import StorageNode
from transfer.chunker import chunk_file, reconstruct_file
from transfer.downloader import Downloader
from transfer.uploader import Uploader

logger = logging.getLogger(__name__)


class CoordinatorManager:
    """
    Entry point for all client operations.

    Instantiate once, then call :meth:`upload_file` and :meth:`download_file`.
    """

    def __init__(self) -> None:
        # Build node registry
        self.nodes: dict[str, StorageNode] = {
            name: StorageNode(name, path)
            for name, path in cfg.NODES.items()
        }

        self.heartbeat   = HeartbeatMonitor(self.nodes)
        self.metadata    = MetadataStore()
        self.replication = ReplicationManager(list(self.nodes.keys()), self.heartbeat)
        self.uploader    = Uploader(self.nodes)
        self.downloader  = Downloader(self.nodes, self.heartbeat)

        self.heartbeat.start()
        logger.info("CoordinatorManager ready — nodes: %s", list(self.nodes.keys()))

    # ─── Upload ───────────────────────────────────────────────────────────────

    def upload_file(self, filepath: str | Path) -> str:
        """
        Chunk, replicate, and store *filepath* across the cluster.

        Returns the assigned file_id (UUID string).
        """
        filepath   = Path(filepath)
        start      = time.perf_counter()
        file_size  = filepath.stat().st_size
        orig_csum  = compute_file_sha256(str(filepath))

        fmeta = self.metadata.register_file(
            filename=filepath.name,
            size=file_size,
            original_checksum=orig_csum,
        )
        file_id = fmeta.file_id
        logger.info("Upload started — '%s' (%d bytes) → file_id=%s", filepath.name, file_size, file_id)

        # Build upload plan
        upload_plan: list[tuple[str, bytes, list[str]]] = []
        chunk_meta_pending = []

        for index, data in chunk_file(filepath):
            chunk_id  = f"{file_id}__chunk_{index:04d}"
            checksum  = compute_sha256(data)
            node_list = self.replication.assign_chunk(chunk_id)

            upload_plan.append((chunk_id, data, node_list))
            chunk_meta_pending.append((chunk_id, index, len(data), checksum, node_list))

        # Parallel upload
        results = self.uploader.upload_all_chunks(upload_plan)

        # Persist metadata for successfully stored chunks
        for chunk_id, index, size, checksum, nodes in chunk_meta_pending:
            node_results = results.get(chunk_id, {})
            stored_nodes = [n for n, ok in node_results.items() if ok]
            if not stored_nodes:
                logger.error("Chunk %s failed on ALL nodes — file may be unrecoverable", chunk_id)
            self.metadata.add_chunk(
                file_id=file_id,
                chunk_id=chunk_id,
                index=index,
                size=size,
                checksum=checksum,
                nodes=stored_nodes,
            )

        elapsed = (time.perf_counter() - start) * 1000
        logger.info(
            "Upload complete — '%s' → %d chunks in %.1f ms",
            filepath.name, len(upload_plan), elapsed,
        )
        return file_id

    # ─── Download ─────────────────────────────────────────────────────────────

    def download_file(
        self,
        file_id: str,
        output_path: Optional[str | Path] = None,
    ) -> Path:
        """
        Reconstruct a file from the cluster and write it to *output_path*.

        Defaults to writing into the current working directory.
        Returns the path of the reconstructed file.
        """
        start = time.perf_counter()
        fmeta = self.metadata.get_file(file_id)
        if fmeta is None:
            raise FileNotFoundError(f"No file found with id={file_id}")

        if output_path is None:
            output_path = Path.cwd() / f"recovered_{fmeta.filename}"
        else:
            output_path = Path(output_path)

        logger.info(
            "Download started — '%s' (file_id=%s) → %s",
            fmeta.filename, file_id, output_path,
        )

        # Build retrieval plan from persisted chunk metadata
        retrieval_plan: list[tuple[str, list[str], str]] = [
            (cmeta.chunk_id, cmeta.nodes, cmeta.checksum)
            for cmeta in fmeta.chunks.values()
        ]

        # Parallel retrieval
        raw_results = self.downloader.retrieve_all_chunks(retrieval_plan)

        # Order chunks by index
        ordered_chunks: list[tuple[int, bytes]] = []
        for chunk_id, data in raw_results.items():
            if data is None:
                raise IOError(
                    f"Chunk {chunk_id} could not be recovered from any replica"
                )
            index = fmeta.chunks[chunk_id].index
            ordered_chunks.append((index, data))

        reconstruct_file(output_path, ordered_chunks)

        # Verify whole-file integrity
        if fmeta.original_checksum:
            actual = compute_file_sha256(str(output_path))
            if actual != fmeta.original_checksum:
                logger.error(
                    "INTEGRITY FAILURE — file '%s' checksum mismatch after reconstruction",
                    fmeta.filename,
                )
            else:
                logger.info("File integrity verified — checksum OK")

        elapsed = (time.perf_counter() - start) * 1000
        logger.info(
            "Download complete — '%s' reconstructed in %.1f ms → %s",
            fmeta.filename, elapsed, output_path,
        )
        return output_path

    # ─── Diagnostics ──────────────────────────────────────────────────────────

    def cluster_health(self) -> dict:
        """Return a summary of cluster health for monitoring."""
        return {
            "nodes": {name: node.get_stats() for name, node in self.nodes.items()},
            "heartbeat": self.heartbeat.all_status(),
            "files": len(self.metadata.list_files()),
            "total_chunks": self.metadata.chunk_count(),
        }

    def simulate_node_failure(self, node_name: str) -> None:
        """Bring a node offline for fault-tolerance testing."""
        self.heartbeat.fail_node(node_name)

    def simulate_node_recovery(self, node_name: str) -> None:
        """Bring a simulated-failed node back online."""
        self.heartbeat.recover_node(node_name)
