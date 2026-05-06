"""
NEXUS — Storage Node
Each node is an independent storage unit responsible for persisting,
serving, and deleting chunks. Nodes are isolated from each other and
communicate only through the coordinator.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from integrity.checksum import compute_sha256, verify_chunk
from networking.protocol import (
    Request, Response, StatusCode,
    make_ok, make_error,
)

logger = logging.getLogger(__name__)


class StorageNode:
    """
    Simulates a single storage node in the distributed cluster.

    In a real deployment each StorageNode would run as a separate process
    (or container) exposing a gRPC / HTTP endpoint. Here they live in-process
    for simulation purposes while honouring the same interface.
    """

    def __init__(self, name: str, storage_path: Path) -> None:
        self.name = name
        self.storage_path = storage_path
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self._alive = True
        logger.info("StorageNode '%s' initialised at %s", name, storage_path)

    # ─── Health ───────────────────────────────────────────────────────────────

    @property
    def alive(self) -> bool:
        return self._alive

    def fail(self) -> None:
        """Simulate a node failure."""
        self._alive = False
        logger.warning("StorageNode '%s' FAILED (simulated)", self.name)

    def recover(self) -> None:
        """Bring a failed node back online."""
        self._alive = True
        logger.info("StorageNode '%s' RECOVERED", self.name)

    # ─── Core operations ──────────────────────────────────────────────────────

    def handle(self, request: Request) -> Response:
        """Dispatch an incoming protocol request."""
        start = time.perf_counter()

        if not self._alive:
            return make_error(
                request.request_id,
                StatusCode.NODE_DOWN,
                f"Node '{self.name}' is offline",
            )

        from networking.protocol import MessageType
        dispatch = {
            MessageType.STORE_CHUNK:    self._store,
            MessageType.RETRIEVE_CHUNK: self._retrieve,
            MessageType.DELETE_CHUNK:   self._delete,
            MessageType.LIST_CHUNKS:    self._list,
        }
        handler = dispatch.get(request.message_type)
        if handler is None:
            return make_error(
                request.request_id,
                StatusCode.INTERNAL_ERROR,
                f"Unknown message type: {request.message_type}",
            )

        response = handler(request)
        response.elapsed_ms = (time.perf_counter() - start) * 1000
        return response

    # ─── Internal handlers ────────────────────────────────────────────────────

    def _store(self, request: Request) -> Response:
        chunk_id: str   = request.payload["chunk_id"]
        data: bytes     = request.payload["data"]
        checksum: str   = request.payload["checksum"]

        if compute_sha256(data) != checksum:
            return make_error(
                request.request_id, StatusCode.CHECKSUM_FAIL,
                f"Checksum mismatch on incoming chunk {chunk_id}",
            )

        chunk_path = self.storage_path / chunk_id
        chunk_path.write_bytes(data)
        meta_path = self.storage_path / f"{chunk_id}.meta"
        meta_path.write_text(checksum)

        logger.debug("Node '%s' stored chunk %s (%d bytes)", self.name, chunk_id, len(data))
        return make_ok(request.request_id, {"chunk_id": chunk_id, "node": self.name})

    def _retrieve(self, request: Request) -> Response:
        chunk_id: str = request.payload["chunk_id"]
        chunk_path = self.storage_path / chunk_id
        meta_path  = self.storage_path / f"{chunk_id}.meta"

        if not chunk_path.exists():
            return make_error(
                request.request_id, StatusCode.NOT_FOUND,
                f"Chunk {chunk_id} not found on node '{self.name}'",
            )

        data: bytes     = chunk_path.read_bytes()
        expected: str   = meta_path.read_text().strip() if meta_path.exists() else ""

        if expected and not verify_chunk(data, expected):
            return make_error(
                request.request_id, StatusCode.CHECKSUM_FAIL,
                f"Chunk {chunk_id} on node '{self.name}' is corrupted",
            )

        logger.debug("Node '%s' served chunk %s", self.name, chunk_id)
        return make_ok(request.request_id, {"chunk_id": chunk_id, "data": data, "checksum": expected})

    def _delete(self, request: Request) -> Response:
        chunk_id: str = request.payload["chunk_id"]
        chunk_path = self.storage_path / chunk_id
        meta_path  = self.storage_path / f"{chunk_id}.meta"

        for p in (chunk_path, meta_path):
            if p.exists():
                p.unlink()

        logger.info("Node '%s' deleted chunk %s", self.name, chunk_id)
        return make_ok(request.request_id, {"chunk_id": chunk_id})

    def _list(self, request: Request) -> Response:
        chunks = [
            p.stem for p in self.storage_path.iterdir()
            if p.is_file() and not p.suffix == ".meta"
        ]
        return make_ok(request.request_id, {"chunks": chunks, "node": self.name})

    # ─── Stats ────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Return storage statistics for this node."""
        chunk_files = [
            p for p in self.storage_path.iterdir()
            if p.is_file() and p.suffix != ".meta"
        ]
        total_bytes = sum(p.stat().st_size for p in chunk_files)
        return {
            "name":        self.name,
            "alive":       self._alive,
            "chunk_count": len(chunk_files),
            "stored_bytes": total_bytes,
            "path":        str(self.storage_path),
        }
