"""
NEXUS — Metadata Store
Persistence layer for the file registry: maps files → chunks and chunks → nodes.
All state is persisted to a single JSON file so the system survives restarts.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import config as cfg

logger = logging.getLogger(__name__)


# ─── Data models ──────────────────────────────────────────────────────────────

@dataclass
class ChunkMeta:
    chunk_id: str
    index: int           # position of this chunk within the file (0-based)
    size: int            # bytes
    checksum: str        # SHA-256 hex digest
    nodes: list[str]     # node names holding this chunk (primary + replicas)


@dataclass
class FileMeta:
    file_id: str
    filename: str
    size: int
    chunk_ids: list[str]                          # ordered list of chunk IDs
    chunks: dict[str, ChunkMeta] = field(default_factory=dict)
    upload_timestamp: float       = field(default_factory=time.time)
    original_checksum: str        = ""            # whole-file SHA-256


# ─── Registry ─────────────────────────────────────────────────────────────────

class MetadataStore:
    """
    Thread-safe metadata registry backed by a JSON file.
    """

    def __init__(self, metadata_file: Path = cfg.METADATA_FILE) -> None:
        self._path  = metadata_file
        self._lock  = threading.Lock()
        self._files: dict[str, FileMeta] = {}
        self._load()

    # ─── Persistence ──────────────────────────────────────────────────────────

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
            for fid, fdata in raw.items():
                chunks = {
                    cid: ChunkMeta(**cdata)
                    for cid, cdata in fdata.pop("chunks", {}).items()
                }
                self._files[fid] = FileMeta(**fdata, chunks=chunks)
            logger.info("Metadata loaded: %d file(s)", len(self._files))
        except Exception as exc:
            logger.error("Failed to load metadata: %s", exc)

    def _save(self) -> None:
        """Persist current state to disk (called while lock is held)."""
        try:
            data: dict = {}
            for fid, fmeta in self._files.items():
                fdict = asdict(fmeta)
                data[fid] = fdict
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(data, indent=2))
        except Exception as exc:
            logger.error("Failed to save metadata: %s", exc)

    # ─── Write API ────────────────────────────────────────────────────────────

    def register_file(
        self,
        filename: str,
        size: int,
        original_checksum: str = "",
    ) -> FileMeta:
        """Create and persist a new FileMeta record. Returns the record."""
        with self._lock:
            fmeta = FileMeta(
                file_id=str(uuid.uuid4()),
                filename=filename,
                size=size,
                chunk_ids=[],
                original_checksum=original_checksum,
            )
            self._files[fmeta.file_id] = fmeta
            self._save()
            logger.info("Registered file '%s' → %s", filename, fmeta.file_id)
            return fmeta

    def add_chunk(
        self,
        file_id: str,
        chunk_id: str,
        index: int,
        size: int,
        checksum: str,
        nodes: list[str],
    ) -> ChunkMeta:
        """Attach a chunk record to an existing file."""
        with self._lock:
            fmeta = self._files.get(file_id)
            if fmeta is None:
                raise KeyError(f"No file with id={file_id}")
            cmeta = ChunkMeta(
                chunk_id=chunk_id,
                index=index,
                size=size,
                checksum=checksum,
                nodes=nodes,
            )
            fmeta.chunks[chunk_id]  = cmeta
            if chunk_id not in fmeta.chunk_ids:
                fmeta.chunk_ids.append(chunk_id)
            self._save()
            return cmeta

    def delete_file(self, file_id: str) -> None:
        with self._lock:
            self._files.pop(file_id, None)
            self._save()

    # ─── Read API ─────────────────────────────────────────────────────────────

    def get_file(self, file_id: str) -> Optional[FileMeta]:
        with self._lock:
            return self._files.get(file_id)

    def find_file_by_name(self, filename: str) -> Optional[FileMeta]:
        with self._lock:
            for fmeta in self._files.values():
                if fmeta.filename == filename:
                    return fmeta
            return None

    def list_files(self) -> list[FileMeta]:
        with self._lock:
            return list(self._files.values())

    def chunk_count(self) -> int:
        with self._lock:
            return sum(len(f.chunks) for f in self._files.values())

    def all_chunks(self) -> list[ChunkMeta]:
        with self._lock:
            result = []
            for fmeta in self._files.values():
                result.extend(fmeta.chunks.values())
            return result
