"""
NEXUS — Chunker
Splits a file into fixed-size byte chunks and reassembles chunks back into
the original file. This module has no I/O side-effects beyond what callers
explicitly request.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Generator

import config as cfg

logger = logging.getLogger(__name__)


def chunk_file(
    filepath: str | Path,
    chunk_size: int = cfg.CHUNK_SIZE_BYTES,
) -> Generator[tuple[int, bytes], None, None]:
    """
    Stream *filepath* and yield ``(chunk_index, data)`` tuples.

    Chunks are 0-indexed.  The last chunk may be smaller than *chunk_size*.

    Yields
    ------
    chunk_index : int
        Zero-based position of this chunk within the file.
    data : bytes
        Raw bytes for this chunk.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    file_size = filepath.stat().st_size
    logger.info(
        "Chunking '%s' (%.2f KB) with chunk_size=%d bytes",
        filepath.name,
        file_size / 1024,
        chunk_size,
    )

    index = 0
    with open(filepath, "rb") as fh:
        while True:
            data = fh.read(chunk_size)
            if not data:
                break
            logger.debug("  chunk[%d]: %d bytes", index, len(data))
            yield index, data
            index += 1

    logger.info("Chunking complete — %d chunks produced", index)


def reconstruct_file(
    output_path: str | Path,
    chunks: list[tuple[int, bytes]],
) -> None:
    """
    Write *chunks* to *output_path* in ascending order of their index.

    Parameters
    ----------
    output_path:
        Destination file path.  Parent directories are created automatically.
    chunks:
        List of ``(chunk_index, data)`` pairs (may be unsorted).
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sorted_chunks = sorted(chunks, key=lambda t: t[0])
    total_bytes = sum(len(d) for _, d in sorted_chunks)

    with open(output_path, "wb") as fh:
        for _, data in sorted_chunks:
            fh.write(data)

    logger.info(
        "Reconstructed '%s' from %d chunks (%.2f KB)",
        output_path.name,
        len(sorted_chunks),
        total_bytes / 1024,
    )


def estimate_chunk_count(file_size: int, chunk_size: int = cfg.CHUNK_SIZE_BYTES) -> int:
    """Return the number of chunks a file of *file_size* bytes will produce."""
    return max(1, -(-file_size // chunk_size))  # ceiling division
