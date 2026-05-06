"""
NEXUS — Integrity Module
SHA-256 checksum computation and verification for chunk corruption detection.
"""

import hashlib
import logging

logger = logging.getLogger(__name__)


def compute_sha256(data: bytes) -> str:
    """Return the hex-encoded SHA-256 digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def verify_chunk(data: bytes, expected: str) -> bool:
    """
    Verify *data* against *expected* SHA-256 hex digest.

    Returns True when the chunk is intact, False when corrupted.
    Logs a warning on mismatch so callers do not need to repeat the message.
    """
    actual = compute_sha256(data)
    if actual != expected:
        logger.warning(
            "Checksum mismatch — expected=%s  got=%s", expected[:16] + "…", actual[:16] + "…"
        )
        return False
    return True


def compute_file_sha256(filepath: str) -> str:
    """Stream a file from disk and return its SHA-256 hex digest."""
    h = hashlib.sha256()
    with open(filepath, "rb") as fh:
        for block in iter(lambda: fh.read(65536), b""):
            h.update(block)
    return h.hexdigest()
