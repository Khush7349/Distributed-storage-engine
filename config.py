"""
NEXUS Distributed Storage Engine
Global configuration module
"""

import os
from pathlib import Path

# ─── Base paths ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.resolve()
STORAGE_ROOT = BASE_DIR / "storage"
METADATA_DIR = BASE_DIR / "metadata"

# ─── Chunk settings ───────────────────────────────────────────────────────────
CHUNK_SIZE_BYTES = 512 * 1024   # 512 KB per chunk (demo-friendly; change freely)

# ─── Replication ──────────────────────────────────────────────────────────────
REPLICATION_FACTOR = 3          # Number of replicas per chunk

# ─── Storage nodes ────────────────────────────────────────────────────────────
NODES: dict[str, Path] = {
    "node_a": STORAGE_ROOT / "node_a",
    "node_b": STORAGE_ROOT / "node_b",
    "node_c": STORAGE_ROOT / "node_c",
}

# ─── Heartbeat ────────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL_SECONDS = 5  # How often node health is checked

# ─── Concurrency ──────────────────────────────────────────────────────────────
MAX_UPLOAD_WORKERS = 4
MAX_DOWNLOAD_WORKERS = 4

# ─── Metadata ─────────────────────────────────────────────────────────────────
METADATA_FILE = METADATA_DIR / "nexus_metadata.json"

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
LOG_LEVEL = "INFO"

# ─── Ensure directories exist at import time ──────────────────────────────────
for _node_path in NODES.values():
    _node_path.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)
