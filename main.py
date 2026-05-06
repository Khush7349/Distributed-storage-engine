"""
NEXUS Distributed Storage Engine — CLI Entry Point
Demonstrates the full system: upload, download, fault tolerance, and integrity.

Usage:
    python main.py                          # full demo with a generated file
    python main.py upload /path/to/file     # upload a specific file
    python main.py download <file_id>       # download a specific file
"""

from __future__ import annotations

import logging
import sys
import os
import tempfile
import hashlib
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import config as cfg

logging.basicConfig(
    format=cfg.LOG_FORMAT,
    level=cfg.LOG_LEVEL,
    stream=sys.stdout,
)
logger = logging.getLogger("nexus.main")


def run_demo() -> None:
    """Run a complete end-to-end demonstration of NEXUS capabilities."""
    from networking.client import NexusClient
    from integrity.checksum import compute_file_sha256

    client = NexusClient()

    print("\n" + "=" * 60)
    print("  NEXUS Distributed Storage Engine — Demo")
    print("=" * 60)

    # ── 1. Create a synthetic test file ───────────────────────────────────────
    print("\n[1/6] Generating synthetic test file (2 MB)…")
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".bin", prefix="nexus_demo_"
    ) as tmp:
        tmp.write(os.urandom(2 * 1024 * 1024))   # 2 MB of random bytes
        tmp_path = tmp.name

    original_checksum = compute_file_sha256(tmp_path)
    print(f"      File: {tmp_path}")
    print(f"      SHA-256: {original_checksum[:32]}…")

    # ── 2. Upload the file ────────────────────────────────────────────────────
    print("\n[2/6] Uploading to NEXUS cluster…")
    file_id = client.upload(tmp_path)
    print(f"      File ID: {file_id}")

    fmeta = client.get_file_meta(file_id)
    if fmeta:
        print(f"      Chunks produced: {len(fmeta.chunk_ids)}")
        for c in sorted(fmeta.chunks.values(), key=lambda x: x.index):
            print(f"        chunk_{c.index:04d} → nodes: {c.nodes}")

    # ── 3. Cluster health ─────────────────────────────────────────────────────
    print("\n[3/6] Cluster health snapshot…")
    health = client.cluster_health()
    for name, stats in health["nodes"].items():
        status = "ONLINE" if health["heartbeat"][name] else "OFFLINE"
        print(f"      {name}: {status} | chunks={stats['chunk_count']} | bytes={stats['stored_bytes']}")

    # ── 4. Download and verify ────────────────────────────────────────────────
    print("\n[4/6] Downloading and reconstructing file…")
    output_path = Path(tmp_path).parent / "nexus_reconstructed.bin"
    client.download(file_id, output_path)
    recovered_checksum = compute_file_sha256(str(output_path))
    integrity_ok = original_checksum == recovered_checksum
    print(f"      Integrity: {'✓ PASS' if integrity_ok else '✗ FAIL'}")
    print(f"      SHA-256: {recovered_checksum[:32]}…")

    # ── 5. Fault tolerance simulation ─────────────────────────────────────────
    print("\n[5/6] Simulating node_a failure…")
    client.fail_node("node_a")
    health2 = client.cluster_health()
    for name, alive in health2["heartbeat"].items():
        print(f"      {name}: {'ONLINE' if alive else 'OFFLINE (SIMULATED)'}")

    print("      Attempting download with node_a offline…")
    output_path2 = Path(tmp_path).parent / "nexus_fault_recovery.bin"
    try:
        client.download(file_id, output_path2)
        recovered2 = compute_file_sha256(str(output_path2))
        ok2 = original_checksum == recovered2
        print(f"      Fault-tolerant recovery: {'✓ SUCCESS' if ok2 else '✗ FAIL'}")
    except Exception as exc:
        print(f"      Recovery failed (insufficient replicas): {exc}")

    # ── 6. Recovery ───────────────────────────────────────────────────────────
    print("\n[6/6] Recovering node_a…")
    client.recover_node("node_a")
    print("      node_a is back ONLINE")

    # Cleanup
    for p in [tmp_path, str(output_path), str(output_path2)]:
        Path(p).unlink(missing_ok=True)

    print("\n" + "=" * 60)
    print("  Demo complete. Launch the dashboard:")
    print("  streamlit run interface/app.py")
    print("=" * 60 + "\n")


def cmd_upload(filepath: str) -> None:
    from networking.client import NexusClient
    client = NexusClient()
    file_id = client.upload(filepath)
    print(f"Uploaded: {filepath}\nFile ID: {file_id}")


def cmd_download(file_id: str) -> None:
    from networking.client import NexusClient
    client = NexusClient()
    output = client.download(file_id)
    print(f"Downloaded to: {output}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        run_demo()
    elif args[0] == "upload" and len(args) == 2:
        cmd_upload(args[1])
    elif args[0] == "download" and len(args) == 2:
        cmd_download(args[1])
    else:
        print(__doc__)
        sys.exit(1)
