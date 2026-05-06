"""
NEXUS Distributed Storage Engine — Dashboard
Streamlit interface for uploading files, managing the cluster, and
visualising chunk distribution, replication health, and node status.
"""

from __future__ import annotations

import io
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

# ── make the project root importable ──────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)

# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NEXUS Storage Engine",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── lazy-init coordinator (cached across reruns) ───────────────────────────────
@st.cache_resource
def get_client():
    from networking.client import NexusClient
    return NexusClient()


client = get_client()

# ═════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("⚡ NEXUS")
    st.caption("Distributed Storage Engine")
    st.divider()

    page = st.radio(
        "Navigation",
        ["Dashboard", "Upload File", "Download File", "Node Control", "Replication View"],
        index=0,
    )
    st.divider()
    st.caption("Built on: Chunked Storage • Replication • Fault Tolerance")

# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    elif n < 1024 ** 2:
        return f"{n/1024:.1f} KB"
    elif n < 1024 ** 3:
        return f"{n/1024**2:.2f} MB"
    return f"{n/1024**3:.3f} GB"


def node_badge(alive: bool) -> str:
    return "🟢 ONLINE" if alive else "🔴 OFFLINE"


# ═════════════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════
if page == "Dashboard":
    st.title("⚡ NEXUS Cluster Dashboard")
    st.caption(f"Live system overview — refreshed at {time.strftime('%H:%M:%S')}")

    health = client.cluster_health()
    nodes  = health["nodes"]
    hb     = health["heartbeat"]

    # ── top KPIs ──────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        online = sum(1 for v in hb.values() if v)
        st.metric("Nodes Online", f"{online} / {len(nodes)}")
    with col2:
        st.metric("Files Stored", health["files"])
    with col3:
        st.metric("Total Chunks", health["total_chunks"])
    with col4:
        total_bytes = sum(n["stored_bytes"] for n in nodes.values())
        st.metric("Total Data", fmt_bytes(total_bytes))

    st.divider()

    # ── node cards ────────────────────────────────────────────────────────────
    st.subheader("Storage Nodes")
    node_cols = st.columns(len(nodes))
    for i, (name, stats) in enumerate(nodes.items()):
        alive = hb.get(name, False)
        with node_cols[i]:
            st.markdown(f"### {name.upper()}")
            st.markdown(node_badge(alive))
            st.metric("Chunks", stats["chunk_count"])
            st.metric("Data Stored", fmt_bytes(stats["stored_bytes"]))
            st.caption(f"Path: `{Path(stats['path']).name}`")

    st.divider()

    # ── file registry ─────────────────────────────────────────────────────────
    st.subheader("File Registry")
    files = client.list_files()
    if not files:
        st.info("No files uploaded yet. Use the **Upload File** page to get started.")
    else:
        rows = []
        for f in sorted(files, key=lambda x: x.upload_timestamp, reverse=True):
            rows.append({
                "File Name": f.filename,
                "File ID": f.file_id[:8] + "…",
                "Size": fmt_bytes(f.size),
                "Chunks": len(f.chunk_ids),
                "Uploaded": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(f.upload_timestamp)),
                "Checksum": f.original_checksum[:16] + "…" if f.original_checksum else "—",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.divider()

    # ── chunk distribution bar chart ─────────────────────────────────────────
    st.subheader("Chunk Distribution Across Nodes")
    node_chunk_counts = {name: stats["chunk_count"] for name, stats in nodes.items()}
    if any(v > 0 for v in node_chunk_counts.values()):
        df_dist = pd.DataFrame(
            list(node_chunk_counts.items()), columns=["Node", "Chunk Count"]
        )
        st.bar_chart(df_dist.set_index("Node"))
    else:
        st.caption("Upload a file to see chunk distribution.")

    if st.button("🔄 Refresh Dashboard"):
        st.rerun()


# ═════════════════════════════════════════════════════════════════════════════
# PAGE: UPLOAD FILE
# ═════════════════════════════════════════════════════════════════════════════
elif page == "Upload File":
    st.title("📤 Upload File")
    st.write(
        "Upload any file — NEXUS will split it into chunks, "
        "replicate them across the storage nodes, and record metadata."
    )

    import config as cfg
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"Chunk size: **{fmt_bytes(cfg.CHUNK_SIZE_BYTES)}**")
    with col2:
        st.info(f"Replication factor: **{cfg.REPLICATION_FACTOR}**")

    uploaded = st.file_uploader("Choose a file to upload", type=None)

    if uploaded is not None:
        st.write(f"**File:** `{uploaded.name}` | **Size:** {fmt_bytes(uploaded.size)}")

        if st.button("🚀 Upload to NEXUS Cluster", type="primary"):
            with st.spinner("Chunking and distributing across nodes…"):
                with tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=Path(uploaded.name).suffix or ".bin",
                    prefix="nexus_upload_",
                ) as tmp:
                    tmp.write(uploaded.getbuffer())
                    tmp_path = tmp.name

                try:
                    t0 = time.perf_counter()
                    file_id = client.upload(tmp_path)
                    elapsed = (time.perf_counter() - t0) * 1000

                    # Rename the temp file to match original name in metadata
                    # (already registered under uploaded.name via the client)
                    st.success(f"✅ Upload complete in **{elapsed:.0f} ms**")
                    st.code(f"File ID: {file_id}")

                    fmeta = client.get_file_meta(file_id)
                    if fmeta:
                        chunks = list(fmeta.chunks.values())
                        st.subheader("Chunk Distribution")
                        rows = []
                        for c in sorted(chunks, key=lambda x: x.index):
                            rows.append({
                                "Chunk": f"chunk_{c.index:04d}",
                                "Size": fmt_bytes(c.size),
                                "Checksum (prefix)": c.checksum[:16] + "…",
                                "Replicated On": ", ".join(c.nodes),
                            })
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                finally:
                    os.unlink(tmp_path)


# ═════════════════════════════════════════════════════════════════════════════
# PAGE: DOWNLOAD FILE
# ═════════════════════════════════════════════════════════════════════════════
elif page == "Download File":
    st.title("📥 Download / Reconstruct File")
    st.write(
        "Select a stored file to reconstruct it from distributed chunks. "
        "NEXUS will fetch chunks from live nodes, verify checksums, and reassemble the file."
    )

    files = client.list_files()
    if not files:
        st.warning("No files in the cluster yet. Upload a file first.")
    else:
        options = {f"{f.filename}  [{f.file_id[:8]}…]": f.file_id for f in files}
        selected_label = st.selectbox("Choose a file", list(options.keys()))
        file_id = options[selected_label]
        fmeta   = client.get_file_meta(file_id)

        if fmeta:
            c1, c2, c3 = st.columns(3)
            c1.metric("Original Size", fmt_bytes(fmeta.size))
            c2.metric("Chunks", len(fmeta.chunk_ids))
            c3.metric("Uploaded", time.strftime("%Y-%m-%d %H:%M", time.localtime(fmeta.upload_timestamp)))

            # show replication table
            st.subheader("Replica Map")
            rows = []
            health = client.cluster_health()
            hb     = health["heartbeat"]
            for c in sorted(fmeta.chunks.values(), key=lambda x: x.index):
                live_replicas = [n for n in c.nodes if hb.get(n, False)]
                rows.append({
                    "Chunk": f"chunk_{c.index:04d}",
                    "Replicas": ", ".join(c.nodes),
                    "Live Replicas": len(live_replicas),
                    "Status": "✅ OK" if live_replicas else "❌ UNRECOVERABLE",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            if st.button("🔁 Reconstruct File", type="primary"):
                with st.spinner("Retrieving chunks and reconstructing…"):
                    with tempfile.TemporaryDirectory() as tmpdir:
                        output_path = Path(tmpdir) / f"recovered_{fmeta.filename}"
                        try:
                            t0 = time.perf_counter()
                            client.download(file_id, output_path)
                            elapsed = (time.perf_counter() - t0) * 1000
                            data = output_path.read_bytes()
                            st.success(f"✅ Reconstructed in **{elapsed:.0f} ms** — {fmt_bytes(len(data))}")
                            st.download_button(
                                label=f"⬇ Download {fmeta.filename}",
                                data=data,
                                file_name=fmeta.filename,
                                mime="application/octet-stream",
                            )
                        except Exception as exc:
                            st.error(f"Reconstruction failed: {exc}")


# ═════════════════════════════════════════════════════════════════════════════
# PAGE: NODE CONTROL
# ═════════════════════════════════════════════════════════════════════════════
elif page == "Node Control":
    st.title("🖥️ Node Control Panel")
    st.write(
        "Simulate node failures and recoveries to test fault tolerance. "
        "While a node is offline, NEXUS will transparently fall back to its replicas."
    )

    health = client.cluster_health()
    nodes  = health["nodes"]
    hb     = health["heartbeat"]

    for name, stats in nodes.items():
        alive = hb.get(name, False)
        with st.expander(f"{'🟢' if alive else '🔴'} {name.upper()} — {node_badge(alive)}", expanded=True):
            c1, c2, c3 = st.columns(3)
            c1.metric("Chunks Stored", stats["chunk_count"])
            c2.metric("Data", fmt_bytes(stats["stored_bytes"]))
            c3.metric("Status", "ONLINE" if alive else "OFFLINE")

            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if alive:
                    if st.button(f"💥 Simulate Failure — {name}", key=f"fail_{name}", type="secondary"):
                        client.fail_node(name)
                        st.warning(f"Node '{name}' is now OFFLINE (simulated failure).")
                        time.sleep(0.3)
                        st.rerun()
            with btn_col2:
                if not alive:
                    if st.button(f"🔧 Recover Node — {name}", key=f"recover_{name}", type="primary"):
                        client.recover_node(name)
                        st.success(f"Node '{name}' is back ONLINE.")
                        time.sleep(0.3)
                        st.rerun()


# ═════════════════════════════════════════════════════════════════════════════
# PAGE: REPLICATION VIEW
# ═════════════════════════════════════════════════════════════════════════════
elif page == "Replication View":
    st.title("🔁 Replication Health View")
    st.write(
        "Inspect per-chunk replication across the cluster. "
        "Chunks with fewer live replicas than the replication factor are flagged as under-replicated."
    )

    import config as cfg
    factor = cfg.REPLICATION_FACTOR

    files = client.list_files()
    if not files:
        st.info("No files uploaded yet.")
    else:
        health = client.cluster_health()
        hb     = health["heartbeat"]

        file_options = {f.filename: f.file_id for f in files}
        selected_name = st.selectbox("Select file", list(file_options.keys()))
        fmeta = client.get_file_meta(file_options[selected_name])

        if fmeta:
            total_chunks = len(fmeta.chunks)
            chunks       = list(fmeta.chunks.values())

            # Replication stats
            fully_replicated   = 0
            under_replicated   = 0
            unrecoverable      = 0
            rows = []

            for c in sorted(chunks, key=lambda x: x.index):
                live = [n for n in c.nodes if hb.get(n, False)]
                live_count = len(live)
                if live_count >= factor:
                    status = "✅ Fully Replicated"
                    fully_replicated += 1
                elif live_count > 0:
                    status = f"⚠️ Under-replicated ({live_count}/{factor})"
                    under_replicated += 1
                else:
                    status = "❌ Unrecoverable"
                    unrecoverable += 1

                rows.append({
                    "Chunk": f"chunk_{c.index:04d}",
                    "Total Replicas": len(c.nodes),
                    "Live Replicas": live_count,
                    "Live Nodes": ", ".join(live) if live else "—",
                    "Replication Status": status,
                })

            # Summary metrics
            m1, m2, m3 = st.columns(3)
            m1.metric("Fully Replicated", fully_replicated, delta=None)
            m2.metric("Under-replicated", under_replicated, delta=None)
            m3.metric("Unrecoverable", unrecoverable, delta=None)

            if under_replicated or unrecoverable:
                st.warning(
                    f"⚠️ {under_replicated} chunk(s) under-replicated, "
                    f"{unrecoverable} unrecoverable. Recover failed nodes to restore redundancy."
                )
            else:
                st.success("All chunks are fully replicated.")

            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            # Node availability summary
            st.subheader("Node Availability")
            ndf = pd.DataFrame([
                {"Node": n, "Status": "🟢 ONLINE" if alive else "🔴 OFFLINE"}
                for n, alive in hb.items()
            ])
            st.dataframe(ndf, use_container_width=True, hide_index=True)
