# ⚡ NEXUS Distributed Storage Engine

A production-inspired distributed file storage system simulating the internals of systems like Google File System (GFS), HDFS, Dropbox, and MinIO.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      NexusClient (Facade)                   │
└─────────────────────┬───────────────────────────────────────┘
                      │
         ┌────────────▼────────────┐
         │   CoordinatorManager   │   ← Central orchestrator
         │ ┌──────┐ ┌──────────┐  │
         │ │Meta  │ │Replication│  │
         │ │Store │ │Manager   │  │
         │ └──────┘ └──────────┘  │
         │ ┌──────────┐           │
         │ │Heartbeat │           │
         │ │Monitor   │           │
         │ └──────────┘           │
         └─────┬─────────┬────────┘
               │         │
     ┌─────────▼──┐  ┌───▼──────────┐
     │  Uploader  │  │  Downloader  │
     │(ThreadPool)│  │ (ThreadPool) │
     └────────────┘  └──────────────┘
               │         │
    ┌──────────▼─────────▼──────────┐
    │     Storage Nodes (3x)        │
    │  ┌────────┐  ┌────────┐  ┌────────┐  │
    │  │ node_a │  │ node_b │  │ node_c │  │
    │  └────────┘  └────────┘  └────────┘  │
    └───────────────────────────────┘
```

---

## System Flow

### Upload Flow

```
User uploads file
      ↓
CoordinatorManager.upload_file()
      ↓
Chunker splits file into N×512KB chunks
      ↓
ReplicationManager assigns 3 nodes per chunk (round-robin)
      ↓
Uploader sends chunks in parallel (ThreadPoolExecutor)
      ↓
StorageNode stores chunk + SHA-256 checksum to disk
      ↓
MetadataStore persists file→chunk→node mapping to JSON
```

### Download Flow

```
User requests file by file_id
      ↓
CoordinatorManager.download_file()
      ↓
MetadataStore returns chunk list + replica node list
      ↓
Downloader fetches chunks in parallel (healthy nodes first)
      ↓
Integrity verification: verify_chunk(data, sha256)
      ↓
Chunker.reconstruct_file() reassembles in correct order
      ↓
Whole-file SHA-256 verified against original
```

---

## Project Structure

```
nexus-storage-engine/
├── coordinator/
│   ├── manager.py       # Central orchestrator: upload/download/recovery
│   ├── metadata.py      # JSON-backed file & chunk registry
│   └── replication.py   # Round-robin replica assignment
│
├── nodes/
│   ├── storage_node.py  # Independent chunk store (protocol-dispatched)
│   └── heartbeat.py     # Health monitor + failure simulation
│
├── transfer/
│   ├── chunker.py       # File splitting and reconstruction
│   ├── uploader.py      # Parallel chunk upload (ThreadPoolExecutor)
│   └── downloader.py    # Parallel chunk download with replica fallback
│
├── integrity/
│   └── checksum.py      # SHA-256 compute, verify, and file-level hash
│
├── networking/
│   ├── protocol.py      # Request/Response schemas (portable to sockets)
│   └── client.py        # NexusClient facade + module-level singleton
│
├── interface/
│   └── app.py           # Streamlit dashboard
│
├── storage/
│   ├── node_a/          # Physical chunk storage for node_a
│   ├── node_b/          # Physical chunk storage for node_b
│   └── node_c/          # Physical chunk storage for node_c
│
├── metadata/            # nexus_metadata.json (persistent registry)
├── config.py            # Chunk size, replication factor, node paths
├── main.py              # CLI demo and entry point
└── requirements.txt
```

---

## Setup & Run

### Install dependencies

```bash
cd nexus-storage-engine
pip install -r requirements.txt
```

### Launch the dashboard

```bash
streamlit run interface/app.py --server.port 5000
```

### Run the CLI demo

```bash
cd nexus-storage-engine
python main.py
```

### Upload / download specific files

```bash
python main.py upload /path/to/file.mp4
python main.py download <file_id>
```

---

## Engineering Concepts

### Chunked Storage
Files are split into fixed-size chunks (default 512 KB). Each chunk is stored independently, enabling parallel I/O and finer-grained fault isolation. The last chunk may be smaller.

### Replication
Each chunk is written to `REPLICATION_FACTOR` (default 3) nodes using a round-robin cursor. This ensures no two consecutive chunks share the same primary node, balancing load across the cluster.

### Fault Tolerance
The `HeartbeatMonitor` tracks node liveness. On download, the `Downloader` sorts candidate nodes by health status and tries each replica in order until one succeeds. A chunk survives as long as ≥1 replica is reachable.

### Integrity Verification
Every chunk is SHA-256 hashed at write time and verified at read time. The whole-file hash is also stored and checked after reconstruction, catching any corruption that slipped through individual chunk checks.

### Parallel Transfers
Both `Uploader` and `Downloader` use `concurrent.futures.ThreadPoolExecutor`. All chunks in a file are dispatched simultaneously, giving O(1) effective latency relative to number of chunks (bounded by pool size).

### Metadata Persistence
`MetadataStore` serialises the entire registry to a single JSON file after every write. On restart, the store re-hydrates from disk — no in-memory state is lost across process restarts.

---

## Configuration

Edit `config.py` to tune the system:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CHUNK_SIZE_BYTES` | 512 KB | Size of each chunk |
| `REPLICATION_FACTOR` | 3 | Number of replicas per chunk |
| `MAX_UPLOAD_WORKERS` | 4 | Thread pool size for uploads |
| `MAX_DOWNLOAD_WORKERS` | 4 | Thread pool size for downloads |
| `HEARTBEAT_INTERVAL_SECONDS` | 5 | Health check polling frequency |

---

## Fault Tolerance — Detailed Explanation

The system tolerates up to `REPLICATION_FACTOR - 1` simultaneous node failures per chunk. With 3 replicas across 3 nodes:

- **1 node fails** → 2 replicas remain → file fully recoverable  
- **2 nodes fail** → 1 replica remains → file recoverable (read-only)  
- **All 3 nodes fail** → 0 replicas → chunk unrecoverable  

The `ReplicationManager.find_under_replicated()` method identifies which chunks need re-replication after a node failure (re-replication pipeline is visible in the dashboard's Replication View).

---
