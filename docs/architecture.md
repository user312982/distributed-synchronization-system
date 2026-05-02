# Arsitektur Sistem Terdistribusi

## Gambaran Umum

Sistem ini mengimplementasikan tiga komponen sinkronisasi terdistribusi yang berjalan dalam Docker cluster:

1. **Distributed Lock Manager** — berbasis algoritma Raft Consensus
2. **Distributed Queue** — berbasis Consistent Hashing
3. **Distributed Cache Coherence** — protokol MESI

---

## Diagram Arsitektur

```
┌────────────────────────────────────────────────────────────────┐
│                       Docker Compose Network                    │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   REST API Gateway (FastAPI :8000)        │  │
│  │              Swagger UI: http://localhost:8000/docs       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│           ┌──────────────────┼──────────────────┐              │
│           ▼                  ▼                  ▼              │
│  ┌─────────────────┐ ┌─────────────┐ ┌──────────────────┐     │
│  │  Lock Nodes     │ │ Queue Nodes │ │  Cache Nodes     │     │
│  │                 │ │             │ │                  │     │
│  │ lock-node1:8001 │ │queue-node1  │ │ cache-node1:8001 │     │
│  │ lock-node2:8001 │ │queue-node2  │ │ cache-node2:8001 │     │
│  │ lock-node3:8001 │ │queue-node3  │ │ cache-node3:8001 │     │
│  │                 │ │             │ │                  │     │
│  │ [Raft Consensus]│ │[Cons. Hash] │ │  [MESI Protocol] │     │
│  └────────┬────────┘ └──────┬──────┘ └────────┬─────────┘     │
│           └────────────────┬┘────────────────┘               │
│                            │                                   │
│                    ┌───────▼───────┐                          │
│                    │  Redis :6379  │                          │
│                    │ (persistence) │                          │
│                    └───────────────┘                          │
│                                                                │
│  ┌──────────────────┐    ┌──────────────────────────────────┐ │
│  │ Prometheus :9090 │    │        Grafana :3000             │ │
│  │ (metrics store)  │◄───│     (visualization dashboard)    │ │
│  └──────────────────┘    └──────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

---

## Komponen A: Distributed Lock Manager (Raft)

### Algoritma Raft

Raft dibagi menjadi tiga fase utama:

#### 1. Leader Election
- Setiap node mulai sebagai **Follower** dengan randomized election timeout (150–300ms)
- Jika tidak menerima heartbeat dalam timeout, node menjadi **Candidate**
- Candidate menaikkan term, vote untuk diri sendiri, dan kirim `RequestVote` ke peers
- Node menjadi **Leader** jika mendapat suara majority (n/2 + 1)

```
Follower ──(timeout)──► Candidate ──(majority votes)──► Leader
    ▲                        │                              │
    └────────────────────────┘◄─────────────────────────────┘
         (higher term discovered)        (heartbeats)
```

#### 2. Log Replication
- Leader menerima command dan append ke log lokal
- Leader kirim `AppendEntries` ke semua followers
- Entry di-commit setelah majority menerima (acknowledged)
- Leader kirim `commit_index` ke followers via heartbeat

#### 3. Safety Properties
- **Election Safety**: Max 1 leader per term
- **Log Matching**: Jika dua log memiliki entry dengan index dan term sama, log identik hingga index tersebut
- **Leader Completeness**: Leader selalu punya semua committed entries

### Lock Types

| Type | Kompatibilitas |
|------|---------------|
| SHARED (READ) | Compatible dengan SHARED lainnya |
| EXCLUSIVE (WRITE) | Tidak compatible dengan apapun |

### Deadlock Detection

Menggunakan **Wait-For Graph** dengan DFS cycle detection:
1. Saat transaksi T1 menunggu T2 → tambah edge T1→T2
2. Jalankan DFS setiap kali edge baru ditambahkan
3. Jika cycle ditemukan → pilih victim (transaksi termuda = timestamp terbesar)
4. Abort victim, release semua lock-nya

---

## Komponen B: Distributed Queue (Consistent Hashing)

### Consistent Hash Ring

```
         0
    ┌────────────────────┐
    │    Virtual Ring    │
    │                    │
  270──────node2──────90 │
    │                    │
  180──────node1─────────┘
         node3
```

- **Virtual Nodes**: 150 per node untuk distribusi uniform
- **Key Routing**: `MD5(queue_name)` → temukan node di ring
- **Node Join/Leave**: Rehash hanya O(K/N) keys (K=total keys, N=nodes)

### At-Least-Once Delivery

```
Producer → Enqueue → Persist to Redis → Add to Queue
                           ↓
Consumer ← Dequeue ← Move to In-Flight
                           ↓
              ┌────────────────────────┐
              │ Consumer ACK?          │
              │  YES → Delete Redis    │
              │  NO (timeout) → Re-enqueue (front)
              └────────────────────────┘
              (Max retry = 5, then DLQ)
```

### Recovery dari Node Failure

Saat node restart:
1. Baca semua keys `queue:{node_id}:*` dari Redis
2. Reconstruct in-memory queue dari persisted messages
3. Messages yang belum di-ACK akan kembali available untuk consumers

---

## Komponen C: Cache Coherence (MESI)

### State Transitions

```
                    ┌─────────────────────────┐
                    │    Read Miss (no peers)  │
             ┌──────▼──────┐                  │
   Write Hit │     E       │──Write Hit───►  M│
    (E→M)    │  (Exclusive)│                  │
             └──────┬──────┘        ┌─────────┴──────┐
                    │               │       M        │
          Peer Reads│               │   (Modified)   │
                    ▼               └───────┬────────┘
             ┌──────────────┐              │
             │      S       │◄─────────────┘
             │   (Shared)   │   Write-back on eviction
             └──────┬───────┘
                    │ Invalidate received
                    ▼
             ┌──────────────┐
             │      I       │
             │   (Invalid)  │
             └──────────────┘
```

### Protocol Rules

| Operation | State | Action |
|-----------|-------|--------|
| Read Hit | M/E/S | Serve from cache |
| Read Miss | I | Fetch from memory, set E (no peers) or S (peers have copy) |
| Write Hit | M | Update locally, stay M |
| Write Hit | E | Update locally, E→M |
| Write Hit | S | Broadcast Invalidate to peers, S→M |
| Write Miss | I | Broadcast Invalidate, fetch, set M |

### LRU Replacement Policy

Implementasi O(1) menggunakan **OrderedDict**:
- `get(key)`: ambil value, move ke end (most recently used)
- `put(key)`: jika full, remove first item (least recently used)

---

## Monitoring & Metrics

### Prometheus Metrics

| Metric | Deskripsi |
|--------|-----------|
| `raft_current_term` | Current Raft term |
| `raft_node_role` | 0=Follower, 1=Candidate, 2=Leader |
| `lock_acquire_total` | Lock acquisitions by type and status |
| `lock_wait_seconds` | Histogram of lock wait times |
| `deadlock_detected_total` | Total deadlocks detected |
| `queue_depth` | Current queue depth per queue per node |
| `queue_message_latency_seconds` | End-to-end message latency |
| `cache_hits_total` | Cache hits |
| `cache_misses_total` | Cache misses |
| `cache_state_transitions_total` | MESI state transitions |
| `cache_evictions_total` | LRU evictions |

### Grafana Dashboards

Akses: `http://localhost:3000` (admin/admin123)

Pre-built panels:
1. Raft Cluster Status (leader election rate, term progression)
2. Lock Manager (acquire rate, wait time, deadlock count)
3. Queue Throughput (enqueue/dequeue rate, queue depth, latency)
4. Cache Performance (hit rate, miss rate, state distribution)
