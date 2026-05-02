# Arsitektur Sistem Terdistribusi

## Gambaran Umum

Sistem ini mengimplementasikan tiga komponen sinkronisasi terdistribusi dalam Docker cluster:

| Komponen | Algoritma | Nodes |
|---|---|---|
| Distributed Lock Manager | Raft + PBFT | 4 |
| Distributed Queue | Consistent Hashing | 3 |
| Distributed Cache | MESI Protocol | 3 |

---

## Arsitektur Keseluruhan

```mermaid
graph TD
    Client(["Client"])
    API["REST API Gateway\nlocalhost:8000/docs"]

    subgraph Lock["Lock Cluster (Raft + PBFT)"]
        L1["lock-node1 LEADER"]
        L2["lock-node2"]
        L3["lock-node3"]
        L4["lock-node4 Byzantine"]
    end

    subgraph Queue["Queue Cluster (Consistent Hashing)"]
        Q1["queue-node1"]
        Q2["queue-node2"]
        Q3["queue-node3"]
    end

    subgraph Cache["Cache Cluster (MESI)"]
        C1["cache-node1"]
        C2["cache-node2"]
        C3["cache-node3"]
    end

    Redis[("Redis\nPersistence")]
    Prom["Prometheus\n:9090"]
    Graf["Grafana\n:3000"]

    Client --> API
    API --> Lock
    API --> Queue
    API --> Cache
    Lock & Queue --> Redis
    Lock & Queue & Cache --> Prom --> Graf
```

---

## A. Raft Consensus — State Node

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate : Timeout (150-300ms)
    Candidate --> Leader   : Majority votes
    Candidate --> Follower : Higher term found
    Leader --> Follower    : Higher term found
```

**Alur commit:**
1. Client kirim command ke Leader
2. Leader kirim `AppendEntries` ke semua Follower
3. Jika **majority** (n/2+1) balas — commit
4. Leader update semua Follower via heartbeat

---

## B. Distributed Queue — Consistent Hashing

```mermaid
flowchart LR
    P["Producer"] -->|enqueue| QN["Queue Node\n(routing via MD5 hash)"]
    QN -->|persist| R[("Redis")]
    QN -->|dequeue| C["Consumer"]
    C -->|ACK| QN
    QN -->|timeout / no ACK| QN
```

- 150 virtual nodes per node untuk distribusi merata
- At-least-once delivery: pesan di Redis sampai di-ACK

---

## C. Cache Coherence — Protokol MESI

```mermaid
stateDiagram-v2
    I : Invalid
    E : Exclusive
    S : Shared
    M : Modified

    [*] --> I
    I --> E : Read miss (no peers)
    I --> M : Write miss
    E --> M : Write hit
    E --> S : Peer reads
    S --> M : Write + Invalidate peers
    S --> I : Invalidate received
    M --> I : Invalidate received
```

Setiap write memicu broadcast Invalidate ke semua peer, lalu state berubah ke M (exclusive).

---

## D. PBFT Byzantine (Bonus)

```mermaid
sequenceDiagram
    participant L as Primary (node1)
    participant R1 as Replica (node2)
    participant R2 as Replica (node3)
    participant B as Byzantine (node4)

    L->>R1: PRE-PREPARE
    L->>R2: PRE-PREPARE
    L->>B:  PRE-PREPARE
    R1->>R2: PREPARE
    R2->>R1: PREPARE
    B-->>R1: bad digest / drop
    Note over R1,R2: Majority valid, lanjut COMMIT
    R1->>R2: COMMIT
    R2->>R1: COMMIT
    Note over L,R2: Execute berhasil
```

Toleransi: N=4, f=1, tahan 1 malicious node. Butuh 2f+1=3 commit valid.

---

## Port Map

| Service | Host Port | Keterangan |
|---|---|---|
| API Gateway | 8000 | Swagger UI di /docs |
| lock-node1..4 | 8101-8104 | Raft/PBFT nodes |
| queue-node1..3 | 8201-8203 | Queue nodes |
| cache-node1..3 | 8301-8303 | Cache nodes |
| Redis | 6379 | Persistence |
| Prometheus | 9090 | Metrics |
| Grafana | 3000 | Dashboard (admin/admin123) |
