# Arsitektur Sistem Terdistribusi

## Gambaran Umum

Sistem mengimplementasikan tiga komponen sinkronisasi terdistribusi yang berjalan dalam Docker cluster dengan total 14 container:

| Komponen | Algoritma | Nodes | Poin |
|---|---|---|---|
| Distributed Lock Manager | Raft Consensus + PBFT | 4 | 25 |
| Distributed Queue | Consistent Hashing | 3 | 20 |
| Distributed Cache Coherence | MESI Protocol | 3 | 15 |
| Containerization | Docker Compose | — | 10 |

---

## Diagram Arsitektur Keseluruhan

```mermaid
graph TB
    Client(["👤 Client / Browser"])

    subgraph API["REST API Gateway :8000"]
        FastAPI["FastAPI + Swagger UI\n/docs • /health • /status"]
    end

    subgraph LockCluster["🔐 Lock Manager Cluster (Raft + PBFT)"]
        LN1["lock-node1\n:8101\n[LEADER]"]
        LN2["lock-node2\n:8102\n[FOLLOWER]"]
        LN3["lock-node3\n:8103\n[FOLLOWER]"]
        LN4["lock-node4\n:8104\n[BYZANTINE 🦹]"]
    end

    subgraph QueueCluster["📦 Queue Cluster (Consistent Hashing)"]
        QN1["queue-node1\n:8201"]
        QN2["queue-node2\n:8202"]
        QN3["queue-node3\n:8203"]
    end

    subgraph CacheCluster["🗄️ Cache Cluster (MESI Protocol)"]
        CN1["cache-node1\n:8301"]
        CN2["cache-node2\n:8302"]
        CN3["cache-node3\n:8303"]
    end

    subgraph Infra["🏗️ Infrastructure"]
        Redis[("Redis :6379\nPersistence")]
        Prom["Prometheus :9090\nMetrics Store"]
        Grafana["Grafana :3000\nDashboard"]
    end

    Client -->|HTTP REST| FastAPI
    FastAPI -->|RPC| LN1
    FastAPI -->|RPC| LN2
    FastAPI -->|RPC| LN3
    FastAPI -->|RPC| LN4

    LN1 <-->|Raft AppendEntries\nRequestVote| LN2
    LN1 <-->|Raft AppendEntries\nRequestVote| LN3
    LN1 <-->|Raft AppendEntries\nRequestVote| LN4
    LN2 <-->|Raft| LN3
    LN2 <-->|Raft| LN4
    LN3 <-->|Raft| LN4

    QN1 <-->|queue_enqueue RPC| QN2
    QN1 <-->|queue_enqueue RPC| QN3
    QN2 <-->|queue_enqueue RPC| QN3

    CN1 <-->|cache_invalidate\ncache_check| CN2
    CN1 <-->|cache_invalidate\ncache_check| CN3
    CN2 <-->|cache_invalidate\ncache_check| CN3

    LN1 & LN2 & LN3 & LN4 -->|Persist| Redis
    QN1 & QN2 & QN3 -->|Persist| Redis

    LN1 & LN2 & LN3 & LN4 -->|Metrics :9090| Prom
    QN1 & QN2 & QN3 -->|Metrics :9090| Prom
    CN1 & CN2 & CN3 -->|Metrics :9090| Prom
    Prom -->|Query| Grafana
```

---

## Komponen A: Distributed Lock Manager (Raft Consensus)

### State Machine Raft

```mermaid
stateDiagram-v2
    [*] --> Follower : Start

    Follower --> Candidate : Election timeout\n(150–300ms random)
    Candidate --> Follower : Menemukan term lebih tinggi\natau menerima AppendEntries
    Candidate --> Candidate : Split vote\n(restart election)
    Candidate --> Leader : Mendapat majority votes\n(n/2 + 1)
    Leader --> Follower : Menemukan term lebih tinggi\n(step down)

    state Follower {
        [*] --> WaitHeartbeat
        WaitHeartbeat --> ResetTimer : AppendEntries diterima
        WaitHeartbeat --> [*] : Timeout → menjadi Candidate
    }

    state Leader {
        [*] --> SendHeartbeat
        SendHeartbeat --> ReplicateLog : Ada command baru
        ReplicateLog --> CommitIfMajority : Majority ACK
        CommitIfMajority --> SendHeartbeat
    }
```

### Alur Log Replication

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader (node1)
    participant F1 as Follower (node2)
    participant F2 as Follower (node3)

    C->>L: submit(command)
    L->>L: Append to local log [index=N]
    L->>F1: AppendEntries(term, entries, commitIdx)
    L->>F2: AppendEntries(term, entries, commitIdx)
    F1-->>L: {success: true}
    F2-->>L: {success: true}
    Note over L: Majority ACK (2/2 followers) → COMMIT
    L->>L: advance commit_index = N
    L-->>C: return True (committed)
    L->>F1: Next heartbeat (commitIdx=N)
    L->>F2: Next heartbeat (commitIdx=N)
    F1->>F1: Apply entry to state machine
    F2->>F2: Apply entry to state machine
```

### Deadlock Detection — Wait-For Graph

```mermaid
graph LR
    T1(["Txn T1\nwaiting for R2"]) -->|waits| T2(["Txn T2\nwaiting for R3"])
    T2 -->|waits| T3(["Txn T3\nwaiting for R1"])
    T3 -->|waits| T1

    style T1 fill:#ff6b6b,color:#fff
    style T2 fill:#ff6b6b,color:#fff
    style T3 fill:#ff6b6b,color:#fff

    Victim(["🗑️ Victim: T3\n(youngest timestamp)\nAbort → release R1"])
    T3 -.->|DFS detects cycle\n→ abort| Victim
```

### Lock Compatibility Matrix

```mermaid
graph LR
    subgraph Locks["Lock Compatibility"]
        S1["SHARED\n(T1)"]
        S2["SHARED\n(T2)"]
        E1["EXCLUSIVE\n(T3)"]

        S1 ---|✅ Compatible| S2
        S1 ---|❌ Conflict| E1
        S2 ---|❌ Conflict| E1
    end
```

---

## Komponen B: Distributed Queue (Consistent Hashing)

### Consistent Hash Ring

```mermaid
graph TD
    subgraph Ring["Hash Ring (MD5, 150 virtual nodes per node)"]
        direction LR
        H0["0"] --> H90["90"]
        H90 --> H180["180"]
        H180 --> H270["270"]
        H270 --> H360["360 = 0"]

        QN1L["queue-node1\nvnodes 0–149"] -. "mapped to" .-> H90
        QN2L["queue-node2\nvnodes 150–299"] -. "mapped to" .-> H180
        QN3L["queue-node3\nvnodes 300–449"] -. "mapped to" .-> H270
    end

    Key["queue_name: 'orders'"] -->|MD5 hash| Ring
    Ring -->|Route to responsible node| QN1L
```

### Alur At-Least-Once Delivery

```mermaid
sequenceDiagram
    participant P as Producer
    participant QN as Queue Node
    participant R as Redis
    participant C as Consumer

    P->>QN: enqueue(queue, body)
    QN->>R: SET queue:{node}:{queue}:{msg_id} (persist first)
    R-->>QN: OK
    QN->>QN: Append to in-memory queue
    QN-->>P: {status: "ok", msg_id: "uuid"}

    C->>QN: dequeue(queue, consumer_id)
    QN->>QN: Pop from queue → move to in-flight
    QN-->>C: {msg_id, body, delivery_count}

    alt Consumer ACK dalam timeout
        C->>QN: ack(queue, msg_id)
        QN->>R: DEL queue:{node}:{queue}:{msg_id}
        QN-->>C: {status: "acked"}
    else Timeout (delivery_timeout detik)
        Note over QN: Redelivery loop detects timeout
        QN->>QN: Re-insert to front of queue
        Note over QN: delivery_count++\nmax_retry = 5
    else Max retry exceeded
        QN->>QN: Move to Dead Letter Queue
        QN->>R: DEL (remove from persistence)
    end
```

### Recovery dari Node Failure

```mermaid
flowchart TD
    A["Node Crash 💥"] --> B["Node Restart"]
    B --> C["on_start() dipanggil"]
    C --> D["Redis KEYS queue:{node_id}:*"]
    D --> E{Ada persisted messages?}
    E -->|Ya| F["Reconstruct in-memory queue\ndari setiap Redis key"]
    F --> G["Messages tersedia\nuntuk consumers kembali ✅"]
    E -->|Tidak| H["Queue kosong,\nsiap menerima pesan baru"]
    G --> I["Start redelivery loop"]
    H --> I
```

---

## Komponen C: Cache Coherence (MESI Protocol)

### State Transitions MESI

```mermaid
stateDiagram-v2
    [*] --> I : Initial state

    I --> E : Read Miss\n(no peers have copy)\nFetch from backing store
    I --> S : Read Miss\n(peer has copy)
    I --> M : Write Miss\nBroadcast Invalidate\nFetch & write locally

    E --> M : Write Hit\n(upgrade: E→M)
    E --> S : Peer reads same key\n(E→S via bus snooping)
    E --> I : Invalidate received

    S --> M : Write Hit\nBroadcast Invalidate to all peers\n(S→M)
    S --> I : Invalidate received from writer

    M --> M : Write Hit\n(already exclusive)
    M --> S : Write-back on eviction\n(optional)
    M --> I : Invalidate received
```

### Alur Write Protocol (S→M)

```mermaid
sequenceDiagram
    participant C1 as cache-node1 (state=S)
    participant C2 as cache-node2 (state=S)
    participant C3 as cache-node3 (state=S)
    participant BS as Backing Store (Redis)

    Note over C1,C3: Semua node punya key "user:1" dalam state S

    C1->>C2: cache_invalidate {key: "user:1"}
    C1->>C3: cache_invalidate {key: "user:1"}
    C2-->>C1: {status: "invalidated"}
    C3-->>C1: {status: "invalidated"}

    C2->>C2: state[user:1] = I (Invalid)
    C3->>C3: state[user:1] = I (Invalid)

    C1->>BS: Write new value
    C1->>C1: state[user:1] = M (Modified)
    Note over C1: Exclusive ownership ✅
```

### LRU Cache Implementation

```mermaid
graph LR
    subgraph OrderedDict["OrderedDict (O(1) LRU)"]
        direction LR
        LRU["LRU\n(first / oldest)"] --> K2["key2"] --> K3["key3"] --> MRU["MRU\n(last / newest)"]
    end

    Get["get(key3)\n→ move to end"] -->|access| OrderedDict
    Put["put(keyNew)\ncapacity full\n→ evict LRU"] -->|evict 'LRU', add 'keyNew'| OrderedDict
```

---

## Bonus: PBFT Byzantine Fault Tolerance

### Fase PBFT (3-Phase Protocol)

```mermaid
sequenceDiagram
    participant P as Primary (node1)
    participant R1 as Replica (node2) ✅
    participant R2 as Replica (node3) ✅
    participant BYZ as Byzantine (node4) 🦹

    Note over P,BYZ: Phase 1: PRE-PREPARE
    P->>R1: PRE-PREPARE {view, seq, digest, command}
    P->>R2: PRE-PREPARE {view, seq, digest, command}
    P->>BYZ: PRE-PREPARE {view, seq, digest, command}

    Note over P,BYZ: Phase 2: PREPARE
    R1->>P: PREPARE {view, seq, digest}
    R1->>R2: PREPARE {view, seq, digest}
    R1->>BYZ: PREPARE {view, seq, digest}
    R2->>P: PREPARE {view, seq, digest}
    R2->>R1: PREPARE {view, seq, digest}
    R2->>BYZ: PREPARE {view, seq, digest}
    BYZ->>P: PREPARE {view, seq, BAD_DIGEST} ❌
    BYZ->>R1: PREPARE {view, seq, BAD_DIGEST} ❌
    BYZ->>R2: ⬛ DROP (silent) ❌

    Note over P,R2: 2f valid PREPAREs received (2f=2) → PREPARED ✅

    Note over P,BYZ: Phase 3: COMMIT
    P->>R1: COMMIT {view, seq, digest}
    P->>R2: COMMIT {view, seq, digest}
    R1->>P: COMMIT
    R1->>R2: COMMIT
    R2->>P: COMMIT
    R2->>R1: COMMIT
    BYZ->>P: COMMIT {BAD_DIGEST} ❌

    Note over P,R2: 2f+1 valid COMMITs (3) → EXECUTE ✅
    Note over BYZ: Byzantine node tidak bisa corrupt consensus\nkarena f=1 < (N-1)/3 = 1 ✓
```

### Toleransi Byzantine

```mermaid
graph LR
    subgraph Cluster["N=4 nodes, f=1"]
        H1["node1 ✅\nHonest"]
        H2["node2 ✅\nHonest"]
        H3["node3 ✅\nHonest"]
        BYZ["node4 🦹\nByzantine\nIS_MALICIOUS=true"]
    end

    Formula["f = ⌊(N-1)/3⌋ = ⌊3/3⌋ = 1\nToleran terhadap 1 Byzantine node\nCommit butuh 2f+1 = 3 valid commits"]

    Cluster --- Formula
```

---

## Monitoring Stack

```mermaid
graph LR
    subgraph Nodes["All Nodes (port :9090)"]
        M1["lock-node1..4\nRaft metrics\nLock metrics"]
        M2["queue-node1..3\nQueue metrics"]
        M3["cache-node1..3\nMESI metrics"]
    end

    subgraph Monitoring["Monitoring Stack"]
        Prom["Prometheus :9090\nScrape interval: 15s\nTime-series DB"]
        Graf["Grafana :3000\nDashboard\nadmin/admin123"]
    end

    M1 & M2 & M3 -->|"/metrics endpoint"| Prom
    Prom -->|"PromQL queries"| Graf

    subgraph Metrics["Key Metrics"]
        KM1["raft_node_role\nraft_current_term\nraft_leader_elections_total"]
        KM2["lock_acquire_total\nlock_wait_seconds\ndeadlock_detected_total"]
        KM3["cache_hits_total\ncache_misses_total\ncache_state_transitions_total"]
        KM4["queue_depth\nqueue_message_latency_seconds\nqueue_redelivery_total"]
    end
```

---

## Port Map

```mermaid
graph TD
    subgraph Ports["Port Mapping (host:container)"]
        P8000["8000 → API Gateway\nSwagger: /docs"]
        P8101["8101–8104 → Lock Nodes"]
        P8201["8201–8203 → Queue Nodes"]
        P8301["8301–8303 → Cache Nodes"]
        P6379["6379 → Redis"]
        P9090["9090 → Prometheus"]
        P3000["3000 → Grafana"]
    end
```
