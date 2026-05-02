# Distributed Synchronization System

**TUGAS 2 — Sistem Parallel dan Terdistribusi**  
Implementasi Distributed Synchronization System  
Deadline: 3 Mei 2026 | Bobot: 30%

---

## 🏗️ Arsitektur

Sistem ini mengimplementasikan tiga komponen sinkronisasi terdistribusi:

| Komponen | Algoritma | Poin |
|----------|-----------|------|
| Distributed Lock Manager | **Raft Consensus** | 25 |
| Byzantine Fault Tolerance | **PBFT Consensus** | Bonus |
| Distributed Queue | **Consistent Hashing** | 20 |
| Cache Coherence | **MESI Protocol** | 15 |
| Containerization | **Docker Compose** | 10 |

```
REST API (FastAPI) :8000
    ├── Lock Nodes ×3 (Raft Leader Election + Log Replication)
    ├── Queue Nodes ×3 (Consistent Hash Ring + Redis Persistence)
    └── Cache Nodes ×3 (MESI Protocol + LRU Replacement)
              │
          Redis (Persistence + State)
              │
    Prometheus + Grafana (Monitoring)
```

---

## 🚀 Quick Start

```bash
# Clone dan masuk ke directory
git clone <repo-url> && cd distributed-sync-system

# Copy environment config
cp .env.example .env

# Build dan jalankan semua 9 nodes + monitoring
cd docker
docker compose up --build -d

# Cek semua services running
docker compose ps
```

**Akses:**
- 📖 **Swagger UI**: http://localhost:8000/docs
- 📊 **Grafana**: http://localhost:3000 (admin/admin123)
- 📈 **Prometheus**: http://localhost:9090

---

## 🔧 Stack Teknologi

- **Python 3.11** + `asyncio` untuk concurrent programming
- **aiohttp** untuk inter-node HTTP/JSON RPC
- **FastAPI** untuk REST control plane + auto-generated Swagger/OpenAPI
- **Redis** untuk message persistence dan distributed state
- **Prometheus** + **Grafana** untuk observability
- **Docker** + **Docker Compose** untuk containerization

---

## 📁 Struktur Project

```
distributed-sync-system/
├── src/
│   ├── consensus/
│   │   ├── raft.py                # Raft consensus dari scratch
│   │   └── pbft.py                # PBFT consensus (Bonus: Byzantine Tolerance)
│   ├── nodes/
│   │   ├── base_node.py           # Base class semua nodes
│   │   ├── lock_manager.py        # Lock Manager + deadlock detection
│   │   ├── queue_node.py          # Queue + consistent hashing
│   │   └── cache_node.py          # MESI + LRU cache
│   ├── communication/
│   │   ├── message_passing.py     # Async HTTP RPC
│   │   └── failure_detector.py    # Heartbeat failure detection
│   └── utils/
│       ├── config.py              # .env config loader
│       └── metrics.py             # Prometheus metrics
├── api/main.py                    # FastAPI REST gateway
├── tests/unit/                    # Unit tests (pytest)
├── benchmarks/load_test_scenarios.py  # Locust load tests
├── docker/
│   ├── Dockerfile.node
│   ├── Dockerfile.api
│   └── docker-compose.yml         # 9 nodes + monitoring
├── docs/
│   ├── architecture.md
│   ├── api_spec.yaml              # OpenAPI 3.0
│   └── deployment_guide.md
└── prometheus/prometheus.yml
```

---

## 🧪 Testing

```bash
# Unit & Performance tests
pip install -r requirements.txt
pytest tests/unit/ tests/performance/ -v -s

# Load testing
locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8000
# Buka http://localhost:8089 untuk Locust UI
```

---

## 📋 Fitur Utama

### A. Distributed Lock Manager (Raft & PBFT)
- ✅ **Raft (Default):** Leader election, Log replication, Network partition handling.
- ✅ **PBFT (Bonus):** Byzantine Fault Tolerance, handling *Malicious Nodes* yang me-drop packet / mengirim hash palsu.
- ✅ Shared dan Exclusive locks
- ✅ Deadlock detection via Wait-For Graph (DFS cycle detection)
- ✅ Victim selection (abort youngest transaction)

### B. Distributed Queue (Consistent Hashing)
- ✅ Virtual nodes (150 per node) untuk distribusi uniform
- ✅ Multiple producers dan consumers
- ✅ Message persistence ke Redis sebelum ACK
- ✅ Recovery dari Redis saat node restart
- ✅ At-least-once delivery (redelivery setelah timeout)
- ✅ Dead Letter Queue (setelah max retry)

### C. Distributed Cache Coherence (MESI)
- ✅ Empat state: Modified, Exclusive, Shared, Invalid
- ✅ Read protocol: E (no peers) / S (peers have copy)
- ✅ Write protocol: broadcast Invalidate → M
- ✅ Backing Store terpusat via Redis (mendukung inter-container sync)
- ✅ LRU replacement policy O(1) via OrderedDict
- ✅ Prometheus metrics (hit rate, miss rate, state transitions)

### D. Containerization
- ✅ Multi-stage Dockerfile (slim runtime image)
- ✅ Docker Compose dengan 9 nodes + Redis + Prometheus + Grafana
- ✅ Dynamic scaling: `docker compose up --scale lock-node1=5`
- ✅ Environment configuration via `.env`

---

## 🌐 Link

- **GitHub Repository**: _[isi setelah upload]_
- **YouTube Demo**: _[isi setelah upload]_

---

## 📚 Referensi

1. Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (Raft Paper)
2. Tanenbaum & Van Steen, "Distributed Systems: Principles and Paradigms"
3. Castro & Liskov, "Practical Byzantine Fault Tolerance" (PBFT Paper)
4. Redis Distributed Lock Documentation
5. MESI Protocol — IEEE Computer Architecture
