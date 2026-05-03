# Distributed Synchronization System

**TUGAS 2 — Sistem Parallel dan Terdistribusi**  
Implementasi Distributed Synchronization System  
Deadline: 3 Mei 2026 | Bobot: 30%

---

## Arsitektur Sistem

Sistem ini mengimplementasikan tiga komponen sinkronisasi terdistribusi utama:

| Komponen | Algoritma | Poin |
|----------|-----------|------|
| Distributed Lock Manager | **Raft Consensus** | 25 |
| Byzantine Fault Tolerance | **PBFT Consensus** | Bonus |
| Distributed Queue | **Consistent Hashing** | 20 |
| Cache Coherence | **MESI Protocol** | 15 |
| Containerization | **Docker Compose** | 10 |

### Topologi Jaringan

```text
REST API (FastAPI) :8000
    ├── Lock Nodes ×4 (Raft/PBFT, Leader Election, Log Replication)
    ├── Queue Nodes ×3 (Consistent Hash Ring + Redis Persistence)
    └── Cache Nodes ×3 (MESI Protocol + LRU Replacement)
              │
          Redis (Persistence + State)
              │
    Prometheus + Grafana (Monitoring)
```

---

## Panduan Eksekusi & Demonstrasi (Quick Start)

Sistem berjalan sepenuhnya di dalam Docker dan menyediakan antarmuka interaktif untuk demonstrasi fitur.

```bash
# Kloning repositori dan masuk ke direktori proyek
git clone <repo-url> && cd distributed-sync-system

# Salin konfigurasi environment
cp .env.example .env

# Jalankan seluruh arsitektur (14 Container)
cd docker
docker compose up --build -d
```

### Akses Demonstrasi
- **Swagger UI (Testing API)**: http://localhost:8000/docs
  *Gunakan antarmuka ini untuk melakukan simulasi Lock, Queue, dan Cache secara langsung pada browser Anda.*
- **Grafana Dashboard**: http://localhost:3000 (Login: admin/admin123)
  *Pemantauan metrik secara real-time untuk Cache hit/miss, Raft terms, dan antrean Queue.*
- **Prometheus**: http://localhost:9090

### Demonstrasi PBFT (Bonus)
Sistem memiliki kapabilitas toleransi kesalahan Bizantium (Byzantine Fault Tolerance). Untuk mendemonstrasikan fitur ini:
1. Buka file `docker/docker-compose.yml`.
2. Ubah variabel `CONSENSUS_TYPE: raft` menjadi `CONSENSUS_TYPE: pbft` pada seluruh `lock-node`.
3. Hilangkan komentar pada baris `IS_MALICIOUS: "true"` di bawah konfigurasi `lock-node4`.
4. Muat ulang container: `docker compose up -d`
5. Kirim permintaan akuisisi Lock melalui Swagger UI (`POST /lock/acquire`).
6. Verifikasi log perilaku *malicious*: `docker compose logs lock-node4 2>&1 | grep MALICIOUS`

---

## Spesifikasi Teknologi

- **Python 3.11** dengan `asyncio` untuk *concurrent programming*.
- **aiohttp** untuk komunikasi inter-node via HTTP/JSON RPC.
- **FastAPI** sebagai *REST control plane* dan generator OpenAPI.
- **Redis** untuk *message persistence* dan *distributed state*.
- **Prometheus & Grafana** untuk observabilitas dan pemantauan.
- **Docker & Docker Compose** untuk orkestrasi container.

---

## Struktur Proyek

```text
distributed-sync-system/
├── src/
│   ├── consensus/
│   │   ├── raft.py                # Implementasi algoritma Raft
│   │   └── pbft.py                # Implementasi algoritma PBFT (Bonus)
│   ├── nodes/
│   │   ├── base_node.py           # Kelas dasar seluruh node
│   │   ├── lock_manager.py        # Lock Manager dan deteksi deadlock
│   │   ├── queue_node.py          # Sistem antrean dengan Consistent Hashing
│   │   └── cache_node.py          # Protokol MESI dan cache LRU
│   ├── communication/
│   │   ├── message_passing.py     # RPC Asinkronus HTTP
│   │   └── failure_detector.py    # Deteksi kegagalan berbasis Heartbeat
│   └── utils/
│       ├── config.py              # Pemuat konfigurasi environment
│       └── metrics.py             # Definisi metrik Prometheus
├── api/main.py                    # Gateway REST FastAPI
├── tests/unit/                    # Pengujian unit (Pytest)
├── benchmarks/                    # Skenario pengujian beban (Locust)
├── docker/
│   ├── Dockerfile.node
│   ├── Dockerfile.api
│   └── docker-compose.yml         # Konfigurasi orkestrasi container
└── docs/                          # Dokumentasi arsitektur dan spesifikasi API
```

---

## Fitur Sistem Utama

### A. Distributed Lock Manager (Raft & PBFT)
- **Raft (Bawaan):** *Leader election*, *Log replication*, penanganan partisi jaringan.
- **PBFT (Bonus):** *Byzantine Fault Tolerance*, menangani *Malicious Nodes* yang secara sengaja membuang paket atau mengirimkan representasi data (hash) palsu.
- Dukungan *Shared* dan *Exclusive locks*.
- Deteksi kebuntuan (Deadlock) menggunakan *Wait-For Graph* (deteksi siklus DFS).
- Pemilihan korban (Victim selection) dengan membatalkan transaksi termuda.

### B. Distributed Queue (Consistent Hashing)
- Implementasi node virtual (150 per node) untuk distribusi beban yang seragam.
- Dukungan *Multiple producers* dan *consumers*.
- Persistensi pesan ke Redis sebelum *Acknowledgement* (ACK).
- Pemulihan state node pasca restart.
- Pengiriman *At-least-once* (pengiriman ulang pasca timeout).
- Integrasi *Dead Letter Queue*.

### C. Distributed Cache Coherence (MESI)
- Empat status kepemilikan: *Modified, Exclusive, Shared, Invalid*.
- Protokol baca: E (tanpa peer) / S (peer memiliki salinan).
- Protokol tulis: *Broadcast Invalidate* ke M.
- Penyimpanan pendukung terpusat melalui Redis (sinkronisasi antar-container).
- Kebijakan penggantian cache LRU dengan kompleksitas O(1) via `OrderedDict`.
- Metrik Prometheus komprehensif (Hit rate, Miss rate, Transisi state).

---

## Pengujian dan Tolok Ukur (Testing & Benchmark)

```bash
# Pengujian Unit & Performa
pip install -r requirements.txt
pytest tests/unit/ tests/performance/ -v -s

# Pengujian Beban (Load Testing)
locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8000
# Buka UI Locust di http://localhost:8089
```

---

## Tautan

- **Repositori GitHub**: _[isi setelah upload]_
- **Demonstrasi YouTube**: _[isi setelah upload]_

---

## Referensi

1. Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (Raft Paper)
2. Tanenbaum & Van Steen, "Distributed Systems: Principles and Paradigms"
3. Castro & Liskov, "Practical Byzantine Fault Tolerance" (PBFT Paper)
4. Redis Distributed Lock Documentation
5. MESI Protocol — IEEE Computer Architecture
