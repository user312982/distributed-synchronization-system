# 🚀 Project Tracker: Implementasi Distributed Synchronization System

**Mata Kuliah:** Sistem Parallel dan Terdistribusi
**Deadline:** 3 Mei 2026, 18:00 WITA
**Status Keseluruhan:** 🔴 Berjalan (In Progress)

---

## 📌 INSTRUKSI UNTUK LLM
Jika kamu (LLM) membaca file ini, tugasmu adalah membantu user menyelesaikan checklist di bawah ini. 
1. Fokus **HANYA** pada bagian **[🔴 TUGAS WAJIB]** terlebih dahulu.
2. Jangan sentuh bagian **[🟢 TUGAS OPSIONAL]** kecuali user secara eksplisit memintanya.
3. Setiap kali sebuah modul selesai diimplementasi dan diuji, ubah tanda `[ ]` menjadi `[x]`.

---

## 🔴 TUGAS WAJIB (Target: 70 Poin Core + 20 Poin Docs + 10 Poin Video)

### 1. Core Requirements (70 Poin)

**A. Distributed Lock Manager (25 poin)**
- [x] Implementasi algoritma Raft Consensus dari awal (*from scratch*).
- [x] Setup minimum 3 nodes yang saling berkomunikasi.
- [x] Fitur: *Shared locks*.
- [x] Fitur: *Exclusive locks*.
- [x] Skenario Handling: *Network partition*.
- [x] Implementasi *deadlock detection* di lingkungan terdistribusi.

**B. Distributed Queue System (20 poin)**
- [x] Implementasi *distributed queue* menggunakan *consistent hashing*.
- [x] Dukungan untuk *multiple producers* dan *consumers*.
- [x] Implementasi *message persistence* dan *recovery*.
- [x] Skenario Handling: *Node failure* tanpa kehilangan data.
- [x] Jaminan pengiriman: *At-least-once delivery*.

**C. Distributed Cache Coherence (15 poin)**
- [x] Pilih dan implementasi satu protokol: MESI / MOSI / MOESI.
- [x] Dukungan untuk *multiple cache nodes*.
- [x] Handling *cache invalidation* dan *update propagation*.
- [x] Implementasi *replacement policy* (Pilih: LRU / LFU).
- [x] Setup *performance monitoring* dan *metrics collection*.

**D. Containerization (10 poin)**
- [x] Buat `Dockerfile` untuk setiap komponen (Nodes, Redis, dll).
- [x] Buat `docker-compose.yml` untuk orkestrasi sistem.
- [x] Dukungan untuk *scaling nodes* secara dinamis via Docker.
- [x] Setup konfigurasi environment menggunakan file `.env`.

### 2. Documentation & Reporting (20 Poin)

**A. Technical Documentation (10 poin)**
- [x] Gambar/diagram arsitektur sistem lengkap.
- [x] Penjelasan detail algoritma yang digunakan (Raft, Hashing, Cache Protocol).
- [x] Dokumentasi API menggunakan OpenAPI / Swagger Spec (`api_spec.yaml`).
- [x] Panduan *deployment* dan *troubleshooting* (`deployment_guide.md`).

**B. Performance Analysis Report (10 poin)**
- [x] *Benchmarking* hasil dengan berbagai skenario (gunakan `locust` atau skrip kustom).
- [x] Analisis metrik: *Throughput*, *latency*, dan *scalability*.
- [x] Komparasi performa: *Single-node* vs *Distributed*.
- [x] Buat grafik dan visualisasi data performa.
- [x] Compile laporan akhir ke dalam format PDF (`report_[NIM]_[Nama].pdf`).

### 3. Video Demonstration (10 Poin)
- [ ] Rekam pendahuluan dan tujuan (1-2 menit).
- [ ] Rekam penjelasan arsitektur sistem (2-3 menit).
- [ ] Rekam *Live demo* semua fitur berjalan (5-7 menit).
- [ ] Rekam hasil *Performance testing* (2-3 menit).
- [ ] Rekam kesimpulan dan tantangan (1-2 menit).
- [ ] Upload ke YouTube (Publik) dan masukkan link ke `README.md` & PDF.

---

## 🟢 TUGAS OPSIONAL (Bonus Maks 15 Poin)
*Catatan: Kerjakan hanya jika bagian WAJIB sudah 100% selesai dan stabil.*

**Pilihan A: Advanced Consensus Algorithm (Max 10 poin)**
- [x] Implementasi dasar PBFT (Practical Byzantine Fault Tolerance).
- [x] Handling *Byzantine failures* (up to `f = (n-1)/3` faulty nodes).
- [x] Demonstrasi ketahanan terhadap *malicious nodes*.

**Pilihan B: Geo-Distributed System (5 poin)**
- [ ] Simulasi *multi-region deployment*.
- [ ] Implementasi *latency-aware routing*.
- [ ] Support *eventual consistency model* & demonstrasi replikasi antar region.

**Pilihan C: Machine Learning Integration (5 poin)**
- [ ] *Adaptive load balancing* menggunakan ML.
- [ ] *Predictive scaling* berdasarkan pola trafik.
- [ ] *Anomaly detection* untuk kegagalan sistem.

**Pilihan D: Security & Encryption (5 poin)**
- [ ] *End-to-end encryption* untuk komunikasi antar-node.
- [ ] Implementasi RBAC (Role-Based Access Control).
- [ ] *Audit logging* dan *tamper-proof logs*.
- [ ] *Certificate management* untuk autentikasi node.

---

## 🛠️ STACK TEKNOLOGI
- **Bahasa:** Python 3.8+ (direkomendasikan `asyncio`)
- **State/Penyimpanan:** Redis
- **Jaringan/Komunikasi:** `asyncio`, `aiohttp`, atau `zeromq` (Pilih salah satu)
- **Infrastruktur:** Docker & Docker Compose
- **Testing:** `pytest`, `locust` (Load testing)

---

## 📁 STRUKTUR PROYEK (Referensi)
Pastikan hirarki folder mengikuti standar berikut saat melakukan generate kode:
```text
distributed-sync-system/
├── src/
│   ├── nodes/
│   │   ├── __init__.py
│   │   ├── base_node.py
│   │   ├── lock_manager.py
│   │   ├── queue_node.py
│   │   └── cache_node.py
│   ├── consensus/
│   │   ├── __init__.py
│   │   ├── raft.py
│   │   └── pbft.py (opsional)
│   ├── communication/
│   │   ├── __init__.py
│   │   ├── message_passing.py
│   │   └── failure_detector.py
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       └── metrics.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── performance/
├── docker/
│   ├── Dockerfile.node
│   └── docker-compose.yml
├── docs/
│   ├── architecture.md
│   ├── api_spec.yaml
│   └── deployment_guide.md
├── benchmarks/
│   └── load_test_scenarios.py
├── requirements.txt
├── .env.example
├── README.md
└── report_[NIM]_[Nama].pdf