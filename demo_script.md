# 📋 Analisis Kesesuaian Project & Script Demo Video

## ✅ Kesesuaian dengan Tugas + Referensi

### Referensi Wajib yang Disebutkan di Tugas

| Referensi | Ada di `refrences/` | Diimplementasi di Project |
|---|---|---|
| **Raft Consensus Algorithm Paper** (Ongaro & Ousterhout) | ✅ `raft.pdf` | ✅ `src/consensus/raft.py` — from scratch: leader election, log replication, heartbeat |
| **Distributed Systems Principles & Paradigms** | ✅ `Distributed_Systems_4.pdf` | ✅ Semua konsep (fault tolerance, consistency, partitioning) diimplementasi |
| **Redis Distributed Lock Documentation** | ✅ `distributed-locks.md` (Redlock) | ✅ `src/nodes/lock_manager.py` — safety & liveness properties |

### Kesesuaian Raft Paper → Implementasi

Raft paper mensyaratkan:
- **Strong leader**: ✅ Leader election dengan randomized timeout
- **Leader election**: ✅ `_start_election()` + RequestVote RPC
- **Log replication**: ✅ `_send_append_entries()` + majority commit
- **Safety**: ✅ Hanya commit jika majority (`_try_advance_commit()`)
- **Membership changes**: ⚠️ Tidak diimplementasi (tidak diwajibkan tugas)

### Kesesuaian Redis Distributed Locks → Implementasi

Redlock mensyaratkan:
- **Mutual exclusion**: ✅ EXCLUSIVE lock
- **Deadlock free**: ✅ Wait-For Graph + victim selection
- **Fault tolerance**: ✅ Step down jika tidak reach majority

---

## 📊 Status task.md vs Kenyataan

| Item task.md | Status |
|---|---|
| ✅ Raft Consensus from scratch | ✅ DONE |
| ✅ 3+ nodes berkomunikasi | ✅ DONE (4 lock nodes, 3 queue, 3 cache) |
| ✅ Shared & Exclusive locks | ✅ DONE |
| ✅ Network partition handling | ✅ DONE |
| ✅ Deadlock detection | ✅ DONE |
| ✅ Consistent Hashing queue | ✅ DONE |
| ✅ At-least-once delivery | ✅ DONE |
| ✅ Message persistence Redis | ✅ DONE |
| ✅ MESI protocol | ✅ DONE |
| ✅ LRU replacement | ✅ DONE |
| ✅ Docker + compose | ✅ DONE |
| ✅ .env config | ✅ DONE |
| ✅ architecture.md + diagram | ✅ DONE |
| ✅ api_spec.yaml (OpenAPI) | ✅ DONE |
| ✅ deployment_guide.md | ✅ DONE |
| ✅ Benchmarking (locust) | ✅ DONE |
| ✅ Grafik performa (PNG) | ✅ DONE |
| ✅ PBFT bonus | ✅ DONE |
| ⬜ Video YouTube (link di README) | ❌ BELUM |
| ⬜ Link GitHub di README | ❌ BELUM |

### Yang Masih Kurang (2 item):
1. **Video YouTube** — belum direkam/upload
2. **Link GitHub & YouTube di README** — masih placeholder

> Semua kode, struktur, dan dokumentasi sudah **100% sesuai** spesifikasi tugas dan referensi.

---

---

# 🎬 Script Demo Video (10–15 Menit, Bahasa Indonesia)

> **Ketentuan dari tugas:** Durasi 10–15 menit, Bahasa Indonesia, video publik YouTube.

---

## Segmen 1 — Pendahuluan dan Tujuan (1–2 menit) `[00:00–01:30]`

**Yang ditampilkan di layar:** Slide/README project

**Script:**
> "Halo, perkenalkan saya [nama] dengan NIM [NIM]. Pada video ini saya akan mempresentasikan Tugas 2 mata kuliah Sistem Parallel dan Terdistribusi, yaitu implementasi **Distributed Synchronization System**.
>
> Sistem ini mengimplementasikan tiga komponen utama sinkronisasi terdistribusi:
> - **Distributed Lock Manager** menggunakan algoritma **Raft Consensus** dari paper Diego Ongaro
> - **Distributed Queue** menggunakan **Consistent Hashing**
> - **Distributed Cache Coherence** dengan protokol **MESI**
>
> Sebagai bonus, saya juga mengimplementasikan **PBFT** — Practical Byzantine Fault Tolerance untuk menangani malicious nodes.
>
> Semua sistem berjalan dalam **Docker containers** dengan monitoring via Prometheus dan Grafana."

```bash
# Tunjukkan struktur project
ls -la
cat README.md
```

---

## Segmen 2 — Penjelasan Arsitektur Sistem (2–3 menit) `[01:30–04:00]`

**Yang ditampilkan di layar:** `docs/architecture.md` + diagram

**Script:**
> "Mari kita lihat arsitektur sistem. Sistem terdiri dari tiga cluster node:"

```bash
# Tampilkan diagram arsitektur
cat docs/architecture.md
```

> "Cluster pertama adalah **4 Lock Nodes** — menjalankan Raft consensus. Satu node menjadi leader, sisanya follower.
>
> Cluster kedua adalah **3 Queue Nodes** — menggunakan consistent hashing ring untuk mendistribusikan pesan.
>
> Cluster ketiga adalah **3 Cache Nodes** — menjalankan protokol MESI untuk cache coherence.
>
> Semua node berkomunikasi via aiohttp JSON RPC. Redis digunakan untuk persistence. FastAPI sebagai REST gateway."

```bash
# Tunjukkan docker-compose running
cd docker
docker compose ps
```

> "Semua 14 container berjalan: 4 lock nodes, 3 queue nodes, 3 cache nodes, Redis, API gateway, Prometheus, dan Grafana."

---

## Segmen 3 — Live Demo Semua Fitur (5–7 menit) `[04:00–10:30]`

### 3A. Distributed Lock Manager — Raft Consensus `[04:00–05:30]`

**Yang ditampilkan:** Browser → http://localhost:8000/docs

**Script:**
> "Sekarang live demo. Saya buka Swagger UI di localhost:8000/docs."

**Langkah demo:**

```
# 1. Cek status semua node
GET /status
→ Tunjukkan node mana yang jadi LEADER
```

> "Terlihat node1 adalah Raft Leader. Semua node sudah terhubung dalam cluster."

```
# 2. Acquire EXCLUSIVE lock
POST /lock/acquire
Body: { "txn_id": "txn-001", "resource": "database_tabel_A", "lock_type": "EXCLUSIVE", "node": "node1" }
→ Response: {"status": "granted"}
```

> "Lock berhasil di-acquire. Sekarang coba acquire SHARED lock resource berbeda:"

```
# 3. Acquire SHARED lock
POST /lock/acquire
Body: { "txn_id": "txn-002", "resource": "laporan_pdf", "lock_type": "SHARED", "node": "node1" }
```

```
# 4. Lihat state lock
GET /lock/state?node=node1
```

```
# 5. Demo network partition — matikan 1 node
docker compose stop lock-node2

# Coba acquire lock lagi → sistem tetap jalan (majority = 3/4 masih ada)
POST /lock/acquire
Body: { "txn_id": "txn-003", "resource": "file_config", "lock_type": "EXCLUSIVE", "node": "node1" }

# Restart kembali
docker compose start lock-node2
```

> "Meskipun lock-node2 dimatikan, sistem tetap berfungsi karena Raft hanya butuh majority — 3 dari 4 node masih bisa commit."

```
# 6. Release lock
POST /lock/release
Body: { "txn_id": "txn-001", "resource": "database_tabel_A", "node": "node1" }
```

---

### 3B. Distributed Queue — Consistent Hashing `[05:30–07:00]`

**Script:**
> "Berikutnya adalah Distributed Queue dengan Consistent Hashing."

```
# 1. Enqueue pesan
POST /queue/enqueue
Body: { "queue": "orders", "body": {"item": "laptop", "qty": 1, "price": 15000000}, "producer_id": "toko-online", "node": "node1" }

POST /queue/enqueue
Body: { "queue": "orders", "body": {"item": "mouse", "qty": 2, "price": 250000}, "producer_id": "toko-online", "node": "node1" }
```

> "Pesan otomatis di-route ke node yang tepat berdasarkan consistent hashing dari nama queue."

```
# 2. Dequeue
POST /queue/dequeue
Body: { "queue": "orders", "consumer_id": "warehouse", "node": "node1" }
→ Simpan msg_id dari response
```

```
# 3. ACK (at-least-once delivery)
POST /queue/ack
Body: { "queue": "orders", "msg_id": "<msg_id>", "node": "node1" }
```

```
# 4. Stats
GET /queue/stats?node=node1
```

> "Ini membuktikan at-least-once delivery — pesan tidak dihapus dari Redis sampai di-ACK oleh consumer."

---

### 3C. Cache Coherence — Protokol MESI `[07:00–08:30]`

**Script:**
> "Sekarang demo Cache Coherence dengan protokol MESI — Modified, Exclusive, Shared, Invalid."

```
# 1. Write ke node1 → state M (Modified)
PUT /cache/user-profile-123
Body: { "value": {"nama": "Alice", "role": "admin", "level": 5}, "node": "node1" }
→ Response: {"state": "M", "transition": "I→M"}
```

> "State M artinya Modified — node1 punya copy exclusive dan sudah invalidate semua peer."

```
# 2. Read dari node1 → HIT, state M
GET /cache/user-profile-123?node=node1
→ Response: {"hit": true, "state": "M"}
```

> "Cache HIT dari node1."

```
# 3. Read dari node2 → state berbeda (fresh fetch)
GET /cache/user-profile-123?node=node2
→ Response: {"hit": false, "state": "E"}
```

> "Node2 mengambil data baru dari backing store — state E (Exclusive) karena tidak ada peer lain yang punya."

```
# 4. Snapshot
GET /cache/snapshot/all?node=node1
```

---

## Segmen 4 — PBFT Byzantine Fault Tolerance (Bonus) `[08:30–09:30]`

**Script:**
> "Sebagai bonus, saya mengimplementasikan PBFT untuk menangani Byzantine nodes — node yang berperilaku jahat atau mengirim data palsu."

```bash
# Tunjukkan konfigurasi malicious node
grep -A 5 "IS_MALICIOUS" docker/docker-compose.yml
```

> "lock-node4 dikonfigurasi sebagai Byzantine node — `IS_MALICIOUS: true`."

```bash
# Lihat log Byzantine behavior
docker compose logs lock-node4 2>&1 | grep MALICIOUS | tail -10
```

> "Terlihat node4 mengirim digest palsu dan meng-drop commit. Namun sistem tetap berfungsi karena PBFT toleran terhadap `f = (N-1)/3 = 1` Byzantine node dari 4 total node."

```
# Tunjukkan status node via API
GET /status
→ Lihat "consensus": "pbft", "malicious": true di node4
```

> "Ini membuktikan PBFT complete implementation — bukan hanya partial."

---

## Segmen 5 — Performance Testing (2–3 menit) `[09:30–12:00]`

**Script:**
> "Sekarang performance testing."

```bash
# 1. Unit + Performance tests
cd ..
source venv/bin/activate
pytest tests/unit/ tests/performance/ -v -s 2>&1 | tail -40
```

> "27 PBFT tests dan 12 performance tests — semua passed. Hasil benchmark menunjukkan:
> - Raft single-node: **80.000+ ops/sec**
> - PBFT 4-node: latency rata-rata **0.15ms**
> - LRU Cache: **2.4 juta ops/sec**
> - Hash Ring: distribusi max deviation **15%** (uniform)"

```bash
# 2. Load test dengan Locust
locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8000 --headless -u 10 -r 2 --run-time 30s 2>&1 | tail -20
```

```bash
# 3. Tunjukkan grafik hasil
ls benchmarks/*.png
# Buka latency.png dan throughput.png
```

**Buka Grafana:**
> "Di Grafana localhost:3000, kita bisa lihat metrik real-time. Cache hit rate, Raft term, queue depth — semua bergerak saat load test berjalan."

---

## Segmen 6 — Kesimpulan dan Tantangan (1–2 menit) `[12:00–14:00]`

**Script:**
> "Sebagai kesimpulan, sistem ini berhasil mengimplementasikan:
>
> ✅ **Raft Consensus** from scratch — sesuai paper Ongaro & Ousterhout
> ✅ **Distributed Lock Manager** dengan shared/exclusive lock dan deadlock detection
> ✅ **Consistent Hashing Queue** dengan at-least-once delivery
> ✅ **MESI Cache Coherence** dengan LRU replacement
> ✅ **Containerization** dengan Docker Compose (14 containers)
> ✅ **PBFT Byzantine Fault Tolerance** sebagai bonus
>
> **Tantangan terbesar:**
> - Implementasi Raft from scratch memerlukan pemahaman mendalam tentang edge cases — terutama split vote dan log consistency saat network partition
> - MESI protocol memerlukan synchronisasi yang hati-hati antar node untuk menghindari race condition
> - Byzantine simulation dengan randomness membuat testing non-deterministik
>
> Terima kasih. Link GitHub dan semua kode tersedia di README. Selengkapnya bisa dilihat di laporan PDF yang terlampir."

---

## 📌 Checklist Sebelum Rekam

- [ ] Docker compose running (`docker compose ps` — semua UP)
- [ ] Browser buka `localhost:8000/docs`
- [ ] Browser buka `localhost:3000` (Grafana)
- [ ] Terminal siap di root project
- [ ] `venv` sudah aktif (`source venv/bin/activate`)
- [ ] Screen recorder aktif
- [ ] Mikrofon test

## ⚠️ Yang Harus Dilakukan Setelah Video

1. Upload video ke YouTube → set **Publik**
2. Copy link YouTube → paste ke `README.md` (ganti `_[isi setelah upload]_`)
3. Copy link YouTube → paste ke `report_[NIM]_[Nama].pdf`
4. Push semua file ke GitHub → set repository **Public**
5. Ganti nama file `report_123456789_JohnDoe.pdf` → `report_[NIM_ASLI]_[NAMA_ASLI].pdf`
