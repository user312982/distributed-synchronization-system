# Deployment Guide & Troubleshooting

## Prerequisites

- Docker Engine ≥ 24.0
- Docker Compose v2 (plugin)
- 4 GB RAM minimum (9 nodes + monitoring)
- Port tersedia: 3000, 6379, 8000, 8101-8103, 8201-8203, 8301-8303, 9090, 9101-9303

---

## Quick Start

```bash
# 1. Clone repository
git clone <repo-url>
cd distributed-sync-system

# 2. Copy environment template
cp .env.example .env

# 3. Build dan jalankan semua services
cd docker
docker compose up --build -d

# 4. Verifikasi semua container running
docker compose ps

# 5. Akses Swagger UI
open http://localhost:8000/docs

# 6. Akses Grafana
open http://localhost:3000  # admin / admin123
```

---

## Struktur Port

| Service | Port Host | Port Container |
|---------|-----------|----------------|
| REST API (Swagger) | 8000 | 8000 |
| lock-node1 | 8101 | 8001 |
| lock-node2 | 8102 | 8001 |
| lock-node3 | 8103 | 8001 |
| queue-node1 | 8201 | 8001 |
| queue-node2 | 8202 | 8001 |
| queue-node3 | 8203 | 8001 |
| cache-node1 | 8301 | 8001 |
| cache-node2 | 8302 | 8001 |
| cache-node3 | 8303 | 8001 |
| Redis | 6379 | 6379 |
| Prometheus | 9090 | 9090 |
| Grafana | 3000 | 3000 |

---

## Scaling Nodes

```bash
# Scale lock nodes ke 5 (harus update peers config)
docker compose up -d --scale lock-node1=1  # Individual scaling

# Atau tambah node baru dengan .env override
NODE_ID=lock-node4 NODE_PORT=8004 \
NODE_PEERS=lock-node1:8001,lock-node2:8001,lock-node3:8001 \
docker compose run --rm lock-node1
```

---

## Testing Fungsionalitas

### 1. Cek Raft Leader Election

```bash
# Check siapa leader
curl http://localhost:8101/status | python3 -m json.tool
curl http://localhost:8102/status | python3 -m json.tool
curl http://localhost:8103/status | python3 -m json.tool
```

### 2. Test Lock Acquire/Release

```bash
# Acquire exclusive lock
curl -X POST http://localhost:8000/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"txn_id": "tx-001", "resource": "database", "lock_type": "EXCLUSIVE"}'

# Release lock
curl -X POST http://localhost:8000/lock/release \
  -H "Content-Type: application/json" \
  -d '{"txn_id": "tx-001", "resource": "database"}'

# View lock state
curl http://localhost:8000/lock/state | python3 -m json.tool
```

### 3. Test Network Partition (Raft)

```bash
# Simulasi network partition — pause salah satu node
docker compose stop lock-node1

# Tunggu ~300ms untuk election
sleep 1

# Verify leader election terjadi di majority partition
curl http://localhost:8102/status | python3 -m json.tool

# Restore node
docker compose start lock-node1
```

### 4. Test Queue At-Least-Once Delivery

```bash
# Enqueue message
MSG_ID=$(curl -s -X POST http://localhost:8000/queue/enqueue \
  -H "Content-Type: application/json" \
  -d '{"queue": "orders", "body": {"order": 1}}' | python3 -c "import sys,json; print(json.load(sys.stdin)['msg_id'])")

# Dequeue (tapi jangan ACK)
curl -X POST http://localhost:8000/queue/dequeue \
  -d '{"queue": "orders"}' -H "Content-Type: application/json"

# Tunggu redelivery timeout (30s default, set ke 5s untuk demo)
sleep 35

# Message akan re-appear di queue (at-least-once!)
curl -X POST http://localhost:8000/queue/dequeue \
  -d '{"queue": "orders"}' -H "Content-Type: application/json"
```

### 5. Test MESI Cache Coherence

```bash
# Write ke cache-node1 (sets M state)
curl -X PUT http://localhost:8000/cache/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": 42, "node": "node1"}'

# Read dari cache-node1 (hit, M state)
curl "http://localhost:8000/cache/mykey?node=node1"

# Read dari cache-node2 (miss → S state setelah detect peer has copy)
curl "http://localhost:8000/cache/mykey?node=node2"

# Write dari cache-node2 (S→M, broadcasts invalidate ke node1 dan node3)
curl -X PUT http://localhost:8000/cache/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": 99, "node": "node2"}'

# Verify cache-node1 sekarang Invalid
curl "http://localhost:8000/cache/mykey?node=node1"
```

### 6. Load Testing dengan Locust

```bash
# Install locust (jika belum)
pip install locust

# Jalankan load test
locust -f benchmarks/load_test_scenarios.py \
  --host=http://localhost:8000 \
  --users=50 \
  --spawn-rate=5 \
  --run-time=60s \
  --headless

# Atau dengan Web UI
locust -f benchmarks/load_test_scenarios.py \
  --host=http://localhost:8000
# Buka http://localhost:8089
```

---

## Menjalankan Unit Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run all unit tests
pytest tests/unit/ -v

# Run dengan coverage
pytest tests/unit/ -v --tb=short

# Run test spesifik
pytest tests/unit/test_raft.py -v
pytest tests/unit/test_cache.py -v
pytest tests/unit/test_queue.py -v
```

---

## Troubleshooting

### Node tidak mau start

```bash
# Check logs
docker compose logs lock-node1

# Common issues:
# 1. Redis belum ready → tunggu healthcheck
# 2. Port conflict → check `docker ps` untuk port usage
# 3. Build error → rebuild: docker compose build --no-cache
```

### Raft tidak elect leader

```bash
# Pastikan semua 3 lock nodes running
docker compose ps | grep lock

# Check peers config — NODE_PEERS harus match container hostnames
docker compose exec lock-node1 env | grep NODE_PEERS

# Check connectivity antar nodes
docker compose exec lock-node1 wget -qO- http://lock-node2:8001/health
```

### Redis connection error

```bash
# Check Redis status
docker compose exec redis redis-cli ping
# Expected: PONG

# Check dari dalam node container
docker compose exec lock-node1 python3 -c "
import asyncio, redis.asyncio as r
async def test():
    c = r.from_url('redis://redis:6379')
    print(await c.ping())
asyncio.run(test())
"
```

### High lock wait time

- Pastikan tidak ada deadlock yang tidak terdeteksi: `curl /lock/state`
- Check `deadlock_detected_total` metric di Prometheus
- Reduce concurrent transactions jika contention tinggi

---

## Environment Variables Reference

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `NODE_ID` | node1 | Unique node identifier |
| `NODE_HOST` | 0.0.0.0 | Listen address |
| `NODE_PORT` | 8001 | Listen port |
| `NODE_PEERS` | "" | Comma-separated peer addresses |
| `NODE_ROLE` | lock | Node role: lock/queue/cache |
| `REDIS_HOST` | localhost | Redis hostname |
| `REDIS_PORT` | 6379 | Redis port |
| `RAFT_ELECTION_TIMEOUT_MIN` | 150 | Min election timeout (ms) |
| `RAFT_ELECTION_TIMEOUT_MAX` | 300 | Max election timeout (ms) |
| `RAFT_HEARTBEAT_INTERVAL` | 50 | Heartbeat interval (ms) |
| `QUEUE_VIRTUAL_NODES` | 150 | Virtual nodes per node in hash ring |
| `QUEUE_DELIVERY_TIMEOUT` | 30 | Seconds before redelivery |
| `QUEUE_MAX_RETRY` | 5 | Max delivery attempts before DLQ |
| `CACHE_MAX_SIZE` | 1000 | Max cache entries per node |
| `METRICS_PORT` | 9090 | Prometheus metrics port |
| `LOG_LEVEL` | INFO | Logging level |
