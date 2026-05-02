"""
benchmarks/load_test_scenarios.py — Locust load tests

Run with:
  locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8000

Scenarios:
  1. Lock acquire/release cycle
  2. Queue enqueue/dequeue/ack cycle
  3. Cache read/write mixed workload
"""
import random
import uuid
from locust import HttpUser, task, between, events


class LockUser(HttpUser):
    """Simulates clients acquiring and releasing distributed locks."""
    wait_time = between(0.1, 0.5)
    weight = 3

    @task(5)
    def acquire_exclusive_lock(self):
        txn_id = str(uuid.uuid4())
        resource = f"resource-{random.randint(1, 10)}"
        with self.client.post(
            "/lock/acquire",
            json={"txn_id": txn_id, "resource": resource, "lock_type": "EXCLUSIVE"},
            catch_response=True,
            name="lock/acquire_exclusive",
        ) as resp:
            if resp.status_code in (200, 307):
                resp.success()
                # Release it
                self.client.post(
                    "/lock/release",
                    json={"txn_id": txn_id, "resource": resource},
                    name="lock/release",
                )
            else:
                resp.failure(f"Unexpected {resp.status_code}")

    @task(2)
    def acquire_shared_lock(self):
        txn_id = str(uuid.uuid4())
        resource = f"resource-{random.randint(1, 10)}"
        with self.client.post(
            "/lock/acquire",
            json={"txn_id": txn_id, "resource": resource, "lock_type": "SHARED"},
            catch_response=True,
            name="lock/acquire_shared",
        ) as resp:
            if resp.status_code in (200, 307):
                resp.success()
                self.client.post(
                    "/lock/release",
                    json={"txn_id": txn_id, "resource": resource},
                    name="lock/release",
                )
            else:
                resp.failure(f"Unexpected {resp.status_code}")

    @task(1)
    def check_lock_state(self):
        self.client.get("/lock/state", name="lock/state")


class QueueUser(HttpUser):
    """Simulates producers and consumers on the distributed queue."""
    wait_time = between(0.05, 0.2)
    weight = 4

    @task(5)
    def produce_message(self):
        queue = f"queue-{random.randint(1, 5)}"
        self.client.post(
            "/queue/enqueue",
            json={
                "queue": queue,
                "body": {"payload": random.randint(1, 1000), "ts": str(uuid.uuid4())},
                "producer_id": f"locust-{self.environment.runner.user_count}",
            },
            name="queue/enqueue",
        )

    @task(4)
    def consume_message(self):
        queue = f"queue-{random.randint(1, 5)}"
        node = random.choice(["node1", "node2", "node3"])
        with self.client.post(
            "/queue/dequeue",
            json={"queue": queue, "consumer_id": "locust-consumer", "node": node},
            catch_response=True,
            name="queue/dequeue",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("msg_id"):
                    self.client.post(
                        "/queue/ack",
                        json={"queue": queue, "msg_id": data["msg_id"]},
                        name="queue/ack",
                    )
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}")

    @task(1)
    def queue_stats(self):
        self.client.get("/queue/stats", name="queue/stats")


class CacheUser(HttpUser):
    """Simulates mixed read/write workload on MESI cache."""
    wait_time = between(0.01, 0.1)
    weight = 5

    KEYS = [f"key-{i}" for i in range(50)]

    @task(8)
    def cache_read(self):
        key = random.choice(self.KEYS)
        node = random.choice(["node1", "node2", "node3"])
        self.client.get(f"/cache/{key}?node={node}", name="cache/read")

    @task(2)
    def cache_write(self):
        key = random.choice(self.KEYS)
        node = random.choice(["node1", "node2", "node3"])
        self.client.put(
            f"/cache/{key}",
            json={"value": random.randint(1, 10000), "node": node},
            name="cache/write",
        )

    @task(1)
    def cache_snapshot(self):
        node = random.choice(["node1", "node2", "node3"])
        self.client.get(f"/cache/snapshot/all?node={node}", name="cache/snapshot")
