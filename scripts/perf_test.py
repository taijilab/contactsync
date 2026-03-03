#!/usr/bin/env python3
import argparse
import json
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib import request, error


class ApiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.token = None

    def _call(self, method: str, path: str, payload=None, headers=None):
        url = self.base_url + path
        body = None
        req_headers = {"Content-Type": "application/json"}
        if headers:
            req_headers.update(headers)
        if self.token:
            req_headers["Authorization"] = f"Bearer {self.token}"
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
        req = request.Request(url=url, data=body, headers=req_headers, method=method)
        started = time.perf_counter()
        try:
            with request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
                elapsed = (time.perf_counter() - started) * 1000
                return resp.status, raw, elapsed
        except error.HTTPError as e:
            raw = e.read().decode("utf-8")
            elapsed = (time.perf_counter() - started) * 1000
            return e.code, raw, elapsed

    def register_and_login(self):
        email = f"perf-{uuid.uuid4().hex[:8]}@example.com"
        status, raw, _ = self._call("POST", "/api/v1/auth/register", {"email": email, "password": "Password123"})
        if status != 200:
            raise RuntimeError(f"register failed: {status} {raw}")
        data = json.loads(raw)
        self.token = data["access_token"]


def build_contact(i: int):
    return {
        "local_id": f"perf-{i}",
        "display_name": f"Perf User {i}",
        "given_name": "Perf",
        "family_name": f"User{i}",
        "phone_numbers": [{"type": "mobile", "value": f"+1202555{i:04d}"}],
        "email_addresses": [{"type": "work", "value": f"perf{i}@example.com"}],
        "postal_addresses": [],
        "organization": "CloudSync",
        "job_title": "Tester",
        "notes": "bulk-seed",
        "source_device_id": "perf-device-a",
    }


def pct(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    idx = int((len(values) - 1) * p)
    return values[idx]


def run_full_upload(client: ApiClient, total_contacts: int, batch_size: int, workers: int):
    batches = []
    for start in range(0, total_contacts, batch_size):
        batch = [build_contact(i) for i in range(start, min(start + batch_size, total_contacts))]
        batches.append(batch)

    latencies = []
    ok = 0
    failed = 0
    started = time.perf_counter()

    def submit(batch):
        return client._call("POST", "/api/v1/contacts/batch", {"contacts": batch})

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(submit, b) for b in batches]
        for f in as_completed(futures):
            status, raw, elapsed = f.result()
            latencies.append(elapsed)
            if status == 200:
                ok += 1
            else:
                failed += 1
                print("batch error:", status, raw)

    total_ms = (time.perf_counter() - started) * 1000
    return {
        "name": "full_upload",
        "contacts": total_contacts,
        "batches": len(batches),
        "workers": workers,
        "total_ms": round(total_ms, 2),
        "throughput_contacts_per_sec": round(total_contacts / (total_ms / 1000), 2) if total_ms else 0,
        "ok": ok,
        "failed": failed,
        "latency_ms": {
            "avg": round(statistics.mean(latencies), 2) if latencies else 0,
            "p50": round(pct(latencies, 0.50), 2),
            "p95": round(pct(latencies, 0.95), 2),
            "p99": round(pct(latencies, 0.99), 2),
        },
    }


def run_incremental_sync(client: ApiClient, changes: int, workers: int):
    last_sync = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

    def change_payload(i: int):
        return {
            "last_sync_time": last_sync,
            "device_id": f"perf-device-{i % 2}",
            "local_changes": [
                {
                    "op": "upsert",
                    "local_id": f"inc-{i}",
                    "display_name": f"Inc User {i}",
                    "phone_numbers": [{"type": "mobile", "value": f"+1415555{i:04d}"}],
                    "email_addresses": [{"type": "work", "value": f"inc{i}@example.com"}],
                    "postal_addresses": [],
                    "organization": "CloudSync",
                    "job_title": "IncTester",
                    "notes": "incremental",
                }
            ],
        }

    latencies = []
    ok = 0
    failed = 0
    started = time.perf_counter()

    def submit(i: int):
        return client._call("POST", "/api/v1/sync", change_payload(i))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(submit, i) for i in range(changes)]
        for f in as_completed(futures):
            status, raw, elapsed = f.result()
            latencies.append(elapsed)
            if status == 200:
                ok += 1
            else:
                failed += 1
                print("sync error:", status, raw)

    total_ms = (time.perf_counter() - started) * 1000
    return {
        "name": "incremental_sync",
        "changes": changes,
        "workers": workers,
        "total_ms": round(total_ms, 2),
        "throughput_changes_per_sec": round(changes / (total_ms / 1000), 2) if total_ms else 0,
        "ok": ok,
        "failed": failed,
        "latency_ms": {
            "avg": round(statistics.mean(latencies), 2) if latencies else 0,
            "p50": round(pct(latencies, 0.50), 2),
            "p95": round(pct(latencies, 0.95), 2),
            "p99": round(pct(latencies, 0.99), 2),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="CloudSyncContacts performance baseline tool")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--contacts", type=int, default=5000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--sync-changes", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    client = ApiClient(args.base_url)
    client.register_and_login()

    print("[1/2] running full upload test...")
    full = run_full_upload(client, args.contacts, args.batch_size, args.workers)
    print(json.dumps(full, ensure_ascii=False, indent=2))

    print("[2/2] running incremental sync test...")
    inc = run_incremental_sync(client, args.sync_changes, args.workers)
    print(json.dumps(inc, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
