import unittest
import uuid
from datetime import datetime, timezone, timedelta

from fastapi.testclient import TestClient

from app.main import app


class SyncFlowTest(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        email = f"u-{uuid.uuid4().hex[:8]}@example.com"
        r = self.client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "Password123"},
        )
        self.assertEqual(r.status_code, 200)
        token = r.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {token}"}

    def test_bidirectional_sync_and_ack(self):
        old = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        # device A uploads a new contact through sync
        r = self.client.post(
            "/api/v1/sync",
            headers=self.headers,
            json={
                "last_sync_time": old,
                "device_id": "device-a",
                "local_changes": [
                    {
                        "op": "upsert",
                        "local_id": "a-1",
                        "display_name": "Alice",
                        "phone_numbers": [{"type": "mobile", "value": "+14155550100"}],
                        "email_addresses": [{"type": "work", "value": "alice@example.com"}],
                        "postal_addresses": [],
                        "organization": "ACME",
                        "job_title": "PM",
                        "notes": "created on A",
                        "source_device_id": "device-a",
                    }
                ],
            },
        )
        self.assertEqual(r.status_code, 200)
        sync_time = r.json()["sync_time"]

        # device B pulls changes since old time
        r = self.client.get(
            "/api/v1/sync/changes",
            headers=self.headers,
            params={"since": old, "device_id": "device-b"},
        )
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(r.json()["count"], 1)
        self.assertEqual(r.json()["changes"][0]["op"], "upsert")

        # device B deletes the same contact
        r = self.client.post(
            "/api/v1/sync",
            headers=self.headers,
            json={
                "last_sync_time": sync_time,
                "device_id": "device-b",
                "local_changes": [
                    {
                        "op": "delete",
                        "local_id": "a-1",
                    }
                ],
            },
        )
        self.assertEqual(r.status_code, 200)

        # device A pulls and sees delete
        r = self.client.get(
            "/api/v1/sync/changes",
            headers=self.headers,
            params={"since": sync_time, "device_id": "device-a"},
        )
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(r.json()["count"], 1)
        self.assertEqual(r.json()["changes"][0]["op"], "delete")

        # ack success
        r = self.client.post(
            "/api/v1/sync/ack",
            headers=self.headers,
            json={"device_id": "device-a", "acked_until": datetime.now(timezone.utc).isoformat()},
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "acknowledged")


if __name__ == "__main__":
    unittest.main()
