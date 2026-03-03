import unittest
import uuid
from datetime import datetime, timezone, timedelta

from fastapi.testclient import TestClient

from app.main import app


class ConflictFlowTest(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        email = f"c-{uuid.uuid4().hex[:8]}@example.com"
        r = self.client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "Password123"},
        )
        self.assertEqual(r.status_code, 200)
        token = r.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {token}"}

    def test_conflict_resolve_and_rollback(self):
        old = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        # device-a creates contact
        r = self.client.post(
            "/api/v1/sync",
            headers=self.headers,
            json={
                "last_sync_time": old,
                "device_id": "device-a",
                "local_changes": [
                    {
                        "op": "upsert",
                        "local_id": "c1",
                        "display_name": "Alice",
                        "notes": "v1",
                    }
                ],
            },
        )
        self.assertEqual(r.status_code, 200)
        t1 = r.json()["sync_time"]

        # device-a updates to v2
        r = self.client.post(
            "/api/v1/sync",
            headers=self.headers,
            json={
                "last_sync_time": t1,
                "device_id": "device-a",
                "local_changes": [
                    {
                        "op": "upsert",
                        "local_id": "c1",
                        "display_name": "Alice",
                        "notes": "v2-server",
                    }
                ],
            },
        )
        self.assertEqual(r.status_code, 200)

        # device-b (stale timestamp) edits same contact -> conflict
        r = self.client.post(
            "/api/v1/sync",
            headers=self.headers,
            json={
                "last_sync_time": old,
                "device_id": "device-b",
                "local_changes": [
                    {
                        "op": "upsert",
                        "local_id": "c1",
                        "display_name": "Alice",
                        "notes": "v2-local",
                    }
                ],
            },
        )
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(len(r.json()["conflicts"]), 1)
        conflict_id = r.json()["conflicts"][0]["conflict_id"]

        # list conflicts
        r = self.client.get("/api/v1/conflicts", headers=self.headers)
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(r.json()["count"], 1)

        # resolve by keep_local
        r = self.client.post(
            f"/api/v1/conflicts/{conflict_id}/resolve",
            headers=self.headers,
            json={"strategy": "keep_local", "device_id": "device-b"},
        )
        self.assertEqual(r.status_code, 200)

        # check final note equals local
        r = self.client.get("/api/v1/contacts", headers=self.headers)
        self.assertEqual(r.status_code, 200)
        contact = r.json()["items"][0]
        self.assertEqual(contact["notes"], "v2-local")
        contact_id = contact["contact_id"]

        # force another update so rollback has visible effect
        latest = datetime.now(timezone.utc).isoformat()
        r = self.client.post(
            "/api/v1/sync",
            headers=self.headers,
            json={
                "last_sync_time": latest,
                "device_id": "device-a",
                "local_changes": [
                    {
                        "op": "upsert",
                        "local_id": "c1",
                        "display_name": "Alice",
                        "notes": "v3",
                    }
                ],
            },
        )
        self.assertEqual(r.status_code, 200)

        # list history and rollback to first snapshot
        r = self.client.get(f"/api/v1/contacts/{contact_id}/history", headers=self.headers)
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(r.json()["count"], 1)
        history_id = r.json()["items"][-1]["history_id"]

        r = self.client.post(
            f"/api/v1/contacts/{contact_id}/rollback",
            headers=self.headers,
            json={"history_id": history_id, "device_id": "device-a"},
        )
        self.assertEqual(r.status_code, 200)

        r = self.client.get("/api/v1/contacts", headers=self.headers)
        self.assertEqual(r.status_code, 200)
        self.assertNotEqual(r.json()["items"][0]["notes"], "v3")


if __name__ == "__main__":
    unittest.main()
