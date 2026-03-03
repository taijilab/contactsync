import unittest
import uuid

from fastapi.testclient import TestClient

from app.main import app


class DedupeFlowTest(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        email = f"d-{uuid.uuid4().hex[:8]}@example.com"
        r = self.client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "Password123"},
        )
        self.assertEqual(r.status_code, 200)
        token = r.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {token}"}

    def _batch_upload(self, contacts):
        r = self.client.post(
            "/api/v1/contacts/batch",
            headers=self.headers,
            json={"contacts": contacts},
        )
        self.assertEqual(r.status_code, 200)

    def test_scan_and_ignore(self):
        self._batch_upload(
            [
                {
                    "local_id": "c1",
                    "display_name": "Alice Zhang",
                    "phone_numbers": [{"type": "mobile", "value": "+86 138-0013-8000"}],
                    "email_addresses": [],
                    "postal_addresses": [],
                },
                {
                    "local_id": "c2",
                    "display_name": "A. Zhang",
                    "phone_numbers": [{"type": "mobile", "value": "0138 0013 8000"}],
                    "email_addresses": [],
                    "postal_addresses": [],
                },
                {
                    "local_id": "c3",
                    "display_name": "Alice Zhang",
                    "phone_numbers": [{"type": "mobile", "value": "+1 202 555 0100"}],
                    "email_addresses": [],
                    "postal_addresses": [],
                },
            ]
        )

        r = self.client.get("/api/v1/dedupe/candidates", headers=self.headers)
        self.assertEqual(r.status_code, 200)
        pair_keys = [item["pair_key"] for item in r.json()["items"]]
        self.assertIn("c1|c2", pair_keys)
        self.assertNotIn("c1|c3", pair_keys)

        r = self.client.post(
            "/api/v1/dedupe/ignore",
            headers=self.headers,
            json={"local_id_a": "c1", "local_id_b": "c2"},
        )
        self.assertEqual(r.status_code, 200)

        r = self.client.get("/api/v1/dedupe/candidates", headers=self.headers)
        self.assertEqual(r.status_code, 200)
        pair_keys = [item["pair_key"] for item in r.json()["items"]]
        self.assertNotIn("c1|c2", pair_keys)

    def test_merge_preserves_data(self):
        self._batch_upload(
            [
                {
                    "local_id": "m1",
                    "display_name": "Bob Li",
                    "phone_numbers": [{"type": "mobile", "value": "+8613800138000"}],
                    "email_addresses": [{"type": "work", "value": "bob@acme.com"}],
                    "postal_addresses": [],
                    "notes": "note-a",
                },
                {
                    "local_id": "m2",
                    "display_name": "Bobby Li",
                    "phone_numbers": [{"type": "home", "value": "13800138000"}],
                    "email_addresses": [{"type": "home", "value": "bobli@example.com"}],
                    "postal_addresses": [],
                    "notes": "note-b",
                },
            ]
        )

        r = self.client.post(
            "/api/v1/dedupe/merge",
            headers=self.headers,
            json={"local_ids": ["m1", "m2"], "device_id": "device-a"},
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "merged")

        r = self.client.get("/api/v1/contacts", headers=self.headers)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["total"], 1)
        merged = r.json()["items"][0]

        emails = {x["value"].lower() for x in merged["email_addresses"]}
        self.assertIn("bob@acme.com", emails)
        self.assertIn("bobli@example.com", emails)
        self.assertIn("note-a", merged["notes"])
        self.assertIn("note-b", merged["notes"])


if __name__ == "__main__":
    unittest.main()
